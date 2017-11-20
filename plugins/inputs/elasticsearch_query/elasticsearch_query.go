package elasticsearch_query

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	elastic "gopkg.in/olivere/elastic.v5"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/inputs"
)

const description = `Queries Elasticseach`
const sampleConfig = `
  ## The full HTTP endpoint URL for your Elasticsearch instance
  ## Multiple urls can be specified as part of the same cluster,
  ## this means that only ONE of the urls will be written to each interval.
  urls = [ "http://node1.es.example.com:9200" ] # required.
  ## Elasticsearch client timeout, defaults to "5s" if not set.
  timeout = "5s"
  ## Set to true to ask Elasticsearch a list of all cluster nodes,
  ## thus it is not necessary to list all nodes in the urls config option
  enable_sniffer = false
  ## Set the interval to check if the Elasticsearch nodes are available
  ## Setting to "0s" will disable the health check (not recommended in production)
  health_check_interval = "10s"
  ## HTTP basic authentication details (eg. when using Shield)
  # username = "telegraf"
  # password = "mypassword"
  tracelog = false

  [[inputs.elasticsearch_query.aggregation]]
    measurement_name = "cpu_agg"
    index = "metricbeat-*"
    filter_query = "type:metricsets"
    metric_fields = ["system.cpu.*.pct"]
    metric_function = "avg"
    tags = ["beat.hostname"]
	include_missing_tag = true
	missing_tag_value = "null"
	date_field = "@timestamp"
	query_period = "5m"

  [[inputs.elasticsearch_query.aggregation]]
    measurement_name = "disk_agg"
    index = "metricbeat-*"
    filter_query = "type:metricsets"
    metric_fields = ["system.filesystem.used.bytes"]
	date_field = "@timestamp"
    tags = [ "beat.hostname","system.filesystem.mount_point"]
	include_missing_tag = true
	missing_tag_value = "null"
    metric_function = "avg"
	query_window_interval = "2m"

  [[inputs.elasticsearch_query.search]]
    measurement_name = "http_error"
	index = "*"
	filter_query = "ERROR"
	query_period = "1m"

`

type ElasticsearchQuery struct {
	URLs                []string `toml:"urls"`
	Username            string
	Password            string
	EnableSniffer       bool
	Tracelog            bool
	Timeout             internal.Duration
	HealthCheckInterval internal.Duration
	Aggregations        []Aggregation `toml:"aggregation"`
	Client              *elastic.Client
	acc                 telegraf.Accumulator
}

type Aggregation struct {
	Index             string
	MeasurementName   string
	FilterQuery       string
	QueryPeriod       internal.Duration
	MetricFields      []string `toml:"metric_fields"`
	DateField         string
	Tags              []string `toml:"tags"`
	IncludeMissingTag bool
	MissingTagValue   string
	MetricFunction    string
}

type aggKey struct {
	measurement string
	name        string
	function    string
	field       string
}

type aggregationQueryData struct {
	aggKey
	isParent    bool
	aggregation elastic.Aggregation
}

func (e *ElasticsearchQuery) init() error {
	if e.URLs == nil {
		return fmt.Errorf("Elasticsearch urls is not defined")
	}

	err := e.connectToES()

	return err

}

func (e *ElasticsearchQuery) connectToES() error {
	var clientOptions []elastic.ClientOptionFunc

	clientOptions = append(clientOptions,
		elastic.SetSniff(e.EnableSniffer),
		elastic.SetURL(e.URLs...),
		elastic.SetHealthcheckInterval(e.HealthCheckInterval.Duration),
	)

	if e.Username != "" && e.Password != "" {
		clientOptions = append(clientOptions,
			elastic.SetBasicAuth(e.Username, e.Password),
		)
	}

	if e.HealthCheckInterval.Duration == 0 {
		clientOptions = append(clientOptions,
			elastic.SetHealthcheck(false),
		)
	}

	if e.Tracelog {
		clientOptions = append(clientOptions,
			elastic.SetTraceLog(log.New(os.Stdout, "", log.LstdFlags)),
		)
	}

	client, err := elastic.NewClient(clientOptions...)
	if err != nil {
		return err
	}

	// check for ES version on first node
	esVersion, err := client.ElasticsearchVersion(e.URLs[0])

	if err != nil {
		return fmt.Errorf("Elasticsearch query version check failed: %s", err)
	}

	// quit if ES version is not supported
	i, err := strconv.Atoi(strings.Split(esVersion, ".")[0])
	if err != nil || i < 5 {
		return fmt.Errorf("Elasticsearch query: ES version not supported: %s", esVersion)
	}

	e.Client = client

	return nil

}

func (e *ElasticsearchQuery) Gather(acc telegraf.Accumulator) error {
	if err := e.init(); err != nil {
		return err
	}

	e.acc = acc

	var wg sync.WaitGroup

	for _, agg := range e.Aggregations {
		wg.Add(1)
		go func(agg Aggregation) {
			defer wg.Done()
			err := e.esAggregationQuery(agg)
			if err != nil {
				acc.AddError(fmt.Errorf("Elasticsearch query aggregation %s: %s ", agg.MeasurementName, err.Error()))
			}
		}(agg)
	}

	wg.Wait()
	return nil
}

func (e *ElasticsearchQuery) esAggregationQuery(aggregation Aggregation) error {

	ctx, cancel := context.WithTimeout(context.Background(), e.Timeout.Duration)
	defer cancel()

	mapMetricFields, err := e.getMetricFields(ctx, aggregation)
	if err != nil {
		return err
	}

	aggregationQueryList, err := e.buildAggregationQuery(mapMetricFields, aggregation)
	if err != nil {
		return err
	}

	searchResult, err := e.esRunAggregationQuery(ctx, aggregation, aggregationQueryList)
	if err != nil {
		return err
	}

	if searchResult.Aggregations != nil {
		err = e.parseAggregationResult(&aggregationQueryList, searchResult)
		if err != nil {
			return err
		}
	} else {
		err = e.parseSimpleResult(aggregation.MeasurementName, searchResult)
		if err != nil {
			return err
		}
	}

	return nil
}

func init() {
	inputs.Add("elasticsearch_query", func() telegraf.Input {
		return &ElasticsearchQuery{
			Timeout:             internal.Duration{Duration: time.Second * 5},
			HealthCheckInterval: internal.Duration{Duration: time.Second * 10},
		}
	})
}

func (e *ElasticsearchQuery) SampleConfig() string {
	return sampleConfig
}

func (e *ElasticsearchQuery) Description() string {
	return description
}
