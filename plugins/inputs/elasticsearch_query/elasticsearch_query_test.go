package elasticsearch_query

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/testutil"
	elastic "gopkg.in/olivere/elastic.v5"
)

//var testindex = "test_query"

func init() {

	type nginxlog struct {
		IPaddress    string    `json:"IP"`
		Timestamp    time.Time `json:"@timestamp"`
		Method       string    `json:"method"`
		URI          string    `json:"URI"`
		Httpversion  string    `json:"http_version"`
		Response     string    `json:"response"`
		Responsetime float64   `json:"response_time"`
	}

	e := &ElasticsearchQuery{
		URLs:                []string{"http://" + testutil.GetLocalHost() + ":9200"},
		Timeout:             internal.Duration{Duration: time.Second * 5},
		HealthCheckInterval: internal.Duration{Duration: time.Second * 10},
	}

	err := e.connectToES()
	if err != nil {
		fmt.Printf("Error connecting to Elasticsearch")
	}

	bulkRequest := e.Client.Bulk()

	// populate elasticsearch with nginx_logs test data file
	file, err := os.Open("testdata/nginx_logs")
	if err != nil {
		fmt.Printf("Error opening testdata file")
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), " ")
		responsetime, _ := strconv.Atoi(parts[9])

		logline := nginxlog{
			IPaddress:    parts[0],
			Timestamp:    time.Now().UTC(),
			Method:       strings.Replace(parts[5], `"`, "", -1),
			URI:          parts[6],
			Httpversion:  strings.Replace(parts[7], `"`, "", -1),
			Response:     parts[8],
			Responsetime: float64(responsetime),
		}

		bulkRequest.Add(elastic.NewBulkIndexRequest().
			Index("testquery").
			Type("testquery_data").
			Doc(logline))

	}

	if err = scanner.Err(); err != nil {
		fmt.Printf("Error reading testdata file")
	}

	_, err = bulkRequest.Do(context.Background())
	if err != nil {
		fmt.Printf("Error sending bulk request to Elasticsearch: %s", err)
	}
}

func TestElasticsearchQuery_getMetricFields(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	type args struct {
		ctx         context.Context
		aggregation Aggregation
	}

	e := &ElasticsearchQuery{
		URLs:                []string{"http://" + testutil.GetLocalHost() + ":9200"},
		Timeout:             internal.Duration{Duration: time.Second * 5},
		HealthCheckInterval: internal.Duration{Duration: time.Second * 10},
	}

	tests := []struct {
		name    string
		e       *ElasticsearchQuery
		args    args
		want    map[string]string
		wantErr bool
	}{
		{
			"getMetricFields",
			e,
			args{
				context.Background(),
				Aggregation{
					Index:        "testquery",
					MetricFields: []string{"URI", "http_version", "method", "response", "response_time"},
				},
			},
			map[string]string{
				"URI":           "text",
				"http_version":  "text",
				"method":        "text",
				"response":      "text",
				"response_time": "long"},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.e.getMetricFields(tt.args.ctx, tt.args.aggregation)
			if (err != nil) != tt.wantErr {
				t.Errorf("ElasticsearchQuery.getMetricFields() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ElasticsearchQuery.getMetricFields() = %v, want %v", got, tt.want)
			}
		})
	}
}

// func TestBuildAggregationQuery(t *testing.T) {
// 	if testing.Short() {
// 		t.Skip("Skipping integration test in short mode")
// 	}

// 	urls := []string{"http://" + testutil.GetLocalHost() + ":9200"}

// 	var e = &ElasticsearchQuery{
// 		URLs:                urls,
// 		Timeout:             internal.Duration{Duration: time.Second * 5},
// 		HealthCheckInterval: internal.Duration{Duration: time.Second * 10},
// 	}

// 	var tests = []struct {
// 		Agg      Aggregation
// 		Expected []string
// 	}{
// 		{
// 			Aggregation{
// 				Index:           "testagg_data",
// 				MeasurementName: "avg_order_price",
// 				FilterQuery:     "type: order AND status: completed",
// 				QueryPeriod:     internal.Duration{Duration: time.Second * 300},
// 				MetricFields:    []string{"price"},
// 				DateField:       "time",
// 				Tags:            []string{"manufacturer", "article"},
// 				MetricFunction:  "avg",
// 			},
// 			[]string{
// 				"{\"aggregations\":{\"manufacturer\":{\"aggregations\":{\"price\":{\"avg\":{\"field\":\"price\"}}},\"terms\":{\"field\":\"manufacturer\",\"size\":1000}}},\"terms\":{\"field\":\"article\",\"size\":1000}}",
// 			},
// 		},
// 	}

// 	mapMetricFields := make(map[string]string)
// 	mapMetricFields["price"] = ("float")

// 	for _, test := range tests {
// 		queryData, err := e.buildAggregationQuery(mapMetricFields, test.Agg)
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		for _, query := range queryData {
// 			if query.aggregation != nil {
// 				if query.isParent == true {
// 					src, err := query.aggregation.Source()
// 					if err != nil {
// 						t.Fatal(err)
// 					}
// 					data, err := json.Marshal(src)
// 					if err != nil {
// 						t.Fatal(err)
// 					}
// 					got := string(data)

// 					if got != test.Expected[0] {
// 						t.Errorf("expected %q; got: %q", test.Expected[0], got)
// 					}
// 				}
// 			}
// 		}

// 		// if path != test.Expected {
// 		// 	t.Errorf("expected %q; got: %q", test.Expected, path)
// 		// }

// 		var acc testutil.Accumulator
// 		err = e.Gather(&acc)
// 	}

// 	// // Verify that we can successfully write data to Elasticsearch
// 	// err = e.Write(testutil.MockMetrics())
// 	// require.NoError(t, err)

// }

// func TestElasticsearchQuery_connectToES(t *testing.T) {
// 	type fields struct {
// 		URLs                []string
// 		Username            string
// 		Password            string
// 		EnableSniffer       bool
// 		Tracelog            bool
// 		Timeout             internal.Duration
// 		HealthCheckInterval internal.Duration
// 		Aggregations        []Aggregation
// 		Client              *elastic.Client
// 		acc                 telegraf.Accumulator
// 	}
// 	tests := []struct {
// 		name    string
// 		fields  fields
// 		wantErr bool
// 	}{
// 		{
// 			"Connect to Elasticsearch on localhost",
// 			fields{
// 				URLs:                []string{"http://localhost:9200"},
// 				Timeout:             internal.Duration{Duration: time.Second * 5},
// 				HealthCheckInterval: internal.Duration{Duration: time.Second * 5},
// 			},
// 			false,
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			e := &ElasticsearchQuery{
// 				URLs:                tt.fields.URLs,
// 				Timeout:             tt.fields.Timeout,
// 				HealthCheckInterval: tt.fields.HealthCheckInterval,
// 			}
// 			if err := e.connectToES(); (err != nil) != tt.wantErr {
// 				t.Errorf("ElasticsearchQuery.connectToES() error = %v, wantErr %v", err, tt.wantErr)
// 			}
// 		})
// 	}
// }

// func TestElasticsearchQuery_Gather(t *testing.T) {

// 	//var acc testutil.Accumulator

// 	type fields struct {
// 		URLs                []string
// 		Username            string
// 		Password            string
// 		EnableSniffer       bool
// 		Tracelog            bool
// 		Timeout             internal.Duration
// 		HealthCheckInterval internal.Duration
// 		Aggregations        []Aggregation
// 		Client              *elastic.Client
// 		acc                 telegraf.Accumulator
// 	}

// 	// type args struct {
// 	// 	acc telegraf.Accumulator
// 	// }

// 	tests := []struct {
// 		name    string
// 		fields  fields
// 		wantErr bool
// 	}{
// 		{
// 			"Simple count",
// 			fields{
// 				URLs:                []string{"http://localhost:9200"},
// 				Timeout:             internal.Duration{Duration: time.Second * 5},
// 				HealthCheckInterval: internal.Duration{Duration: time.Second * 5},
// 			},
// 			false,
// 		},
// 		// TODO: Add test cases.
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			e := &ElasticsearchQuery{
// 				URLs:                tt.fields.URLs,
// 				Username:            tt.fields.Username,
// 				Password:            tt.fields.Password,
// 				EnableSniffer:       tt.fields.EnableSniffer,
// 				Tracelog:            tt.fields.Tracelog,
// 				Timeout:             tt.fields.Timeout,
// 				HealthCheckInterval: tt.fields.HealthCheckInterval,
// 				Aggregations:        tt.fields.Aggregations,
// 				Client:              tt.fields.Client,
// 				acc:                 tt.fields.acc,
// 			}

// 			if err := e.Gather(tt.args.acc); (err != nil) != tt.wantErr {
// 				t.Errorf("ElasticsearchQuery.Gather() error = %v, wantErr %v", err, tt.wantErr)
// 			}
// 		})

// 	}
// }

func TestElasticsearchQuery_buildAggregationQuery(t *testing.T) {
	type fields struct {
		URLs                []string
		Username            string
		Password            string
		EnableSniffer       bool
		Tracelog            bool
		Timeout             internal.Duration
		HealthCheckInterval internal.Duration
		Aggregations        []Aggregation
		Client              *elastic.Client
		acc                 telegraf.Accumulator
	}
	type args struct {
		mapMetricFields map[string]string
		aggregation     Aggregation
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []aggregationQueryData
		wantErr bool
	}{
		{
			"http_error",
			fields{},
			args{},
			[]aggregationQueryData{},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &ElasticsearchQuery{
				URLs:                tt.fields.URLs,
				Username:            tt.fields.Username,
				Password:            tt.fields.Password,
				EnableSniffer:       tt.fields.EnableSniffer,
				Tracelog:            tt.fields.Tracelog,
				Timeout:             tt.fields.Timeout,
				HealthCheckInterval: tt.fields.HealthCheckInterval,
				Aggregations:        tt.fields.Aggregations,
				Client:              tt.fields.Client,
				acc:                 tt.fields.acc,
			}
			got, err := e.buildAggregationQuery(tt.args.mapMetricFields, tt.args.aggregation)
			if (err != nil) != tt.wantErr {
				t.Errorf("ElasticsearchQuery.buildAggregationQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ElasticsearchQuery.buildAggregationQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}
