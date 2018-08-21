package kibana

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/internal/tls"
	"github.com/influxdata/telegraf/plugins/inputs"
)

const statusPath = "/api/status"

type kibanaStatus struct {
	Name    string  `json:"name"`
	Version version `json:"version"`
	Status  status  `json:"status"`
	Metrics metrics `json:"metrics"`
}

type version struct {
	Number        string `json:"number"`
	BuildHash     string `json:"build_hash"`
	BuildNumber   int    `json:"build_number"`
	BuildSnapshot bool   `json:"build_snapshot"`
}

type status struct {
	Overall  overallStatus `json:"overall"`
	Statuses interface{}   `json:"statuses"`
}

type overallStatus struct {
	State string `json:"state"`
}

type metrics struct {
	UptimeInMillis        int64         `json:"uptime_in_millis"`
	ConcurrentConnections int64         `json:"concurrent_connections"`
	ResponseTimes         responseTimes `json:"response_times"`
	Process               process       `json:"process"`
}

type responseTimes struct {
	AvgInMillis float64 `json:"avg_in_millis"`
	MaxInMillis int64   `json:"max_in_millis"`
}

type process struct {
	Mem mem `json:"mem"`
}

type mem struct {
	HeapMaxInBytes  int64 `json:"heap_max_in_bytes"`
	HeapUsedInBytes int64 `json:"heap_used_in_bytes"`
}

const sampleConfig = `
  ## specify a list of one or more Kibana servers
  servers = ["http://localhost:5601"]

  ## Timeout for HTTP requests
  timeout = "5s"

  ## HTTP Basic Auth credentials
  # username = "username"
  # password = "pa$$word"

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
`

type Kibana struct {
	Local    bool
	Servers  []string
	Username string
	Password string
	Timeout  internal.Duration
	tls.ClientConfig

	client *http.Client
}

func NewKibana() *Kibana {
	return &Kibana{
		Timeout: internal.Duration{Duration: time.Second * 5},
	}
}

// perform status mapping
func mapHealthStatusToCode(s string) int {
	switch strings.ToLower(s) {
	case "green":
		return 1
	case "yellow":
		return 2
	case "red":
		return 3
	}
	return 0
}

// SampleConfig returns sample configuration for this plugin.
func (k *Kibana) SampleConfig() string {
	return sampleConfig
}

// Description returns the plugin description.
func (k *Kibana) Description() string {
	return "Read status information from one or more Kibana servers"
}

func (k *Kibana) Gather(acc telegraf.Accumulator) error {
	if k.client == nil {
		client, err := k.createHttpClient()

		if err != nil {
			return err
		}
		k.client = client
	}

	var wg sync.WaitGroup
	wg.Add(len(k.Servers))

	for _, serv := range k.Servers {
		go func(baseUrl string, acc telegraf.Accumulator) {
			defer wg.Done()
			if err := k.gatherKibanaStatus(baseUrl, acc); err != nil {
				acc.AddError(fmt.Errorf("[url=%s]: %s", baseUrl, err))
				return
			}
		}(serv, acc)
	}

	wg.Wait()
	return nil
}

func (k *Kibana) createHttpClient() (*http.Client, error) {
	tlsCfg, err := k.ClientConfig.TLSConfig()
	if err != nil {
		return nil, err
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsCfg,
		},
		Timeout: k.Timeout.Duration,
	}

	return client, nil
}

func (k *Kibana) gatherKibanaStatus(baseUrl string, acc telegraf.Accumulator) error {

	kibanaStatus := &kibanaStatus{}
	url := baseUrl + statusPath

	host, err := k.gatherJsonData(url, kibanaStatus)
	if err != nil {
		return err
	}

	fields := make(map[string]interface{})
	tags := make(map[string]string)

	tags["name"] = kibanaStatus.Name
	tags["source"] = host
	tags["version"] = kibanaStatus.Version.Number
	tags["status"] = kibanaStatus.Status.Overall.State

	fields["status_code"] = mapHealthStatusToCode(kibanaStatus.Status.Overall.State)

	fields["uptime_ms"] = kibanaStatus.Metrics.UptimeInMillis
	fields["concurrent_connections"] = kibanaStatus.Metrics.ConcurrentConnections
	fields["heap_max_bytes"] = kibanaStatus.Metrics.Process.Mem.HeapMaxInBytes
	fields["heap_used_bytes"] = kibanaStatus.Metrics.Process.Mem.HeapUsedInBytes
	fields["response_time_avg_ms"] = kibanaStatus.Metrics.ResponseTimes.AvgInMillis
	fields["response_time_max_ms"] = kibanaStatus.Metrics.ResponseTimes.MaxInMillis

	acc.AddFields("kibana", fields, tags)

	return nil
}

func (k *Kibana) gatherJsonData(url string, v interface{}) (host string, err error) {

	request, err := http.NewRequest("GET", url, nil)

	if (k.Username != "") || (k.Password != "") {
		request.SetBasicAuth(k.Username, k.Password)
	}

	response, err := k.client.Do(request)
	if err != nil {
		return "", err
	}

	defer response.Body.Close()

	if err = json.NewDecoder(response.Body).Decode(v); err != nil {
		return request.Host, err
	}

	return request.Host, nil
}

func init() {
	inputs.Add("kibana", func() telegraf.Input {
		return NewKibana()
	})
}
