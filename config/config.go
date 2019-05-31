// Copyright 2017-2019, Square, Inc.

package config

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v2"
)

const (
	DEFAULT_ADDR                  = "127.0.0.1:8050"
	DEFAULT_DATASOURCE_URL        = "mongodb://localhost:27017"
	DEFAULT_DB                    = "etre_dev"
	DEFAULT_DB_TIMEOUT            = 5000
	DEFAULT_CDC_COLLECTION        = "" // disabled
	DEFAULT_CDC_DELAY_COLLECTION  = "cdc_delay"
	DEFAULT_CDC_WRITE_RETRY_COUNT = 3
	DEFAULT_CDC_WRITE_RETRY_WAIT  = 50
	DEFAULT_CDC_FALLBACK_FILE     = "/tmp/etre-cdc.json"
	DEFAULT_CDC_STATIC_DELAY      = -1 // if negative, system will use a dynamic delayer
	DEFAULT_FEED_BUFFER_SIZE      = 100
	DEFAULT_FEED_POLL_INTERVAL    = 2000
	DEFAULT_ENTITY_TYPE           = "host"
	DEFAULT_QUERY_LATENCY_SLA     = "1s"
)

var reservedNames = []string{"entity", "entities"}

func Default() Config {
	return Config{
		Entity: EntityConfig{
			Types: []string{DEFAULT_ENTITY_TYPE},
		},
		Server: ServerConfig{
			Addr: DEFAULT_ADDR,
		},
		Datasource: DatasourceConfig{
			URL:      DEFAULT_DATASOURCE_URL,
			Database: DEFAULT_DB,
			Timeout:  DEFAULT_DB_TIMEOUT,
		},
		CDC: CDCConfig{
			Collection:      DEFAULT_CDC_COLLECTION,
			FallbackFile:    DEFAULT_CDC_FALLBACK_FILE,
			WriteRetryCount: DEFAULT_CDC_WRITE_RETRY_COUNT,
			WriteRetryWait:  DEFAULT_CDC_WRITE_RETRY_WAIT,
			DelayCollection: DEFAULT_CDC_DELAY_COLLECTION,
			StaticDelay:     DEFAULT_CDC_STATIC_DELAY,
		},
		Feed: FeedConfig{
			StreamerBufferSize: DEFAULT_FEED_BUFFER_SIZE,
			PollInterval:       DEFAULT_FEED_POLL_INTERVAL,
		},
		ACL: ACLConfig{},
		Metrics: MetricsConfig{
			QueryLatencySLA: DEFAULT_QUERY_LATENCY_SLA,
		},
	}
}

func Load(file string) (Config, error) {
	file, err := filepath.Abs(file)
	if err != nil {
		return Config{}, err
	}

	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		// err includes file name, e.g. "read config file: open <file>: no such file or directory"
		return Config{}, fmt.Errorf("cannot read config file: %s", err)
	}

	config := Default()
	if err := yaml.Unmarshal(bytes, &config); err != nil {
		return Config{}, fmt.Errorf("cannot decode YAML in %s: %s", file, err)
	}

	if len(config.Entity.Types) == 0 {
		return Config{}, fmt.Errorf("invalid config: no entity types specified in %s", file)
	}

	// Ensure no entityType name is a reserved word
	for _, t := range config.Entity.Types {
		for _, r := range reservedNames {
			if t != r {
				continue
			}
			return Config{}, fmt.Errorf("entity type %s is a reserved word: %s", t, strings.Join(reservedNames, ","))
		}
	}

	return config, nil
}

type Config struct {
	Server     ServerConfig     `yaml:"server"`
	Datasource DatasourceConfig `yaml:"datasource"`
	Entity     EntityConfig     `yaml:"entity"`
	CDC        CDCConfig        `yaml:"cdc"`
	Feed       FeedConfig       `yaml:"feed"`
	ACL        ACLConfig        `yaml:"acl"`
	Metrics    MetricsConfig    `yaml:"metrics"`
}

type DatasourceConfig struct {
	URL      string `yaml:"url"`
	Database string `yaml:"database"`
	Timeout  int    `yaml:"timeout"`

	// Certs
	TLSCert string `yaml:"tls_cert"`
	TLSKey  string `yaml:"tls_key"`
	TLSCA   string `yaml:"tls_ca"`

	// Credentials
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
	Source    string `yaml:"source"`
	Mechanism string `yaml:"mechanism"`
}

type EntityConfig struct {
	Types []string `yaml:"types"`
}

type CDCConfig struct {
	// The collection that CDC events are stored in.
	Collection string `yaml:"collection"`
	// If set, CDC events will attempt to be written to this file if they cannot
	// be written to mongo.
	FallbackFile string `yaml:"fallback_file"`
	// Number of times CDC events will retry writing to mongo in the event of an error.
	WriteRetryCount int `yaml:"write_retry_count"`
	// Wait time in milliseconds between write retry events.
	WriteRetryWait int `yaml:"write_retry_wait"` // milliseconds
	// The collection that delays are stored in.
	DelayCollection string `yaml:"delay_collection"`
	// If this value is positive, the delayer will always return a max timestamp
	// that is time.Now() minus this config value. If this value is negative,
	// the delayer will return a max timestamp that dynamically changes
	// depending on the active API calls into Etre. Units in milliseconds.
	StaticDelay int `yaml:"static_delay"`
}

type FeedConfig struct {
	// The buffer size that a streamer has when consuming from the poller. If the
	// poller fills the buffer up then the streamer will error out since it won't
	// be able to catch up to the poller anymore.
	StreamerBufferSize int `yaml:"streamer_buffer_size"`
	// The amount of time that the poller will sleep between polls, in milliseconds.
	PollInterval int `yaml:"poll_interval"`
}

type ServerConfig struct {
	Addr    string `yaml:"addr"`
	TLSCert string `yaml:"tls_cert"`
	TLSKey  string `yaml:"tls_key"`
	TLSCA   string `yaml:"tls_ca"`

	// If client does not set X-Etre-Version, default to this version.
	DefaultClientVersion string `yaml:"default_client_version"`
}

type ACLConfig struct {
	Roles []ACL `yaml:"roles"`
}

type ACL struct {
	Name              string   `yaml:"name"`
	Admin             bool     `yaml:"admin"`
	Read              []string `yaml:"read"`
	Write             []string `yaml:"write"`
	TraceKeysRequired []string `yaml:"trace_keys_required"`
}

type MetricsConfig struct {
	QueryLatencySLA string `yaml:"query_latency_sla"` // duration string
}
