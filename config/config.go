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
	DEFAULT_ADDR                           = "127.0.0.1:32084"
	DEFAULT_DATASOURCE_URL                 = "mongodb://localhost:27017"
	DEFAULT_DB                             = "etre_dev"
	DEFAULT_DB_CONNECT_TIMEOUT             = "5s"
	DEFAULT_DB_QUERY_TIMEOUT               = "5s"
	DEFAULT_DB_MAX_CONN                    = 500
	DEFAULT_CDC_COLLECTION                 = "" // disabled
	DEFAULT_CDC_WRITE_RETRY_COUNT          = 3
	DEFAULT_CDC_WRITE_RETRY_WAIT           = 2
	DEFAULT_CDC_FALLBACK_FILE              = "/tmp/etre-cdc.json"
	DEFAULT_CHANGESTREAM_BUFFER_SIZE       = 100
	DEFAULT_CHANGESTREAM_MAX_CLIENTS       = 100
	DEFAULT_ENTITY_TYPE                    = "host"
	DEFAULT_QUERY_LATENCY_SLA              = "1s"
	DEFAULT_QUERY_PROFILE_SAMPLE_RATE      = 0.2
	DEFAULT_QUERY_PROFILE_REPORT_THRESHOLD = "500ms"
)

const CDC_COLLECTION = "cdc"

var reservedNames = []string{"entity", "entities", "cdc"}

func Default() Config {
	return Config{
		Entity: EntityConfig{
			Types: []string{DEFAULT_ENTITY_TYPE},
		},
		Server: ServerConfig{
			Addr: DEFAULT_ADDR,
		},
		Datasource: DatasourceConfig{
			URL:            DEFAULT_DATASOURCE_URL,
			Database:       DEFAULT_DB,
			ConnectTimeout: DEFAULT_DB_CONNECT_TIMEOUT,
			QueryTimeout:   DEFAULT_DB_QUERY_TIMEOUT,
			MaxConnections: DEFAULT_DB_MAX_CONN,
		},
		CDC: CDCConfig{
			Datasource:      DatasourceConfig{}, // default to Config.Datasource
			FallbackFile:    DEFAULT_CDC_FALLBACK_FILE,
			WriteRetryCount: DEFAULT_CDC_WRITE_RETRY_COUNT,
			WriteRetryWait:  DEFAULT_CDC_WRITE_RETRY_WAIT,
			ChangeStream: ChangeStreamConfig{
				MaxClients: DEFAULT_CHANGESTREAM_MAX_CLIENTS,
				BufferSize: DEFAULT_CHANGESTREAM_BUFFER_SIZE,
			},
		},
		ACL: ACLConfig{},
		Metrics: MetricsConfig{
			QueryLatencySLA:             DEFAULT_QUERY_LATENCY_SLA,
			QueryProfileSampleRate:      DEFAULT_QUERY_PROFILE_SAMPLE_RATE,
			QueryProfileReportThreshold: DEFAULT_QUERY_PROFILE_REPORT_THRESHOLD,
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
	var config Config
	if err := yaml.Unmarshal(bytes, &config); err != nil {
		return Config{}, fmt.Errorf("cannot decode YAML in %s: %s", file, err)
	}
	return config, nil
}

func Validate(config Config) error {
	if len(config.Entity.Types) == 0 {
		return fmt.Errorf("no entity types specified")
	}

	for _, t := range config.Entity.Types {
		for _, r := range reservedNames {
			if t != r {
				continue
			}
			return fmt.Errorf("entity type %s is a reserved word: %s", t, strings.Join(reservedNames, ","))
		}
	}

	return nil
}

type Config struct {
	Server     ServerConfig     `yaml:"server"`
	Datasource DatasourceConfig `yaml:"datasource"`
	Entity     EntityConfig     `yaml:"entity"`
	CDC        CDCConfig        `yaml:"cdc"`
	ACL        ACLConfig        `yaml:"acl"`
	Metrics    MetricsConfig    `yaml:"metrics"`
}

type DatasourceConfig struct {
	URL            string `yaml:"url"`
	Database       string `yaml:"database"`
	ConnectTimeout string `yaml:"connect_timeout"`
	QueryTimeout   string `yaml:"query_timeout"`
	MaxConnections uint64 `yaml:"max_connections"`

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

func (c DatasourceConfig) WithDefaults(d DatasourceConfig) DatasourceConfig {
	if c.URL == "" {
		c.URL = d.URL
	}
	if c.Database == "" {
		c.Database = d.Database
	}
	if c.ConnectTimeout == "" {
		c.ConnectTimeout = d.ConnectTimeout
	}
	if c.QueryTimeout == "" {
		c.QueryTimeout = d.QueryTimeout
	}
	if c.MaxConnections == 0 {
		c.MaxConnections = d.MaxConnections
	}

	if c.TLSCert == "" {
		c.TLSCert = d.TLSCert
	}
	if c.TLSKey == "" {
		c.TLSKey = d.TLSKey
	}
	if c.TLSCA == "" {
		c.TLSCA = d.TLSCA
	}

	if c.Username == "" {
		c.Username = d.Username
	}
	if c.Password == "" {
		c.Password = d.Password
	}
	if c.Source == "" {
		c.Source = d.Source
	}
	if c.Mechanism == "" {
		c.Mechanism = d.Mechanism
	}
	return c
}

type EntityConfig struct {
	Types []string `yaml:"types"`
}

type CDCConfig struct {
	Disabled bool `yaml:"disabled"`

	Datasource DatasourceConfig `yaml:"datasource"`

	// If set, CDC events will attempt to be written to this file if they cannot
	// be written to mongo.
	FallbackFile string `yaml:"fallback_file"`
	// Number of times CDC events will retry writing to mongo in the event of an error.
	WriteRetryCount int `yaml:"write_retry_count"`
	// Wait time in milliseconds between write retry events.
	WriteRetryWait int `yaml:"write_retry_wait"` // milliseconds
	// The collection that delays are stored in.

	ChangeStream ChangeStreamConfig `yaml:"change_stream"`
}

type ChangeStreamConfig struct {
	// The buffer size that a streamer has when consuming from the poller. If the
	// poller fills the buffer up then the streamer will error out since it won't
	// be able to catch up to the poller anymore.
	BufferSize uint `yaml:"buffer_size"`

	// The amount of time that the poller will sleep between polls, in milliseconds.
	MaxClients uint `yaml:"max_clients"`
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
	QueryLatencySLA             string  `yaml:"query_latency_sla"` // duration string
	QueryProfileSampleRate      float64 `yaml:"query_profile_sample_rate"`
	QueryProfileReportThreshold string  `yaml:"query_profile_report_threshold"` // duration string
}
