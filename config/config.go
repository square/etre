// Copyright 2017-2021, Square, Inc.

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
	DEFAULT_DB_QUERY_TIMEOUT               = "2s"
	DEFAULT_DB_MIN_CONN                    = 10
	DEFAULT_DB_MAX_CONN                    = 1000
	DEFAULT_CDC_WRITE_RETRY_COUNT          = 2
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

var reservedNames = []string{"entity", "entities", "cdc", "etre"}

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
			MinConnections: DEFAULT_DB_MIN_CONN,
			MaxConnections: DEFAULT_DB_MAX_CONN,
		},
		CDC: CDCConfig{
			Datasource: DatasourceConfig{
				URL:            DEFAULT_DATASOURCE_URL,
				Database:       DEFAULT_DB,
				ConnectTimeout: DEFAULT_DB_CONNECT_TIMEOUT,
				QueryTimeout:   DEFAULT_DB_QUERY_TIMEOUT,
				MinConnections: DEFAULT_DB_MIN_CONN,
				MaxConnections: DEFAULT_DB_MAX_CONN,
			},
			FallbackFile:    DEFAULT_CDC_FALLBACK_FILE,
			WriteRetryCount: DEFAULT_CDC_WRITE_RETRY_COUNT,
			WriteRetryWait:  DEFAULT_CDC_WRITE_RETRY_WAIT,
			ChangeStream: ChangeStreamConfig{
				MaxClients: DEFAULT_CHANGESTREAM_MAX_CLIENTS,
				BufferSize: DEFAULT_CHANGESTREAM_BUFFER_SIZE,
			},
		},
		Security: SecurityConfig{},
		Metrics: MetricsConfig{
			QueryLatencySLA:             DEFAULT_QUERY_LATENCY_SLA,
			QueryProfileSampleRate:      DEFAULT_QUERY_PROFILE_SAMPLE_RATE,
			QueryProfileReportThreshold: DEFAULT_QUERY_PROFILE_REPORT_THRESHOLD,
		},
	}
}

// Load reads config file into base config. file is YAML with Config structure.
// base is usually a Default() config. Values in file config overwrite base config.
// Returns a new Config representing file applied to base.
func Load(file string, base Config) (Config, error) {
	file, err := filepath.Abs(file)
	if err != nil {
		return Config{}, err
	}
	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		// err includes file name, e.g. "read config file: open <file>: no such file or directory"
		return Config{}, fmt.Errorf("cannot read config file: %s", err)
	}
	if err := yaml.Unmarshal(bytes, &base); err != nil {
		return Config{}, fmt.Errorf("cannot decode YAML in %s: %s", file, err)
	}
	return base, nil
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
	Security   SecurityConfig   `yaml:"security"`
	Metrics    MetricsConfig    `yaml:"metrics"`
}

func Redact(c Config) Config {
	c.Datasource.Password = "<redacted>"
	c.CDC.Datasource.Password = "<redacted>"
	return c
}

type DatasourceConfig struct {
	URL            string `yaml:"url"`
	Database       string `yaml:"database"`
	ConnectTimeout string `yaml:"connect_timeout"`
	QueryTimeout   string `yaml:"query_timeout"`
	MinConnections uint64 `yaml:"min_connections"`
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
	if c.MinConnections == 0 {
		c.MinConnections = d.MinConnections
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
	MaxClients uint `yaml:"max_clients"`

	// BufferSize is the number of etre.CDCEvent to buffer per-client. If the
	// buffer fills because the client is slow to receive events, the server
	// drops the client.
	BufferSize uint `yaml:"buffer_size"`
}

type ServerConfig struct {
	Addr    string `yaml:"addr"`
	TLSCert string `yaml:"tls_cert"`
	TLSKey  string `yaml:"tls_key"`
	TLSCA   string `yaml:"tls_ca"`
}

type SecurityConfig struct {
	ACL []ACL `yaml:"acl"`
}

type ACL struct {
	Role              string   `yaml:"role"`
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
