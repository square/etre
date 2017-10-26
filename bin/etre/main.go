// Copyright 2017, Square, Inc.

package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/square/etre/api"
	"github.com/square/etre/cdc"
	"github.com/square/etre/db"
	"github.com/square/etre/entity"

	"gopkg.in/yaml.v2"
)

type DatasourceConfig struct {
	URL      string `yaml:"url"`
	Database string `yaml:"database"`
	Timeout  int    `yaml:"timeout"`

	// Certs
	TLSCert string `yaml:"tls-cert"`
	TLSKey  string `yaml:"tls-key"`
	TLSCA   string `yaml:"tls-ca"`

	// Credentials
	Username  string `yaml:"username"`
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
}

type DelayConfig struct {
	// The collection that delays are stored in.
	Collection string `yaml:"collection"`
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
	Addr string `yaml:"addr"`

	// Certs
	TLSCert string `yaml:"tls-cert"`
	TLSKey  string `yaml:"tls-key"`
	TLSCA   string `yaml:"tls-ca"`

	// Etre will look at this HTTP header to get the username of the requestor of
	// all API calls.
	UsernameHeader string `yaml:"username_header"`
}

type Config struct {
	Server     ServerConfig     `yaml:"server"`
	Datasource DatasourceConfig `yaml:"datasource"`
	Entity     EntityConfig     `yaml:"entity"`
	CDC        CDCConfig        `yaml:"cdc"`
	Delay      DelayConfig      `yaml:"delay"`
	Feed       FeedConfig       `yaml:"feed"`
}

var flagConfig string
var default_addr = "127.0.0.1:8080"
var default_database_url = "localhost"
var default_database = "etre"
var default_entity_types = []string{"node"} // @todo: remove
var default_database_timeout_seconds = 5
var default_cdc_collection = "cdc"
var default_delay_collection = "delay"
var default_cdc_fallback_file = ""
var default_cdc_write_retry_count = 3
var default_cdc_write_retry_wait = 50
var default_static_delay = -1 // if negative, system will use a dynamic delayer
var default_streamer_buffer_size = 100
var default_poll_interval = 2000

func init() {
	flag.StringVar(&flagConfig, "config", "", "Config file")
}

func main() {
	log.SetFlags(log.Lshortfile | log.Ldate | log.Lmicroseconds)
	log.SetOutput(os.Stdout)

	flag.Parse()

	// //////////////////////////////////////////////////////////////////////
	// Load config file
	// //////////////////////////////////////////////////////////////////////
	configFile := flagConfig
	log.Printf("config: %s", configFile)

	bytes, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	config := Config{
		Server: ServerConfig{
			Addr: default_addr,
		},
		Datasource: DatasourceConfig{
			URL:      default_database_url,
			Database: default_database,
			Timeout:  default_database_timeout_seconds,
		},
		Entity: EntityConfig{
			Types: default_entity_types,
		},
		CDC: CDCConfig{
			Collection:      default_cdc_collection,
			FallbackFile:    default_cdc_fallback_file,
			WriteRetryCount: default_cdc_write_retry_count,
			WriteRetryWait:  default_cdc_write_retry_wait,
		},
		Feed: FeedConfig{
			StreamerBufferSize: default_streamer_buffer_size,
			PollInterval:       default_poll_interval,
		},
		Delay: DelayConfig{
			Collection:  default_delay_collection,
			StaticDelay: default_static_delay,
		},
	}
	if err := yaml.Unmarshal(bytes, &config); err != nil {
		log.Println(err)
		os.Exit(1)
	}

	log.Printf("config: %+v\n", config)

	// //////////////////////////////////////////////////////////////////////
	// Load TLS if given
	// //////////////////////////////////////////////////////////////////////
	var tlsConfig *tls.Config
	if config.Datasource.TLSCert != "" && config.Datasource.TLSKey != "" {
		cert, err := tls.LoadX509KeyPair(config.Datasource.TLSCert, config.Datasource.TLSKey)
		if err != nil {
			log.Fatal(err)
		}

		caCert, err := ioutil.ReadFile(config.Datasource.TLSCA)
		if err != nil {
			log.Fatal(err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caCertPool,
		}
		tlsConfig.BuildNameToCertificate()
		log.Println("TLS Loaded")
	} else {
		log.Println("TLS cert and key not given")
	}

	// //////////////////////////////////////////////////////////////////////
	// Build DB dbCredentials map
	// //////////////////////////////////////////////////////////////////////
	dbCredentials := make(map[string]string)
	if config.Datasource.Username != "" && config.Datasource.Source != "" && config.Datasource.Mechanism != "" {
		dbCredentials["username"] = config.Datasource.Username
		dbCredentials["source"] = config.Datasource.Source
		dbCredentials["mechanism"] = config.Datasource.Mechanism
	}

	// //////////////////////////////////////////////////////////////////////
	// Create DB Connection Pool
	// //////////////////////////////////////////////////////////////////////
	conn := db.NewConnector(config.Datasource.URL, config.Datasource.Timeout, tlsConfig, dbCredentials)

	// Verify we can connect to the db.
	_, err = conn.Connect()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	// //////////////////////////////////////////////////////////////////////
	// CDC Store
	// //////////////////////////////////////////////////////////////////////
	wrp := cdc.RetryPolicy{
		RetryCount: config.CDC.WriteRetryCount,
		RetryWait:  config.CDC.WriteRetryWait,
	}
	cdcs := cdc.NewStore(
		conn,
		config.Datasource.Database,
		config.CDC.Collection,
		config.CDC.FallbackFile,
		wrp,
	)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	// //////////////////////////////////////////////////////////////////////
	// Delayer
	// //////////////////////////////////////////////////////////////////////
	var dm cdc.Delayer
	if config.Delay.StaticDelay >= 0 {
		dm, err = cdc.NewStaticDelayer(
			config.Delay.StaticDelay,
		)
	} else {
		dm, err = cdc.NewDynamicDelayer(
			conn,
			config.Datasource.Database,
			config.Delay.Collection,
		)
	}
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	// //////////////////////////////////////////////////////////////////////
	// Entity Store
	// //////////////////////////////////////////////////////////////////////
	entityStore, err := entity.NewStore(
		conn,
		config.Datasource.Database,
		config.Entity.Types,
		cdcs,
		dm,
	)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	// //////////////////////////////////////////////////////////////////////
	// Create and Run Poller
	// //////////////////////////////////////////////////////////////////////
	poller := cdc.NewPoller(
		cdcs,
		dm,
		config.Feed.StreamerBufferSize,
		time.NewTicker(time.Duration(config.Feed.PollInterval)*time.Millisecond),
	)
	go func() {
		err = poller.Run()
		if err != nil {
			log.Println(err)
		}
	}()

	// //////////////////////////////////////////////////////////////////////
	// Feed Factory
	// //////////////////////////////////////////////////////////////////////
	feedFactory := cdc.NewFeedFactory(poller, cdcs)

	// //////////////////////////////////////////////////////////////////////
	// Launch App (initialize router/API, start server)
	// //////////////////////////////////////////////////////////////////////
	router := &api.Router{
		UsernameHeader: config.Server.UsernameHeader,
	}
	api := api.NewAPI(config.Server.Addr, router, entityStore, feedFactory)

	// Start the web server.
	if config.Server.TLSCert != "" && config.Server.TLSKey != "" {
		log.Println("Listening on ", config.Server.Addr, " with TLS enabled")
		err = http.ListenAndServeTLS(config.Server.Addr, config.Server.TLSCert, config.Server.TLSKey, api.Router())
	} else {
		log.Println("Listening on ", config.Server.Addr, " with TLS disabled")
		err = http.ListenAndServe(config.Server.Addr, api.Router())
	}
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
}
