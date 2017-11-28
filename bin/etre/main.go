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

	"gopkg.in/v1/yaml"

	"github.com/square/etre"
	"github.com/square/etre/api"
	"github.com/square/etre/cdc"
	"github.com/square/etre/config"
	"github.com/square/etre/db"
	"github.com/square/etre/entity"
	"github.com/square/etre/test/mock"

	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
)

var flagConfig string
var default_config_file = "/etc/etre/etre.yaml"
var default_addr = "127.0.0.1:8080"
var default_datasource_url = "localhost"
var default_database = "etre"
var default_entity_types = []string{}
var default_database_timeout_seconds = 5
var default_cdc_collection = "" // disabled
var default_delay_collection = "delay"
var default_cdc_fallback_file = ""
var default_cdc_write_retry_count = 3
var default_cdc_write_retry_wait = 50
var default_static_delay = -1 // if negative, system will use a dynamic delayer
var default_streamer_buffer_size = 100
var default_poll_interval = 2000

func init() {
	flag.StringVar(&flagConfig, "config", default_config_file, "Config file")
}

func main() {
	log.SetFlags(log.Lshortfile | log.Ldate | log.Lmicroseconds)
	log.SetOutput(os.Stdout)

	log.Printf("Etre %s\n", etre.VERSION)

	flag.Parse()

	// //////////////////////////////////////////////////////////////////////
	// Load config file
	// //////////////////////////////////////////////////////////////////////
	configFile := flagConfig
	log.Printf("config file: %s", configFile)

	bytes, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Fatalf("Cannot read -config file %s: %s", configFile, err)
	}

	config := config.Config{
		Server: config.ServerConfig{
			Addr: default_addr,
		},
		Datasource: config.DatasourceConfig{
			URL:      default_datasource_url,
			Database: default_database,
			Timeout:  default_database_timeout_seconds,
		},
		Entity: config.EntityConfig{
			Types: default_entity_types,
		},
		CDC: config.CDCConfig{
			Collection:      default_cdc_collection,
			FallbackFile:    default_cdc_fallback_file,
			WriteRetryCount: default_cdc_write_retry_count,
			WriteRetryWait:  default_cdc_write_retry_wait,
		},
		Feed: config.FeedConfig{
			StreamerBufferSize: default_streamer_buffer_size,
			PollInterval:       default_poll_interval,
		},
		Delay: config.DelayConfig{
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
	// CDC Store, Delayer, and Poller (if enabled)
	// //////////////////////////////////////////////////////////////////////
	var cdcs cdc.Store
	var dm cdc.Delayer
	var poller cdc.Poller
	if config.CDC.Collection != "" {
		log.Printf("CDC enabled on %s.%s\n", config.Datasource.Database, config.CDC.Collection)

		// Store
		wrp := cdc.RetryPolicy{
			RetryCount: config.CDC.WriteRetryCount,
			RetryWait:  config.CDC.WriteRetryWait,
		}
		cdcs = cdc.NewStore(
			conn,
			config.Datasource.Database,
			config.CDC.Collection,
			config.CDC.FallbackFile,
			wrp,
		)

		// Delayer
		var err error
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

		// Poller
		poller = cdc.NewPoller(
			cdcs,
			dm,
			config.Feed.StreamerBufferSize,
			time.NewTicker(time.Duration(config.Feed.PollInterval)*time.Millisecond),
		)
		go func() {
			if err := poller.Run(); err != nil {
				log.Fatalf("poller error: %s", err)
			}
		}()
	} else {
		log.Println("CDC disabled (config.cdc.collection not set)")

		// The CDC store and delayer must not be nil because the entity store
		// always updates them. But when CDC is disabled, the updates are no-ops.
		cdcs = &mock.CDCStore{}
		dm = &mock.Delayer{}

		// This results in a nil FeedFactory (below) which causes the /changes
		// controller returns http code 501 (StatusNotImplemented).
		poller = nil // cdc disabled
	}

	// //////////////////////////////////////////////////////////////////////
	// Feed Factory
	// //////////////////////////////////////////////////////////////////////
	var feedFactory cdc.FeedFactory
	if poller != nil {
		feedFactory = cdc.NewFeedFactory(poller, cdcs)
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
	// API
	// //////////////////////////////////////////////////////////////////////
	api := api.NewAPI(config.Server.Addr, entityStore, feedFactory)

	// If you want to add custom middleware for authentication, authorization,
	// etc., you should do that here. See https://echo.labstack.com/middleware
	// for more details.
	api.Use((func(h echo.HandlerFunc) echo.HandlerFunc {
		// This middleware will always set the username of the request to be
		// "admin". You can change this as necessary.
		return func(c echo.Context) error {
			c.Set("username", "admin")
			return h(c)
		}
	}))
	api.Use(middleware.Recover())

	// Start the web server.
	if config.Server.TLSCert != "" && config.Server.TLSKey != "" {
		log.Println("Listening on ", config.Server.Addr, " with TLS enabled")
		err = http.ListenAndServeTLS(config.Server.Addr, config.Server.TLSCert, config.Server.TLSKey, api)
	} else {
		log.Println("Listening on ", config.Server.Addr, " with TLS disabled")
		err = http.ListenAndServe(config.Server.Addr, api)
	}
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
}
