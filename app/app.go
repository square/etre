package app

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/square/etre/auth"
	"github.com/square/etre/cdc"
	"github.com/square/etre/config"
	"github.com/square/etre/db"
	"github.com/square/etre/entity"
	"github.com/square/etre/metrics"
	"github.com/square/etre/test/mock"
)

type Context struct {
	Config          config.Config
	EntityStore     entity.Store
	EntityValidator entity.Validator
	CDCStore        cdc.Store
	FeedFactory     cdc.FeedFactory
	AuthPlugin      auth.Plugin
	MetricsStore    metrics.Store
	MetricsFactory  metrics.Factory
}

func DefaultContext(config config.Config) Context {
	// //////////////////////////////////////////////////////////////////////
	// Database
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

	dbCredentials := make(map[string]string)
	if config.Datasource.Username != "" && config.Datasource.Source != "" && config.Datasource.Mechanism != "" {
		dbCredentials["username"] = config.Datasource.Username
		dbCredentials["source"] = config.Datasource.Source
		dbCredentials["mechanism"] = config.Datasource.Mechanism
	}

	conn := db.NewConnector(config.Datasource.URL, config.Datasource.Timeout, tlsConfig, dbCredentials)

	// //////////////////////////////////////////////////////////////////////
	// CDC Store, Delayer, and Poller (if enabled)
	// //////////////////////////////////////////////////////////////////////

	var cdcStore cdc.Store
	var dm cdc.Delayer
	var poller cdc.Poller
	if config.CDC.Collection != "" {
		log.Printf("CDC enabled on %s.%s\n", config.Datasource.Database, config.CDC.Collection)

		// Store
		wrp := cdc.RetryPolicy{
			RetryCount: config.CDC.WriteRetryCount,
			RetryWait:  config.CDC.WriteRetryWait,
		}
		cdcStore = cdc.NewStore(
			conn,
			config.Datasource.Database,
			config.CDC.Collection,
			config.CDC.FallbackFile,
			wrp,
		)

		// Delayer
		var err error
		if config.CDC.StaticDelay >= 0 {
			dm, err = cdc.NewStaticDelayer(config.CDC.StaticDelay)
		} else {
			dm, err = cdc.NewDynamicDelayer(
				conn,
				config.Datasource.Database,
				config.CDC.DelayCollection,
			)
		}
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}

		// Poller
		poller = cdc.NewPoller(
			cdcStore,
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
		cdcStore = &mock.CDCStore{}
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
		feedFactory = cdc.NewFeedFactory(poller, cdcStore)
	}

	// //////////////////////////////////////////////////////////////////////
	// Entity Store
	// //////////////////////////////////////////////////////////////////////

	entityStore := entity.NewStore(
		conn,
		config.Datasource.Database,
		config.Entity.Types,
		cdcStore,
		dm,
	)
	entityValidator := entity.NewValidator(config.Entity.Types)

	// //////////////////////////////////////////////////////////////////////
	// Auth and metrics
	// //////////////////////////////////////////////////////////////////////

	authPlugin := auth.NewManager(nil, auth.AllowAll{})
	metricsStore := metrics.NewMemoryStore()

	return Context{
		Config:          config,
		EntityStore:     entityStore,
		EntityValidator: entityValidator,
		CDCStore:        cdcStore,
		FeedFactory:     feedFactory,
		AuthPlugin:      authPlugin,
		MetricsStore:    metricsStore,
		MetricsFactory:  metrics.GroupFactory{Store: metricsStore},
	}
}
