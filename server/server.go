// Copyright 2018-2019, Square, Inc.

package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/square/etre"
	"github.com/square/etre/api"
	"github.com/square/etre/app"
	"github.com/square/etre/auth"
	"github.com/square/etre/cdc"
	"github.com/square/etre/config"
	"github.com/square/etre/db"
	"github.com/square/etre/entity"
	"github.com/square/etre/metrics"
)

type Server struct {
	appCtx   app.Context
	api      *api.API
	poller   cdc.Poller
	stopChan chan struct{}
	conn     db.Connector
}

func NewServer(appCtx app.Context) *Server {
	return &Server{
		appCtx:   appCtx,
		stopChan: make(chan struct{}),
	}
}

func (s *Server) Boot(configFile string) error {
	log.SetFlags(log.Lshortfile | log.Ldate | log.Lmicroseconds)
	log.Printf("Etre %s\n", etre.VERSION)

	// Load config file
	s.appCtx.ConfigFile = configFile
	cfg, err := s.appCtx.Hooks.LoadConfig(s.appCtx)
	if err != nil {
		return fmt.Errorf("error loading config: %s", err)
	}
	s.appCtx.Config = cfg

	// //////////////////////////////////////////////////////////////////////
	// Database
	// //////////////////////////////////////////////////////////////////////
	var tlsConfig *tls.Config
	if (cfg.Datasource.TLSCert != "" && cfg.Datasource.TLSKey != "") || cfg.Datasource.TLSCA != "" {
		tlsConfig = &tls.Config{}

		// Root CA
		if cfg.Datasource.TLSCA != "" {
			caCert, err := ioutil.ReadFile(cfg.Datasource.TLSCA)
			if err != nil {
				return err
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			tlsConfig.RootCAs = caCertPool
			log.Println("TLS root CA loaded")
		}

		// Cert and key
		if cfg.Datasource.TLSCert != "" && cfg.Datasource.TLSKey != "" {
			cert, err := tls.LoadX509KeyPair(cfg.Datasource.TLSCert, cfg.Datasource.TLSKey)
			if err != nil {
				return err
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
			tlsConfig.BuildNameToCertificate()
			log.Println("TLS cert and key loaded")
		}
	} else {
		log.Println("TLS cert and key not given")
	}
	dbCredentials := map[string]string{
		"username":  cfg.Datasource.Username,
		"password":  cfg.Datasource.Password,
		"source":    cfg.Datasource.Source,
		"mechanism": cfg.Datasource.Mechanism,
	}
	s.conn = db.NewConnector(s.appCtx.Config.Datasource.URL, cfg.Datasource.Timeout, tlsConfig, dbCredentials)

	// //////////////////////////////////////////////////////////////////////
	// CDC Store, Delayer, and Poller (if enabled)
	// //////////////////////////////////////////////////////////////////////
	var cdcStore cdc.Store
	var dm cdc.Delayer
	if cfg.CDC.Collection != "" {
		log.Printf("CDC enabled on %s.%s\n", cfg.Datasource.Database, cfg.CDC.Collection)

		// Store
		wrp := cdc.RetryPolicy{
			RetryCount: cfg.CDC.WriteRetryCount,
			RetryWait:  cfg.CDC.WriteRetryWait,
		}
		cdcStore = cdc.NewStore(
			s.conn,
			cfg.Datasource.Database,
			cfg.CDC.Collection,
			cfg.CDC.FallbackFile,
			wrp,
		)

		// Delayer
		var err error
		if cfg.CDC.StaticDelay >= 0 {
			dm, err = cdc.NewStaticDelayer(cfg.CDC.StaticDelay)
		} else {
			dm, err = cdc.NewDynamicDelayer(
				s.conn,
				cfg.Datasource.Database,
				cfg.CDC.DelayCollection,
			)
		}
		if err != nil {
			return err
		}

		// Poller
		log.Printf("CDC feed poll interval: %d ms", cfg.Feed.PollInterval)
		s.poller = cdc.NewPoller(
			cdcStore,
			dm,
			cfg.Feed.StreamerBufferSize,
			time.NewTicker(time.Duration(cfg.Feed.PollInterval)*time.Millisecond),
		)
	} else {
		log.Println("CDC disabled (cfg.cdc.collection not set)")

		// The CDC store and delayer must not be nil because the entity store
		// always updates them. But when CDC is disabled, the updates are no-ops.
		cdcStore = cdc.NoopStore{}
		dm = cdc.NoopDelayer{}

		// This results in a nil FeedFactory (below) which causes the /changes
		// controller returns http code 501 (StatusNotImplemented).
		s.poller = nil // cdc disabled
	}
	s.appCtx.CDCStore = cdcStore

	// //////////////////////////////////////////////////////////////////////
	// Feed Factory
	// //////////////////////////////////////////////////////////////////////
	if s.poller != nil {
		s.appCtx.FeedFactory = cdc.NewFeedFactory(s.poller, cdcStore)
	}

	// //////////////////////////////////////////////////////////////////////
	// Entity Store
	// //////////////////////////////////////////////////////////////////////
	s.appCtx.EntityStore = entity.NewStore(
		cdcStore,
		dm,
	)
	s.appCtx.EntityValidator = entity.NewValidator(cfg.Entity.Types)

	// //////////////////////////////////////////////////////////////////////
	// Auth
	// //////////////////////////////////////////////////////////////////////
	acls, err := MapConfigACLRoles(cfg.ACL.Roles)
	if err != nil {
		return fmt.Errorf("invalid ACL role: %s", err)
	}
	s.appCtx.Auth = auth.NewManager(acls, s.appCtx.Plugins.Auth)

	// //////////////////////////////////////////////////////////////////////
	// Metrics
	// //////////////////////////////////////////////////////////////////////
	if _, err := time.ParseDuration(s.appCtx.Config.Metrics.QueryLatencySLA); err != nil {
		return fmt.Errorf("invalid config.metrics.query_latency_sla: %s: %s", s.appCtx.Config.Metrics.QueryLatencySLA, err)
	}

	s.appCtx.MetricsStore = metrics.NewMemoryStore()
	s.appCtx.MetricsFactory = metrics.GroupFactory{Store: s.appCtx.MetricsStore}
	s.appCtx.SystemMetrics = metrics.NewSystemMetrics()

	// //////////////////////////////////////////////////////////////////////
	// API
	// //////////////////////////////////////////////////////////////////////
	s.api = api.NewAPI(s.appCtx)

	log.Printf("Config: %+v", s.appCtx.Config)

	return nil
}

func (s *Server) Run() error {
	// Verify we can connect to the db.
	// @todo: removing this causes mgo panic "Session already closed" after 1st query
	for {
		if s.stopped() {
			return nil
		}
		log.Printf("Verifying database connection to %s", s.appCtx.Config.Datasource.URL)
		if err := s.conn.Init(); err != nil {
			log.Printf("WARNING: cannot connect to %s: %s", s.appCtx.Config.Datasource.URL, err)
			continue
		}
		log.Printf("Connected to %s", s.appCtx.Config.Datasource.URL)
		break
	}

	if s.poller != nil {
		go s.runPoller()
	}

	// Run the API - this will block until the API is stopped (or encounters
	// some fatal error). If the RunAPI hook has been provided, call that instead
	// of the default api.Run.
	var err error
	if s.appCtx.Hooks.RunAPI != nil {
		err = s.appCtx.Hooks.RunAPI()
	} else {
		err = s.api.Run()
	}
	return err
}

func (s *Server) Stop() error {
	log.Println("Etre stopping...")
	close(s.stopChan)

	// Stop the API, using the StopAPI hook if provided and api.Stop otherwise.
	var err error
	if s.appCtx.Hooks.StopAPI != nil {
		err = s.appCtx.Hooks.StopAPI()
	} else {
		err = s.api.Stop()
	}
	return err
}

func (s *Server) API() *api.API {
	return s.api
}

func (s *Server) Context() app.Context {
	return s.appCtx
}

func (s *Server) runPoller() {
	if s.poller == nil {
		return
	}
	for {
		if s.stopped() {
			return
		}
		if err := s.poller.Run(); err != nil {
			log.Printf("poller error: %s (restarting in 1s)", err)
			time.Sleep(1 * time.Second)
		}
	}
}

func (s *Server) stopped() bool {
	select {
	case <-s.stopChan:
		return true
	default:
	}
	return false
}

func MapConfigACLRoles(aclRoles []config.ACL) ([]auth.ACL, error) {
	acls := make([]auth.ACL, len(aclRoles))
	for i, acl := range aclRoles {
		acls[i] = auth.ACL{
			Role:              acl.Name,
			Admin:             acl.Admin,
			Read:              acl.Read,
			Write:             acl.Write,
			TraceKeysRequired: acl.TraceKeysRequired,
		}
	}
	return acls, nil
}
