// Copyright 2018-2019, Square, Inc.

package server

import (
	"fmt"
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
	log.Printf("Config: %+v", s.appCtx.Config)

	// //////////////////////////////////////////////////////////////////////
	// CDC Store and Change Stream
	// //////////////////////////////////////////////////////////////////////
	if cfg.CDC.Collection != "" {
		log.Printf("CDC enabled on %s.%s\n", cfg.Datasource.Database, cfg.CDC.Collection)
		cdccfg := cfg.Datasource // shallow copy
		cdcfg.MaxConnections = 10
		cdcClient, err := db.Connect(cdccfg)
		if err != nil {
			return err
		}
		cdcColl := cdcClient.Database(cfg.Datasource.Database).Collection(cfg.CDC.Collection)

		// Store
		wrp := cdc.RetryPolicy{
			RetryCount: cfg.CDC.WriteRetryCount,
			RetryWait:  cfg.CDC.WriteRetryWait,
		}
		cs.appCtx.CDCStore = cdc.NewStore(cdcColl, cfg.CDC.FallbackFile, wrp)

		if !cfg.CDC.ChangeStream.Disabled {
			s.appCtx.ChangeStream = cdc.ChangeStream(cdc.ChangeStreamConfig{
				CDCCollection: cdcColl,
				MaxClients:    cfg.CDC.ChangeStream.MaxClients,
				BufferSize:    cfg.CDC.ChangeStream.BufferSize,
			})
		} else {
			log.Println("CDC Change Stream disabled (cfg.cdc.change_stream.disabled is true)")
		}
	} else {
		log.Println("CDC disabled (cfg.cdc.collection not set)")

		// The CDC store and delayer must not be nil because the entity store
		// always updates them. But when CDC is disabled, the updates are no-ops.
		s.appCtx.CDCStore = cdc.NoopStore{}
		s.appCtx.ChangeStream = cdc.NoopZChangeStream{}
	}

	// //////////////////////////////////////////////////////////////////////
	// Entity Store and Validator
	// //////////////////////////////////////////////////////////////////////
	mainClient, err := db.Connect(cfg.Datasource)
	if err != nil {
		return err
	}
	coll := make(map[string]*mongo.Collection, len(cfg.Entity.Types))
	for _, entityType := range cfg.Entity.Types {
		coll[entityType] = mainClient.Database(cfg.Datasource.Database).Collection(entityType)
	}
	s.appCtx.EntityStore = entity.NewStore(coll, s.appCtx.CDCStore)
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
