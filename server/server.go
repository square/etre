// Copyright 2018-2020, Square, Inc.

package server

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/mongo"

	"github.com/square/etre"
	"github.com/square/etre/api"
	"github.com/square/etre/app"
	"github.com/square/etre/auth"
	"github.com/square/etre/cdc"
	"github.com/square/etre/cdc/changestream"
	"github.com/square/etre/config"
	"github.com/square/etre/entity"
	"github.com/square/etre/metrics"
)

type Server struct {
	appCtx       app.Context
	api          *api.API
	mainDbClient *mongo.Client
	cdcDbClient  *mongo.Client
	stopChan     chan struct{}
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
		return fmt.Errorf("cannot load config: %s", err)
	}
	if err := config.Validate(cfg); err != nil {
		return fmt.Errorf("invalid config: %s", err)
	}
	s.appCtx.Config = cfg
	log.Printf("Config: %+v", config.Redact(s.appCtx.Config))

	// //////////////////////////////////////////////////////////////////////
	// CDC Store and Change Stream
	// //////////////////////////////////////////////////////////////////////
	if cfg.CDC.Disabled {
		log.Println("CDC and change feeds are disabled because cdc.disabled=true in config")
	} else {
		log.Printf("CDC enabled on %s.%s\n", cfg.Datasource.Database, config.CDC_COLLECTION)
		cdcClient, err := s.appCtx.Plugins.DB.Connect(cfg.CDC.Datasource)
		if err != nil {
			return fmt.Errorf("cannot connect to CDC datasource: %s", err)
		}
		s.cdcDbClient = cdcClient
		cdcColl := cdcClient.Database(cfg.Datasource.Database).Collection(config.CDC_COLLECTION)

		// Store
		wrp := cdc.RetryPolicy{
			RetryCount: cfg.CDC.WriteRetryCount,
			RetryWait:  cfg.CDC.WriteRetryWait,
		}
		s.appCtx.CDCStore = cdc.NewStore(cdcColl, cfg.CDC.FallbackFile, wrp)

		s.appCtx.ChangesServer = changestream.NewMongoDBServer(changestream.ServerConfig{
			CDCCollection: cdcColl,
			MaxClients:    cfg.CDC.ChangeStream.MaxClients,
			BufferSize:    cfg.CDC.ChangeStream.BufferSize,
		})

		s.appCtx.StreamerFactory = changestream.ServerStreamFactory{
			Server: s.appCtx.ChangesServer,
			Store:  s.appCtx.CDCStore,
		}
	}

	// //////////////////////////////////////////////////////////////////////
	// Entity Store and Validator
	// //////////////////////////////////////////////////////////////////////
	mainClient, err := s.appCtx.Plugins.DB.Connect(cfg.Datasource)
	if err != nil {
		return fmt.Errorf("cannot connect to main datasource: %s", err)
	}
	s.mainDbClient = mainClient
	coll := make(map[string]*mongo.Collection, len(cfg.Entity.Types))
	for _, entityType := range cfg.Entity.Types {
		coll[entityType] = mainClient.Database(cfg.Datasource.Database).Collection(entityType)
	}
	s.appCtx.EntityStore = entity.NewStore(coll, s.appCtx.CDCStore)
	s.appCtx.EntityValidator = entity.NewValidator(cfg.Entity.Types)

	// //////////////////////////////////////////////////////////////////////
	// Auth
	// //////////////////////////////////////////////////////////////////////
	acls, err := MapConfigACLRoles(cfg.Security.ACL)
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
	cdcEnabled := !s.appCtx.Config.CDC.Disabled

	// Verify we can connect to the db.
	mainDbDoneChan := make(chan struct{})
	log.Printf("Connecting to main database: %s", s.appCtx.Config.Datasource.URL)
	go s.connectToDatasource(s.appCtx.Config.Datasource, s.mainDbClient, mainDbDoneChan)

	var cdcDbDoneChan chan struct{}
	if cdcEnabled {
		log.Printf("Connecting to CDC database: %s", s.appCtx.Config.CDC.Datasource.URL)
		cdcDbDoneChan = make(chan struct{})
		go s.connectToDatasource(s.appCtx.Config.CDC.Datasource, s.cdcDbClient, cdcDbDoneChan)
	}

	notifyTimeout := time.NewTimer(2100 * time.Millisecond)
DB_CONN_WAIT:
	for {
		select {
		case <-mainDbDoneChan:
			log.Println("Connected to main database")
			mainDbDoneChan = nil
			if cdcDbDoneChan == nil {
				break DB_CONN_WAIT
			}
		case <-cdcDbDoneChan:
			log.Println("Connected to CDC database")
			cdcDbDoneChan = nil
			if mainDbDoneChan == nil {
				break DB_CONN_WAIT
			}
		case <-notifyTimeout.C:
			notifyTimeout.Stop()
			log.Println("WARNING: Etre offline until connected to databases")
		case <-s.stopChan:
			return nil
		}
	}
	notifyTimeout.Stop()

	if cdcEnabled {
		go func() {
			for {
				if err := s.appCtx.ChangesServer.Run(); err != nil {
					log.Printf("ERROR: change stream server: %s", err)
				}
				time.Sleep(100 * time.Millisecond)
			}
		}()
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

func (s *Server) stopped() bool {
	select {
	case <-s.stopChan:
		return true
	default:
		return false
	}
}

func (s *Server) connectToDatasource(ds config.DatasourceConfig, client *mongo.Client, doneChan chan struct{}) {
	defer close(doneChan)
	firstError := true
	for !s.stopped() {
		err := client.Ping(context.TODO(), nil)
		if err == nil {
			return
		}
		if firstError {
			log.Printf("Error connecting to %s: %s. Will retry every 500ms until successful.", ds.URL, err)
			firstError = false
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func MapConfigACLRoles(aclRoles []config.ACL) ([]auth.ACL, error) {
	acls := make([]auth.ACL, len(aclRoles))
	for i, acl := range aclRoles {
		acls[i] = auth.ACL{
			Role:              acl.Role,
			Admin:             acl.Admin,
			Read:              acl.Read,
			Write:             acl.Write,
			CDC:               acl.CDC,
			TraceKeysRequired: acl.TraceKeysRequired,
		}
	}
	return acls, nil
}
