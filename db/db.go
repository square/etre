// Copyright 2020, Square, Inc.

package db

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/square/etre/config"
)

// Plugin is the db plugin. Implement this interface to enable custom db connections.
type Plugin interface {
	// Connect returns a mongo.Client connected to the database.
	Connect(cfg config.DatasourceConfig) (*mongo.Client, error)
}

type Default struct{}

func (d Default) Connect(cfg config.DatasourceConfig) (*mongo.Client, error) {
	tlsConfig, err := loadTLS(cfg)
	if err != nil {
		return nil, err
	}

	timeout, err := time.ParseDuration(cfg.ConnectTimeout)
	if err != nil {
		return nil, err
	}

	// SetServerSelectionTimeout is different and more important than SetConnectTimeout.
	// Internally, the mongo driver is polling and updating the topology,
	// i.e. the list of replicas/nodes in the cluster. SetServerSelectionTimeout
	// applies to selecting a node from the topology, which should be nearly
	// instantaneous when the cluster is ok _and_ when it's down. When a node
	// is down, it's reflected in the topology, so there's no need to wait for
	// another server because we only use one server: the master replica.
	// The 500ms below is really how long the driver will wait for the master
	// replica to come back online.
	//
	// SetConnectTimeout is what is seems: timeout when a connection is actually
	// made. This guards against slows networks, or the case when the mongo driver
	// thinks the master is online but really it's not.
	opts := options.Client().
		ApplyURI(cfg.URL).
		SetTLSConfig(tlsConfig).
		SetMaxPoolSize(cfg.MinConnections).
		SetMaxPoolSize(cfg.MaxConnections).
		SetConnectTimeout(timeout).
		SetServerSelectionTimeout(time.Duration(500 * time.Millisecond))

	if cfg.Username != "" {
		creds := options.Credential{
			AuthMechanism: cfg.Mechanism,
			AuthSource:    cfg.Source,
			Username:      cfg.Username,
			Password:      cfg.Password,
		}
		opts = opts.SetAuth(creds)
	} else {
		log.Printf("WARNING: No database username for %s specified in config. Authentication will fail unless MongoDB access control is disabled.", cfg.URL)
	}

	client, err := mongo.NewClient(opts)
	if err != nil {
		return nil, err
	}

	// mongo.Connect() does not actually connect:
	//   The Client.Connect method starts background goroutines to monitor the
	//   state of the deployment and does not do any I/O in the main goroutine to
	//   prevent the main goroutine from blocking. Therefore, it will not error if
	//   the deployment is down.
	// https://pkg.go.dev/go.mongodb.org/mongo-driver/mongo?tab=doc#Connect
	// The caller must call client.Ping() to actually connect. Consequently,
	// we don't need a context here. As long as there's not a bug in the mongo
	// driver, this won't block.
	if err := client.Connect(context.Background()); err != nil {
		return nil, err
	}
	return client, nil
}

func loadTLS(cfg config.DatasourceConfig) (*tls.Config, error) {
	var tlsConfig *tls.Config
	if (cfg.TLSCert != "" && cfg.TLSKey != "") || cfg.TLSCA != "" {
		tlsConfig = &tls.Config{}

		// Root CA
		if cfg.TLSCA != "" {
			caCert, err := ioutil.ReadFile(cfg.TLSCA)
			if err != nil {
				return nil, err
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			tlsConfig.RootCAs = caCertPool
			log.Println("TLS root CA loaded")
		}

		// Cert and key
		if cfg.TLSCert != "" && cfg.TLSKey != "" {
			cert, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
			if err != nil {
				return nil, err
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
			tlsConfig.BuildNameToCertificate()
			log.Println("TLS cert and key loaded")
		}
	} else {
		log.Println("TLS cert and key not given")
	}
	return tlsConfig, nil
}
