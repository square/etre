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

func Connect(cfg config.DatasourceConfig) (*mongo.Client, error) {
	tlsConfig, err := loadTLS(cfg)
	if err != nil {
		return nil, err
	}

	timeout, err := time.ParseDuration(cfg.ConnectTimeout)
	if err != nil {
		return nil, err
	}

	creds := options.Credential{
		AuthMechanism: cfg.Mechanism,
		AuthSource:    cfg.Source,
		Username:      cfg.Username,
		Password:      cfg.Password,
	}

	opts := options.Client().
		ApplyURI(cfg.URL).
		SetTLSConfig(tlsConfig).
		SetAuth(creds).
		SetMaxPoolSize(cfg.MaxConnections).
		SetConnectTimeout(timeout)

	client, err := mongo.NewClient(opts)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
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
