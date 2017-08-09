// Copyright 2017, Square, Inc.

package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"io/ioutil"
	"log"
	"os"

	"github.com/square/etre/db"

	"gopkg.in/yaml.v2"
)

type Config struct {
	URL        string `yaml:"url"`
	Database   string `yaml:"database"`
	Collection string `yaml:"collection"`
	Timeout    int    `yaml:"timeout"`
	TLSCert    string `yaml:"tls-cert"`
	TLSKey     string `yaml:"tls-key"`
	TLSCA      string `yaml:"tls-ca"`
}

const (
	DEFAULT_DATABASE        = "etre"
	DEFAULT_COLLECTION      = "entities"
	DEFAULT_TIMEOUT_SECONDS = 5
)

var flagConfig string

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
		Database:   DEFAULT_DATABASE,
		Collection: DEFAULT_COLLECTION,
		Timeout:    DEFAULT_TIMEOUT_SECONDS,
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
	if config.TLSCert != "" && config.TLSKey != "" {
		cert, err := tls.LoadX509KeyPair(config.TLSCert, config.TLSKey)
		if err != nil {
			log.Fatal(err)
		}

		caCert, err := ioutil.ReadFile(config.TLSCA)
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
	// Launch App (connect to DB, initialize router/API, start server)
	// //////////////////////////////////////////////////////////////////////

	c := db.NewConnector(config.URL, config.Database, config.Collection, config.Timeout, tlsConfig)
	err = c.Connect()
	if err != nil {
		log.Fatal(err)
	}

	// TODO: initialize router and  API once that code is ready
	// TODO: add server
}
