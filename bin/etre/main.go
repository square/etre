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

	"github.com/square/etre/api"
	"github.com/square/etre/db"
	"github.com/square/etre/router"

	"gopkg.in/yaml.v2"
)

type DBConfig struct {
	URL         string   `yaml:"url"`
	Database    string   `yaml:"database"`
	EntityTypes []string `yaml:"entity_types"`
	Timeout     int      `yaml:"timeout"`

	// Certs
	TLSCert string `yaml:"tls-cert"`
	TLSKey  string `yaml:"tls-key"`
	TLSCA   string `yaml:"tls-ca"`

	// Credentials
	Username  string `yaml:"username"`
	Source    string `yaml:"source"`
	Mechanism string `yaml:"mechanism"`
}

type ServerConfig struct {
	Addr string `yaml:"addr"`

	// Certs
	TLSCert string `yaml:"tls-cert"`
	TLSKey  string `yaml:"tls-key"`
	TLSCA   string `yaml:"tls-ca"`
}

type Config struct {
	Server ServerConfig
	DB     DBConfig
}

var flagConfig string
var default_addr = "127.0.0.1:8080"
var default_database_url = "localhost"
var default_database = "etre"
var default_entity_types = []string{"entities"}
var default_database_timeout_seconds = 5

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
		DB: DBConfig{
			URL:         default_database_url,
			Database:    default_database,
			EntityTypes: default_entity_types,
			Timeout:     default_database_timeout_seconds,
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
	if config.DB.TLSCert != "" && config.DB.TLSKey != "" {
		cert, err := tls.LoadX509KeyPair(config.DB.TLSCert, config.DB.TLSKey)
		if err != nil {
			log.Fatal(err)
		}

		caCert, err := ioutil.ReadFile(config.DB.TLSCA)
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
	if config.DB.Username != "" && config.DB.Source != "" && config.DB.Mechanism != "" {
		dbCredentials["username"] = config.DB.Username
		dbCredentials["source"] = config.DB.Source
		dbCredentials["mechanism"] = config.DB.Mechanism
	}

	// //////////////////////////////////////////////////////////////////////
	// Launch App (connect to DB, initialize router/API, start server)
	// //////////////////////////////////////////////////////////////////////
	var c db.Connector
	c, err = db.NewConnector(
		config.DB.URL,
		config.DB.Database,
		config.DB.EntityTypes,
		config.DB.Timeout,
		tlsConfig,
		dbCredentials,
	)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	err = c.Connect()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	api := api.NewAPI(&router.Router{}, c)

	// Start the web server.
	if config.Server.TLSCert != "" && config.Server.TLSKey != "" {
		log.Println("Listening on ", config.Server.Addr, " with TLS enabled")
		err = http.ListenAndServeTLS(config.Server.Addr, config.Server.TLSCert, config.Server.TLSKey, api.Router)
	} else {
		log.Println("Listening on ", config.Server.Addr, " with TLS disabled")
		err = http.ListenAndServe(config.Server.Addr, api.Router)
	}
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
}
