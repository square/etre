// Copyright 2017-2021, Square, Inc.

package config_test

import (
	"testing"

	"github.com/go-test/deep"

	"github.com/square/etre/config"
)

func TestDatasourceConfigDefaults(t *testing.T) {
	d := config.DatasourceConfig{
		URL:            "a",
		Database:       "b",
		ConnectTimeout: "c",
		QueryTimeout:   "d",
		MaxConnections: 123,
		TLSCert:        "e",
		TLSKey:         "f",
		TLSCA:          "g",
		Username:       "h",
		Password:       "i",
		Source:         "j",
		Mechanism:      "k",
	}
	c := config.DatasourceConfig{}
	c = c.WithDefaults(d)
	if diff := deep.Equal(c, d); diff != nil {
		t.Error(diff)
	}

	c = config.DatasourceConfig{
		URL:            "c1",
		MaxConnections: 5,
	}
	m := config.DatasourceConfig{
		URL:            "c1", // from c
		Database:       "b",
		ConnectTimeout: "c",
		QueryTimeout:   "d",
		MaxConnections: 5, // from c
		TLSCert:        "e",
		TLSKey:         "f",
		TLSCA:          "g",
		Username:       "h",
		Password:       "i",
		Source:         "j",
		Mechanism:      "k",
	}
	c = c.WithDefaults(d)
	if diff := deep.Equal(c, m); diff != nil {
		t.Error(diff)
	}
}

func TestLoadEmpty(t *testing.T) {
	cfg := config.Default()
	got, err := config.Load("../test/config/empty.yaml", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if diff := deep.Equal(got, config.Default()); diff != nil {
		t.Error(diff)
	}
}

func TestLoadTest001(t *testing.T) {
	got, err := config.Load("../test/config/test001.yaml", config.Default())
	if err != nil {
		t.Fatal(err)
	}

	expect := config.Default()
	expect.Server.Addr = "10.0.0.1:1234"
	expect.Datasource.URL = "mongodb://10.0.0.2:4567"
	expect.Datasource.Database = "test_db"
	expect.Entity.Types = []string{"test"}
	expect.Metrics.QueryLatencySLA = "10ms"

	if diff := deep.Equal(got, expect); diff != nil {
		t.Error(diff)
	}
}

func TestLoadTest002(t *testing.T) {
	got, err := config.Load("../test/config/test002.yaml", config.Default())
	if err != nil {
		t.Fatal(err)
	}

	expect := config.Default()
	expect.CDC.Disabled = true // testing this

	if diff := deep.Equal(got, expect); diff != nil {
		t.Error(diff)
	}
}
