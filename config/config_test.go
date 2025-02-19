// Copyright 2017-2021, Square, Inc.

package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	assert.Equal(t, d, c)

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
	assert.Equal(t, m, c)
}

func TestLoadEmpty(t *testing.T) {
	cfg := config.Default()
	got, err := config.Load("../test/config/empty.yaml", cfg)
	require.NoError(t, err)
	assert.Equal(t, cfg, got)
}

func TestLoadTest001(t *testing.T) {
	got, err := config.Load("../test/config/test001.yaml", config.Default())
	require.NoError(t, err)

	expect := config.Default()
	expect.Server.Addr = "10.0.0.1:1234"
	expect.Datasource.URL = "mongodb://10.0.0.2:4567"
	expect.Datasource.Database = "test_db"
	expect.Entity.Types = []string{"test"}
	expect.Metrics.QueryLatencySLA = "10ms"
	assert.Equal(t, expect, got)
}

func TestLoadTest002(t *testing.T) {
	got, err := config.Load("../test/config/test002.yaml", config.Default())
	require.NoError(t, err)

	expect := config.Default()
	expect.CDC.Disabled = true // testing this
	assert.Equal(t, expect, got)
}
