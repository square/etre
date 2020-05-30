// Copyright 2017-2020, Square, Inc.

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
