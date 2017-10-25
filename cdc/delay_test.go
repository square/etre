// Copyright 2017, Square, Inc.

package cdc_test

import (
	"testing"

	"github.com/square/etre/cdc"
	"github.com/square/etre/db"
)

// @todo: make this configurable
var delayDatabase = "etre_test"
var delayCollection = "cdc"

func delaySetup(t *testing.T) db.Connector {
	// @todo: make this configurable
	conn := db.NewConnector("localhost:3000", 1, nil, nil)

	_, err := conn.Connect()
	if err != nil {
		t.Error("error connecting to mongo: %s", err)
	}

	return conn
}

func delayTeardown(t *testing.T, conn db.Connector) {
	s, err := conn.Connect()
	if err != nil {
		t.Error("error connecting to mongo: %s", err)
	}

	err = s.DB(delayDatabase).C(delayCollection).DropCollection()
	if err != nil {
		t.Errorf("error deleting CDC events: %s", err)
	}

	// Close db connection.
	conn.Close()
}

func TestDynamicDelayer(t *testing.T) {
	conn := delaySetup(t)
	defer delayTeardown(t, conn)

	d, err := cdc.NewDynamicDelayer(conn, delayDatabase, delayCollection)
	if err != nil {
		t.Error(err)
	}

	// Start change #1.
	changeId1 := "abc"
	cdc.CurrentTimestamp = func() int64 { return 1 }
	err = d.BeginChange(changeId1)
	if err != nil {
		t.Error(err)
	}

	// Verify the max timestamp.
	maxTimestamp, err := d.MaxTimestamp()
	if err != nil {
		t.Error(err)
	}
	if maxTimestamp != 1 {
		t.Errorf("maxTimestamp = %d, expected 1", maxTimestamp)
	}

	// Start change #2.
	changeId2 := "def"
	cdc.CurrentTimestamp = func() int64 { return 3 }
	err = d.BeginChange(changeId2)
	if err != nil {
		t.Error(err)
	}

	// Verify the max timestamp hasn't changed.
	maxTimestamp, err = d.MaxTimestamp()
	if err != nil {
		t.Error(err)
	}
	if maxTimestamp != 1 {
		t.Errorf("maxTimestamp = %d, expected 1", maxTimestamp)
	}

	// Start change #3.
	changeId3 := "ghi"
	cdc.CurrentTimestamp = func() int64 { return 9 }
	err = d.BeginChange(changeId3)
	if err != nil {
		t.Error(err)
	}

	// Verify the max timestamp hasn't changed.
	maxTimestamp, err = d.MaxTimestamp()
	if err != nil {
		t.Error(err)
	}
	if maxTimestamp != 1 {
		t.Errorf("maxTimestamp = %d, expected 1", maxTimestamp)
	}

	// Stop change #1.
	err = d.EndChange(changeId1)
	if err != nil {
		t.Error(err)
	}

	// Verify the max timestamp is now change #2s start time.
	maxTimestamp, err = d.MaxTimestamp()
	if err != nil {
		t.Error(err)
	}
	if maxTimestamp != 3 {
		t.Errorf("maxTimestamp = %d, expected 3", maxTimestamp)
	}

	// Stop change #3.
	err = d.EndChange(changeId3)
	if err != nil {
		t.Error(err)
	}

	// Verify the max timestamp hasn't changed.
	maxTimestamp, err = d.MaxTimestamp()
	if err != nil {
		t.Error(err)
	}
	if maxTimestamp != 3 {
		t.Errorf("maxTimestamp = %d, expected 3", maxTimestamp)
	}

	// Stop change #2.
	err = d.EndChange(changeId2)
	if err != nil {
		t.Error(err)
	}

	// Verify the max timestamp is now the current time.
	cdc.CurrentTimestamp = func() int64 { return 50 }
	maxTimestamp, err = d.MaxTimestamp()
	if err != nil {
		t.Error(err)
	}
	if maxTimestamp != 50 {
		t.Errorf("maxTimestamp = %d, expected 50", maxTimestamp)
	}
}

func TestStaticDelayer(t *testing.T) {
	delay := 5
	d, err := cdc.NewStaticDelayer(delay)
	if err != nil {
		t.Error(err)
	}

	cdc.CurrentTimestamp = func() int64 { return 12 }
	maxTimestamp, err := d.MaxTimestamp()
	if err != nil {
		t.Error(err)
	}
	if maxTimestamp != 7 {
		t.Errorf("maxTimestamp = %d, expected 7", maxTimestamp)
	}
}
