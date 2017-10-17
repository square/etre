// Copyright 2017, Square, Inc.

package feed_test

import (
	"testing"

	"github.com/square/etre/db"
	"github.com/square/etre/feed"
)

// @todo: make the host/port configurable
var mongoUrl = "localhost:3000"
var database = "etre_test"
var collection = "delay"
var timeout = 5

func setup(t *testing.T) db.Connector {
	conn := db.NewConnector(mongoUrl, timeout, nil, nil)

	_, err := conn.Connect()
	if err != nil {
		t.Error("error connecting to mongo: %s", err)
	}

	return conn
}

func teardown(t *testing.T, conn db.Connector) {
	s, err := conn.Connect()
	if err != nil {
		t.Error("error connecting to mongo: %s", err)
	}

	err = s.DB(database).C(collection).DropCollection()
	if err != nil {
		t.Errorf("error deleting CDC events: %s", err)
	}

	// Close db connection.
	conn.Close()
}

func TestDynamicDelayManager(t *testing.T) {
	conn := setup(t)
	defer teardown(t, conn)

	dm, err := feed.NewDynamicDelayManager(conn, database, collection)
	if err != nil {
		t.Error(err)
	}

	// Start change #1.
	changeId1 := "abc"
	feed.CurrentTimestamp = func() int64 { return 1 }
	err = dm.BeginChange(changeId1)
	if err != nil {
		t.Error(err)
	}

	// Verify the max timestamp.
	maxTimestamp, err := dm.MaxTimestamp()
	if err != nil {
		t.Error(err)
	}
	if maxTimestamp != 1 {
		t.Errorf("maxTimestamp = %d, expected 1", maxTimestamp)
	}

	// Start change #2.
	changeId2 := "def"
	feed.CurrentTimestamp = func() int64 { return 3 }
	err = dm.BeginChange(changeId2)
	if err != nil {
		t.Error(err)
	}

	// Verify the max timestamp hasn't changed.
	maxTimestamp, err = dm.MaxTimestamp()
	if err != nil {
		t.Error(err)
	}
	if maxTimestamp != 1 {
		t.Errorf("maxTimestamp = %d, expected 1", maxTimestamp)
	}

	// Start change #3.
	changeId3 := "ghi"
	feed.CurrentTimestamp = func() int64 { return 9 }
	err = dm.BeginChange(changeId3)
	if err != nil {
		t.Error(err)
	}

	// Verify the max timestamp hasn't changed.
	maxTimestamp, err = dm.MaxTimestamp()
	if err != nil {
		t.Error(err)
	}
	if maxTimestamp != 1 {
		t.Errorf("maxTimestamp = %d, expected 1", maxTimestamp)
	}

	// Stop change #1.
	err = dm.EndChange(changeId1)
	if err != nil {
		t.Error(err)
	}

	// Verify the max timestamp is now change #2s start time.
	maxTimestamp, err = dm.MaxTimestamp()
	if err != nil {
		t.Error(err)
	}
	if maxTimestamp != 3 {
		t.Errorf("maxTimestamp = %d, expected 3", maxTimestamp)
	}

	// Stop change #3.
	err = dm.EndChange(changeId3)
	if err != nil {
		t.Error(err)
	}

	// Verify the max timestamp hasn't changed.
	maxTimestamp, err = dm.MaxTimestamp()
	if err != nil {
		t.Error(err)
	}
	if maxTimestamp != 3 {
		t.Errorf("maxTimestamp = %d, expected 3", maxTimestamp)
	}

	// Stop change #2.
	err = dm.EndChange(changeId2)
	if err != nil {
		t.Error(err)
	}

	// Verify the max timestamp is now the current time.
	feed.CurrentTimestamp = func() int64 { return 50 }
	maxTimestamp, err = dm.MaxTimestamp()
	if err != nil {
		t.Error(err)
	}
	if maxTimestamp != 50 {
		t.Errorf("maxTimestamp = %d, expected 50", maxTimestamp)
	}
}

func TestStaticDelayManager(t *testing.T) {
	delay := 5
	dm, err := feed.NewStaticDelayManager(delay)
	if err != nil {
		t.Error(err)
	}

	feed.CurrentTimestamp = func() int64 { return 12 }
	maxTimestamp, err := dm.MaxTimestamp()
	if err != nil {
		t.Error(err)
	}
	if maxTimestamp != 7 {
		t.Errorf("maxTimestamp = %d, expected 7", maxTimestamp)
	}
}
