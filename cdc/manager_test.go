// Copyright 2017, Square, Inc.

package cdc_test

import (
	"testing"

	"github.com/square/etre"
	"github.com/square/etre/cdc"
	"github.com/square/etre/db"
	"github.com/square/etre/test/mock"

	"github.com/go-test/deep"
	"gopkg.in/mgo.v2"
)

// @todo: make the host/port configurable
var url = "localhost:3000"
var database = "etre_test"
var collection = "cdc"
var timeout = 5

func setup(t *testing.T) db.Connector {
	conn := db.NewConnector(url, timeout, nil, nil)
	s, err := conn.Connect()
	if err != nil {
		t.Error("error connecting to mongo: %s", err)
	}

	for _, event := range mock.CDCEvents {
		err = s.DB(database).C(collection).Insert(event)
		if err != nil {
			t.Error("error writing test CDC data to mongo: %s", err)
		}
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

func TestListEvents(t *testing.T) {
	conn := setup(t)
	defer teardown(t, conn)
	cdcm := cdc.NewManager(conn, database, collection, "", cdc.NoRetryStrategy)

	// Filter #1.
	filter := cdc.Filter{
		SinceTs: 13,
		UntilTs: 35,
	}
	events, err := cdcm.ListEvents(filter)
	if err != nil {
		t.Error(err)
	}

	actualIds := []string{}
	for _, event := range events {
		actualIds = append(actualIds, event.EventId)
	}

	expectedIds := []string{"vno", "4pi", "p34"} // order matters
	if diff := deep.Equal(actualIds, expectedIds); diff != nil {
		t.Error(diff)
	}

	// Filter #2.
	filter = cdc.Filter{
		SinceTs: 10,
		UntilTs: 43,
		SortBy:  cdc.SORT_BY_ENTID_REV,
	}
	events, err = cdcm.ListEvents(filter)
	if err != nil {
		t.Error(err)
	}

	actualIds = []string{}
	for _, event := range events {
		actualIds = append(actualIds, event.EventId)
	}

	expectedIds = []string{"nru", "p34", "61p", "qwp", "vno", "4pi", "vb0", "bnu"} // order matters
	if diff := deep.Equal(actualIds, expectedIds); diff != nil {
		t.Error(diff)
	}
}

func TestCreateEventSuccess(t *testing.T) {
	conn := setup(t)
	defer teardown(t, conn)
	cdcm := cdc.NewManager(conn, database, collection, "", cdc.NoRetryStrategy)

	event := etre.CDCEvent{EventId: "abc", EntityId: "e13", Rev: 7, Ts: 54}
	err := cdcm.CreateEvent(event)
	if err != nil {
		t.Error(err)
	}

	// Get the event we just created.
	filter := cdc.Filter{
		SinceTs: 54,
		UntilTs: 55,
	}
	actualEvents, err := cdcm.ListEvents(filter)
	if err != nil {
		t.Error(err)
	}

	if len(actualEvents) != 1 {
		t.Errorf("got back %d events, expected 1", len(actualEvents))
	}

	if diff := deep.Equal(event, actualEvents[0]); diff != nil {
		t.Error(diff)
	}
}

func TestCreateEventFailure(t *testing.T) {
	var tries int
	// Create a mock dbconn that will always throw an error.
	conn := &mock.Connector{
		ConnectFunc: func() (*mgo.Session, error) {
			tries = tries + 1
			return nil, mock.ErrConnector
		},
	}

	wrs := cdc.RetryStrategy{
		RetryCount: 3,
		RetryWait:  0,
	}

	cdcm := cdc.NewManager(conn, database, collection, "", wrs)

	event := etre.CDCEvent{EventId: "abc", EntityId: "e13", Rev: 7, Ts: 54}
	err := cdcm.CreateEvent(event)
	if err == nil {
		t.Error("expected an error but did not get one")
	}

	switch err.(type) {
	case cdc.ErrWriteEvent:
		// this is what we expect
	default:
		t.Errorf("err = %s, expected error of type cdc.ErrWriteEvent", err)
	}

	if tries != 4 {
		t.Errorf("create tries = %d, expected 4", tries)
	}
}

func TestCreateEventFallbackFile(t *testing.T) {
	// @todo: test writing to the fallback file as well
}
