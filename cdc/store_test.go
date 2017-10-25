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

// @todo: make this configurable
var cdcDatabase = "etre_test"
var cdcCollection = "cdc"

func cdcSetup(t *testing.T) db.Connector {
	// @todo: make this configurable
	conn := db.NewConnector("localhost:3000", 5, nil, nil)
	s, err := conn.Connect()
	if err != nil {
		t.Error("error connecting to mongo: %s", err)
	}

	for _, event := range mock.CDCEvents {
		err = s.DB(cdcDatabase).C(cdcCollection).Insert(event)
		if err != nil {
			t.Error("error writing test CDC data to mongo: %s", err)
		}
	}

	return conn
}

func cdcTeardown(t *testing.T, conn db.Connector) {
	s, err := conn.Connect()
	if err != nil {
		t.Error("error connecting to mongo: %s", err)
	}

	err = s.DB(cdcDatabase).C(cdcCollection).DropCollection()
	if err != nil {
		t.Errorf("error deleting CDC events: %s", err)
	}

	// Close db connection.
	conn.Close()
}

func TestRead(t *testing.T) {
	conn := cdcSetup(t)
	defer cdcTeardown(t, conn)
	cdcs := cdc.NewStore(conn, cdcDatabase, cdcCollection, "", cdc.NoRetryPolicy)

	// Filter #1.
	filter := cdc.Filter{
		SinceTs: 13,
		UntilTs: 35,
	}
	events, err := cdcs.Read(filter)
	if err != nil {
		t.Error(err)
	}

	actualIds := []string{}
	for _, event := range events {
		actualIds = append(actualIds, event.EventId)
	}

	expectedIds := []string{"p34", "vno", "4pi"} // order matters
	if diff := deep.Equal(actualIds, expectedIds); diff != nil {
		t.Error(diff)
	}

	// Filter #2.
	filter = cdc.Filter{
		SinceTs: 10,
		UntilTs: 43,
	}
	events, err = cdcs.Read(filter)
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

func TestWriteSuccess(t *testing.T) {
	conn := cdcSetup(t)
	defer cdcTeardown(t, conn)
	cdcs := cdc.NewStore(conn, cdcDatabase, cdcCollection, "", cdc.NoRetryPolicy)

	event := etre.CDCEvent{EventId: "abc", EntityId: "e13", Rev: 7, Ts: 54}
	err := cdcs.Write(event)
	if err != nil {
		t.Error(err)
	}

	// Get the event we just created.
	filter := cdc.Filter{
		SinceTs: 54,
		UntilTs: 55,
	}
	actualEvents, err := cdcs.Read(filter)
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

func TestWriteFailure(t *testing.T) {
	var tries int
	// Create a mock dbconn that will always throw an error.
	conn := &mock.Connector{
		ConnectFunc: func() (*mgo.Session, error) {
			tries = tries + 1
			return nil, mock.ErrConnector
		},
	}

	wrp := cdc.RetryPolicy{
		RetryCount: 3,
		RetryWait:  0,
	}

	cdcs := cdc.NewStore(conn, cdcDatabase, cdcCollection, "", wrp)

	event := etre.CDCEvent{EventId: "abc", EntityId: "e13", Rev: 7, Ts: 54}
	err := cdcs.Write(event)
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

func TestWriteFallbackFile(t *testing.T) {
	// @todo: test writing to the fallback file as well
}
