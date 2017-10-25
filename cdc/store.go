// Copyright 2017, Square, Inc.

// Package cdc provides interfaces for reading and writing change data capture
// (CDC) events.
package cdc

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/square/etre"
	"github.com/square/etre/db"
)

// Filter contains fields that are used to filter events that the CDC reads.
// Unset fields are ignored.
type Filter struct {
	SinceTs int64 // Only read events that have a timestamp greater than or equal to this value.
	UntilTs int64 // Only read events that have a timestamp less than this value.
	Limit   int
}

// NoFilter is a convenience var for calls like Read(cdc.NoFilter). Other
// packages must not modify this var.
var NoFilter Filter

// RetryPolicy represents the retry policy that the Store uses when reading and
// writing events.
type RetryPolicy struct {
	RetryCount int
	RetryWait  int // milliseconds
}

// NoRetryPolicy is a convenience var for calls like NewStore(cdc.NoRetryPolicy).
// Other packages must not modify this var.
var NoRetryPolicy RetryPolicy

// A Store reads and writes CDC events to/from a persistent data store.
type Store interface {
	// Write writes the CDC event to a persisitent data store. If writing
	// fails, it retries according to the RetryPolicy. If retrying fails,
	// the event is written to the fallbackFile. A ErrWriteEvent is
	// returned if writing to the persistent data store fails, even if
	// writing to fallbackFile succeeds.
	Write(etre.CDCEvent) error

	// Read queries a persistent data store for events that satisfy the
	// given filter.
	Read(Filter) ([]etre.CDCEvent, error)
}

// mongoStore implements the Store interface with MongoDB.
type store struct {
	conn         db.Connector // a Mongo connection pool
	database     string       // the db the CDC events are stored in
	collection   string       // the collection that CDC events are stored in
	wrp          RetryPolicy  // retry policy for writing CDC events to Mongo
	fallbackFile string       // path to file that CDC events are written to if we can't write to Mongo
}

func NewStore(conn db.Connector, database, collection, fallbackFile string, writeRetryPolicy RetryPolicy) Store {
	return &store{
		conn:         conn,
		database:     database,
		collection:   collection,
		wrp:          writeRetryPolicy,
		fallbackFile: fallbackFile,
	}
}

func (s *store) Read(filter Filter) ([]etre.CDCEvent, error) {
	ms, err := s.conn.Connect()
	if err != nil {
		return nil, err
	}
	defer ms.Close()

	coll := ms.DB(s.database).C(s.collection)
	q := bson.M{}

	// Apply timestamp filters.
	if filter.SinceTs != 0 || filter.UntilTs != 0 {
		tsf := bson.M{}
		if filter.SinceTs != 0 {
			tsf["$gte"] = filter.SinceTs
		}
		if filter.UntilTs != 0 {
			tsf["$lt"] = filter.UntilTs
		}
		q["ts"] = tsf
	}

	// Create the mgo query.
	mgoQuery := coll.Find(q)

	// Always sort by entityId, revision ascending. This is the only
	// meaningful way to sort CDC events.
	mgoQuery = mgoQuery.Sort("entityId", "rev")

	// Apply limit.
	if filter.Limit != 0 {
		mgoQuery = mgoQuery.Limit(filter.Limit)
	}

	var events []etre.CDCEvent
	err = mgoQuery.All(&events)
	if err != nil {
		return nil, err
	}

	return events, nil
}

func (s *store) Write(event etre.CDCEvent) error {
	// If there is any error writing the event in Mongo, retry according to
	// the write retry policy.
	var ms *mgo.Session
	var err error
	for i := 0; i <= s.wrp.RetryCount; i++ {
		if i > 0 {
			time.Sleep(time.Duration(s.wrp.RetryWait) * time.Millisecond)
		}

		if ms == nil {
			ms, err = s.conn.Connect()
			if err != nil {
				continue
			}
			defer ms.Close()
		}
		coll := ms.DB(s.database).C(s.collection)

		err = coll.Insert(event)
		if err != nil {
			continue
		} else {
			// Successfully wrote the event.
			return nil
		}
	}

	errResp := ErrWriteEvent{
		datastoreError: err.Error(),
	}

	// If we can't write the event to Mongo, and if a fallback file is
	// specified, try to write the event to that file. Even if we succeed
	// at writing to the file, return an error so that the caller knows
	// there was a problem.
	if err != nil && s.fallbackFile != "" {
		bytes, err := json.Marshal(event)
		if err != nil {
			errResp.fileError = err.Error()
			return errResp
		}

		// If the file doesn't exist, create it, or append to the file.
		f, err := os.OpenFile(s.fallbackFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			errResp.fileError = err.Error()
			return errResp
		}
		if _, err := f.Write(bytes); err != nil {
			errResp.fileError = err.Error()
			return errResp
		}
		if err := f.Close(); err != nil {
			errResp.fileError = err.Error()
			return errResp
		}
	}

	return errResp
}

// ErrWriteEvent represents an error in writing a CDC event to a persistent
// data store. It satisfies the Error interface.
type ErrWriteEvent struct {
	datastoreError string
	fileError      string
}

func (e ErrWriteEvent) Error() string {
	msg := fmt.Sprintf("error writing CDC event (datastore error: %s)", e.datastoreError)
	if e.fileError != "" {
		msg = fmt.Sprintf("%s (file error: %s)", msg, e.fileError)
	}
	return msg
}
