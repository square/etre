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

const (
	SORT_BY_DEFAULT   byte = iota
	SORT_BY_ENTID_REV      // sort by entityId, revision ascending
)

// Filter contains fields that can be used to filter, order, and sort events
// that the CDC returns. Unset fields are ignored.
type Filter struct {
	SinceTs int64 // Only include events that have a timestamp greater than or equal to this value.
	UntilTs int64 // Only include events that have a timestamp less than this value.
	Limit   int
	SortBy  byte
}

// NoFilter is a convenience var for calls like ListEvents(cdc.NoFilter). Other
// packages must not modify this var.
var NoFilter Filter

// RetryStrategy specifies the number of times a CDC write event should be
// retried when being written to mongo, as well as a wait time between retries.
type RetryStrategy struct {
	RetryCount int
	RetryWait  int // milliseconds
}

// NoRetryStrategy is a convenience var for calls like CreateEvent(cdc.NoRetryStrategy).
// Other packages must not modify this var.
var NoRetryStrategy RetryStrategy

// A Manager can be used for creating and listing CDC events. It is safe for
// use by concurrent threads.
type Manager interface {
	// CreateEvent writes a CDC event to mongo. If it fails to write the event,
	// it will attemp to retry doing so in accordance with the write retry
	// strategy that the Manager was created with. If it still fails after the
	// retries, it will attempt to write the event to the fallback file
	// specified when the Manager was created (if an empty string was provided,
	// it will skip this step). If the event can't be written to mongo an error
	// will be returned, even if the event is successfully written to the
	// fallback file.
	CreateEvent(etre.CDCEvent) error

	// ListEvents lists CDC events that satisfy the given filter.
	ListEvents(Filter) ([]etre.CDCEvent, error)
}

// manager satisfies the Manager interface.
type manager struct {
	conn         db.Connector  // a mongo connection
	database     string        // the db the CDC events are stored in
	collection   string        // the collection that CDC events are stored in
	wrs          RetryStrategy // retry strategy for failed CDC event writes to mongo
	fallbackFile string        // path to file that CDC events should be written to if we can't write to mongo
}

func NewManager(conn db.Connector, database, collection, fallbackFile string, writeRetryStrategy RetryStrategy) Manager {
	return &manager{
		conn:         conn,
		database:     database,
		collection:   collection,
		wrs:          writeRetryStrategy,
		fallbackFile: fallbackFile,
	}
}

func (c *manager) ListEvents(filter Filter) ([]etre.CDCEvent, error) {
	s, err := c.conn.Connect()
	if err != nil {
		return nil, err
	}
	defer s.Close()

	coll := s.DB(c.database).C(c.collection)
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

	// Apply sort.
	switch filter.SortBy {
	case SORT_BY_ENTID_REV:
		mgoQuery = mgoQuery.Sort("entityId", "rev")
	}

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

func (c *manager) CreateEvent(event etre.CDCEvent) error {
	// If there is any error creating the event in mongo, retry in accordance
	// with the Manager's write retry strategy.
	var s *mgo.Session
	var err error
	for i := 0; i <= c.wrs.RetryCount; i++ {
		if i > 0 {
			time.Sleep(time.Duration(c.wrs.RetryWait) * time.Millisecond)
		}

		if s == nil {
			s, err = c.conn.Connect()
			if err != nil {
				continue
			}
			defer s.Close()
		}
		coll := s.DB(c.database).C(c.collection)

		err = coll.Insert(event)
		if err != nil {
			continue
		} else {
			// Successfully wrote the event.
			return nil
		}
	}

	errResp := ErrWriteEvent{
		mongoError: err.Error(),
	}

	// If we can't write the event to mongo, and if a fallback file is
	// specified, try to write the event to that file. Even if we succeed
	// at writing to the file, return an error so that the caller knows
	// there was a problem.
	if err != nil && c.fallbackFile != "" {
		bytes, err := json.Marshal(event)
		if err != nil {
			errResp.fileError = err.Error()
			return errResp
		}

		// If the file doesn't exist, create it, or append to the file.
		f, err := os.OpenFile(c.fallbackFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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

// ErrWriteEvent represents an error in writing a CDC event to mongo. It satisfies
// the Error interface.
type ErrWriteEvent struct {
	mongoError string
	fileError  string
}

func (e ErrWriteEvent) Error() string {
	msg := fmt.Sprintf("error writing CDC event (mongo error: %s)", e.mongoError)
	if e.fileError != "" {
		msg = fmt.Sprintf("%s (file error: %s)", msg, e.fileError)
	}
	return msg
}
