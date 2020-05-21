// Copyright 2017-2020, Square, Inc.

// Package cdc provides interfaces for reading and writing change data capture (CDC) events.
package cdc

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/square/etre"
)

// Filter contains fields that are used to filter events that the CDC reads.
// Unset fields are ignored.
type Filter struct {
	SinceTs int64 // Only read events that have a timestamp greater than or equal to this value.
	UntilTs int64 // Only read events that have a timestamp less than this value.
	Limit   int64
	Order   sort.Interface
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

// ByEntityIdRevAsc sorts a slice of etre.CDCEvent by EntityId, Ts ascending.
// This is the correct way to sort CDC events. Timestamp are not reliable.
// This is used in Store.Find() instead of having MongoDB sort the results which
// can fail with too many CDC events because MongoDB has a max sort size.
// Also, it's better to offload sorting to Etre which can scale out more easily.
type ByEntityIdRevAsc []etre.CDCEvent

func (a ByEntityIdRevAsc) Len() int      { return len(a) }
func (a ByEntityIdRevAsc) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByEntityIdRevAsc) Less(i, j int) bool {
	if a[i].EntityId == a[j].EntityId {
		return a[i].EntityRev < a[j].EntityRev
	}
	return a[i].EntityId < a[j].EntityId
}

type ByTsAsc []etre.CDCEvent

func (a ByTsAsc) Len() int           { return len(a) }
func (a ByTsAsc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByTsAsc) Less(i, j int) bool { return a[i].Ts < a[j].Ts }

// A Store reads and writes CDC events to/from a persistent data store.
type Store interface {
	// Write writes the CDC event to a persisitent data store. If writing
	// fails, it retries according to the RetryPolicy. If retrying fails,
	// the event is written to the fallbackFile. A ErrWriteEvent is
	// returned if writing to the persistent data store fails, even if
	// writing to fallbackFile succeeds.
	Write(context.Context, etre.CDCEvent) error

	// Read queries a persistent data store for events that satisfy the
	// given filter.
	Read(Filter) ([]etre.CDCEvent, error)
}

// mongoStore implements the Store interface with MongoDB.
type store struct {
	coll         *mongo.Collection
	wrp          RetryPolicy // retry policy for writing CDC events to Mongo
	fallbackFile string      // path to file that CDC events are written to if we can't write to Mongo
}

func NewStore(coll *mongo.Collection, fallbackFile string, writeRetryPolicy RetryPolicy) Store {
	return &store{
		coll:         coll,
		fallbackFile: fallbackFile,
		wrp:          writeRetryPolicy,
	}
}

func (s *store) Read(f Filter) ([]etre.CDCEvent, error) {
	if f.SinceTs == 0 {
		f.SinceTs = time.Now().Add(-1 * time.Hour).UnixNano()
	}
	ts := bson.M{"$gte": f.SinceTs}
	if f.UntilTs > 0 {
		ts["$lt"] = f.UntilTs
	}
	q := bson.M{"ts": ts}

	// Count number of docs we're about to fetch so we can make a slice of
	// etre.CDC to match so, below, cursor.All() doesn't have to realloc the
	// slice. For small fetches, this is overkill, but it makes large fetchs
	// (>100k events) very quick and efficient.
	opts := options.Count().SetMaxTime(5 * time.Second)
	count, err := s.coll.CountDocuments(context.TODO(), q, opts)
	if err != nil {
		return nil, err
	}

	// DO NOT use options SetBatchSize, SetLimit, or SetSort. Testing with a
	// collection of ~200k CDC events shows that Mongo will use the biggest
	// and fewest batches possible. We don't want a limit, we want all results.
	// And we offload sorting from Mongo to Etre which can scale out more easily.
	cursor, err := s.coll.Find(context.TODO(), q)
	if err != nil {
		return nil, err
	}
	events := make([]etre.CDCEvent, count)
	if err := cursor.All(context.TODO(), &events); err != nil {
		return nil, err
	}

	// Sort events here rather than having Mongo do it because 1) if we fetch
	// too many events, it might exceed Mongo's sort limit and throw an error;
	// 2) it's better to offload sorting to the app which can scale out more
	// easily than the database.
	if f.Order != nil {
		switch f.Order.(type) {
		case ByEntityIdRevAsc:
			sort.Sort(ByEntityIdRevAsc(events))
		case ByTsAsc:
			sort.Sort(ByTsAsc(events))
		default:
			panic(fmt.Sprintf("invalid cdc.Filter.Order value type: %T, expected cdc.ByEntityIdRevAsc or cdc.ByTsAsc", f.Order))
		}
	}

	return events, nil
}

func (s *store) Write(ctx context.Context, event etre.CDCEvent) error {
	var err error
	tries := 1 + s.wrp.RetryCount
	for tryNo := 1; tryNo <= tries; tryNo++ {
		if _, err = s.coll.InsertOne(ctx, event); err != nil {
			if tryNo == tries {
				break // don't wait on last try
			}
			time.Sleep(time.Duration(s.wrp.RetryWait) * time.Millisecond)
			continue // try again
		}
		return nil // success
	}

	errResp := ErrWriteEvent{
		datastoreError: err.Error(),
	}

	// If we can't write the event to Mongo, and if a fallback file is
	// specified, try to write the event to that file. Even if we succeed
	// at writing to the file, return an error so that the caller knows
	// there was a problem.
	if s.fallbackFile == "" {
		return errResp
	}

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

type NoopStore struct{}

func (s NoopStore) Write(e etre.CDCEvent) error {
	return nil
}

func (s NoopStore) Read(f Filter) ([]etre.CDCEvent, error) {
	return nil, nil
}
