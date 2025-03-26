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

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

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
	// the event is written to the fallbackFile. An error is returned if
	// writing to the persistent data store fails, even if writing to
	// fallback file succeeds.
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	count, err := s.coll.CountDocuments(ctx, q, options.Count())
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
	var werr error
	tries := 1 + s.wrp.RetryCount
	for tryNo := 1; tryNo <= tries; tryNo++ {
		if _, werr = s.coll.InsertOne(ctx, event); werr != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				break // don't retry when context is done
			}
			if tryNo == tries {
				break // don't wait on last try
			}
			time.Sleep(time.Duration(s.wrp.RetryWait) * time.Millisecond)
			continue // try again
		}
		return nil // success
	}

	// If context timed out, use the context error instead of the mongo driver
	// error so the API knows it's a db timeout
	if ctxErr := ctx.Err(); ctxErr != nil {
		werr = ctxErr
	}

	// If we can't write the event to Mongo, and if a fallback file is
	// specified, try to write the event to that file. Even if we succeed
	// at writing to the file, return an error so that the caller knows
	// there was a problem.
	if s.fallbackFile == "" {
		return werr
	}

	bytes, ferr := json.Marshal(event)
	if ferr != nil {
		return fmt.Errorf("cannot marshal CDCEvent as JSON: %s", ferr)
	}

	// If the file doesn't exist, create it, or append to the file.
	f, ferr := os.OpenFile(s.fallbackFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if ferr != nil {
		return fmt.Errorf("cannot open CDC fallback file: %s", ferr)
	}
	if _, ferr := f.Write(bytes); ferr != nil {
		return fmt.Errorf("cannot write to CDC fallback file: %s", ferr)
	}
	if ferr := f.Close(); ferr != nil {
		return fmt.Errorf("cannot close CDC fallback file: %s", ferr)
	}

	return werr
}
