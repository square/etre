// Copyright 2017, Square, Inc.

package cdc

import (
	"os"
	"sync"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/square/etre/db"
)

// A Delayer keeps track of the maximum upper-bound timestamp that is safe
// to use when querying for CDC events. As long as the maximum upper-bound
// timestamp used, queries are guaranteed to return consistent data. This
// guarantee doesn't apply if the Delayer timestamp is not used.
//
// The Delyer is a singleton. Its need arises from the fact that there is
// no guarantee that CDC events are written in the order that they happen
// in (entities and CDC events are not written in a single transaction). This
// can occur if event1 happens before event2, but event2 is written to the
// datastore before event1. Until event1 is written, the Delayer returns a
// timestamp that is LESS than the starting time for both event1 and event2.
// When both events have been written, the Delayer returns a timestamp
// GREATER than or equal to the completion timestamp of both events. This
// ensures that any queries to the Store that use this timestamp as the
// maximum upper-bound timestamp never get event2 without getting event1 first.
type Delayer interface {
	// Returns the maximum upper-bound timetstamp that is safe to use when
	// querying for CDC events.
	MaxTimestamp() (int64, error)

	// BeginChange marks an entity change as having started. The only
	// argument it takes is a string (id) that must uniquely identify
	// the change.
	BeginChange(changeId string) error

	// EndChange takes the id of an entity change and marks is as having
	// ended.
	EndChange(changeId string) error
}

// Delay represents the maximum upper-bound timestamp for a given Etre instance.
type Delay struct {
	Hostname string `json:"hostname"`
	Ts       int64  `json:"ts"`
}

// activeChange represents the id and starting timestamp of a change that is
// active within Etre.
type activeChange struct {
	id   string
	ts   int64 // timestamp of when the change started
	next *activeChange
	prev *activeChange
}

// activeChanges is a doubly linked list of active changes.
type activeChanges struct {
	head *activeChange
	tail *activeChange
	all  map[string]*activeChange // id => activeChange
}

// dynamicDelayer implements the Delayer interface. It dynamically updates
// the maximum upper-bound timestamp.
type dynamicDelayer struct {
	conn       db.Connector
	database   string
	collection string
	// --
	hostname      string
	activeChanges activeChanges
	*sync.Mutex
}

// staticDelayer implements the Delayer interface. It uses a static
// delay for the maximum uppoer-bound timestamp.
type staticDelayer struct {
	delay int // millisecond delay behind time.Now()
}

// NewDynamicDelayer returns a Delayer that dynamically updates the maximum
// upper-bound timestamp. It keeps track of all active entity changes (inserts,
// updates, deletes) on this Etre instance and continually writes the start
// time of the oldest active change to a persistent data store. Each Etre
// instance maintains its own "oldest active change" record in the data store.
// The max upper-bound timestamp that is safe to use is calculated by querying
// the smallest "oldest active change" value from the data store.
func NewDynamicDelayer(conn db.Connector, database, collection string) (Delayer, error) {
	h, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	return &dynamicDelayer{
		conn:       conn,
		database:   database,
		collection: collection,
		hostname:   h,
		activeChanges: activeChanges{
			all: map[string]*activeChange{},
		},
		Mutex: &sync.Mutex{},
	}, nil
}

func (dd *dynamicDelayer) MaxTimestamp() (int64, error) {
	s, err := dd.conn.Connect()
	if err != nil {
		return 0, err
	}
	defer s.Close()

	coll := s.DB(dd.database).C(dd.collection)
	var delays []Delay
	err = coll.Find(bson.M{}).Sort("Ts").Limit(1).All(&delays)
	if err != nil {
		return 0, err
	}

	// If there are no delay documents in mongo, we can assume that it's
	// safe to query documents up until time.Now() (i.e., no delay required).
	if len(delays) != 1 {
		return CurrentTimestamp(), nil
	}

	return delays[0].Ts, nil
}

func (dd *dynamicDelayer) BeginChange(changeId string) error {
	r := activeChange{
		id: changeId,
		ts: CurrentTimestamp(),
	}

	dd.Lock()
	defer dd.Unlock()

	// Add the change to activeChanges.
	dd.activeChanges.push(&r)

	// If this isn't the oldest active change, return.
	if !dd.activeChanges.isHead(r.id) {
		return nil
	}

	// If this is the oldest active change, set the timestamp for
	// this host in the collection.
	s, err := dd.conn.Connect()
	if err != nil {
		return err
	}
	defer s.Close()

	// Upsert the timestamp for this host in the collection. In the off
	// chance that a document for this host somehow already exists (can
	// happen if a host dies and comes back), we'd rather just update
	// the value for it instead of throwing a duplicate key error (which
	// we'd get in this case if we just did an insert).
	delay := Delay{
		Hostname: dd.hostname,
		Ts:       r.ts, // the starting timestamp of the change
	}
	change := mgo.Change{
		Update: bson.M{"$set": delay},
		Upsert: true,
	}

	coll := s.DB(dd.database).C(dd.collection)
	var result Delay
	_, err = coll.Find(bson.M{"hostname": dd.hostname}).Apply(change, &result)
	if err != nil {
		return err
	}

	return nil
}

func (dd *dynamicDelayer) EndChange(changeId string) error {
	dd.Lock()
	defer dd.Unlock()

	// Is this the oldest active change?
	isOldest := dd.activeChanges.isHead(changeId)

	// Remove from activeChanges.
	dd.activeChanges.remove(changeId)

	// If this is the oldest active change, we need to update the
	// delay document for this host in mongo to hold the timestamp of
	// the next-oldest change (or, in the case that there aren't any
	// other active changes, we need to remove the delay document for
	// this host entirely).
	if isOldest {
		s, err := dd.conn.Connect()
		if err != nil {
			return err
		}
		defer s.Close()

		coll := s.DB(dd.database).C(dd.collection)
		var result Delay
		if dd.activeChanges.size() == 0 {
			// If there are no more active changes, delete the
			// document for this host.
			change := mgo.Change{
				Remove: true,
			}

			_, err = coll.Find(bson.M{"hostname": dd.hostname}).Apply(change, &result)
			if err != nil {
				return err
			}
		} else {
			// If there are more active changes, update the
			// document for this host, setting the timestamp
			// to the value from the now oldest change.
			delay := Delay{
				Hostname: dd.hostname,
				Ts:       dd.activeChanges.head.ts,
			}
			change := mgo.Change{
				Update: bson.M{"$set": delay},
				Upsert: true,
			}

			_, err = coll.Find(bson.M{"hostname": dd.hostname}).Apply(change, &result)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// NewStaticDelayer returns a Delayer that uses a static value for determining
// determining the maximum upper-bound timestamp. It always returns
// time.Now() - delay as the maximum upper-bound timestamp.
func NewStaticDelayer(delay int) (Delayer, error) {
	return &staticDelayer{
		delay: delay,
	}, nil
}

func (sd *staticDelayer) MaxTimestamp() (int64, error) {
	return (CurrentTimestamp()) - int64(sd.delay), nil
}

func (sd *staticDelayer) BeginChange(changeId string) error {
	// noop
	return nil
}

func (sd *staticDelayer) EndChange(changeId string) error {
	// noop
	return nil
}

// ------------------------------------------------------------------------- //

// push adds a change to the end of the active requests linked list
func (a *activeChanges) push(change *activeChange) {
	a.all[change.id] = change
	if a.head == nil {
		a.head = change
	} else {
		a.tail.next = change
		change.prev = a.tail
	}
	a.tail = change
}

// remove removes a change from the active requests linked list
func (a *activeChanges) remove(changeId string) error {
	if change, ok := a.all[changeId]; ok {
		if a.isTail(changeId) && change.prev != nil {
			a.tail = change.prev
		}
		if a.isHead(changeId) && change.next != nil {
			a.head = change.next
		}
		if change.prev != nil {
			change.prev.next = change.next
		}
		if change.next != nil {
			change.next.prev = change.prev
		}

		delete(a.all, changeId)
	}
	return nil
}

// is tail returns whether or not a change is the newest active request
func (a *activeChanges) isTail(changeId string) bool {
	if a.tail != nil && a.tail.id == changeId {
		return true
	} else {
		return false
	}
}

// is tail returns whether or not a change is the oldest active request
func (a *activeChanges) isHead(changeId string) bool {
	if a.head != nil && a.head.id == changeId {
		return true
	} else {
		return false
	}
}

// size returns the number of active changes
func (a *activeChanges) size() int {
	return len(a.all)
}
