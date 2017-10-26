// Package etre provides API clients and low-level primitive data types.
package etre

import (
	"errors"
	"fmt"
	"log"
)

const (
	API_ROOT          string = "/api/v1"
	META_LABEL_ID            = "_id"
	META_LABEL_TYPE          = "_type"
	CDC_WRITE_TIMEOUT int    = 5 // seconds
)

// Entity represents a single Etre entity. The caller is responsible for knowing
// or determining the type of value for each key.
//
// If label _type is set, the Client verifies that it matches its type. For example,
// if _type = "foo", Insert or Update with a Client bound to entity type "bar"
// returns ErrTypeMismatch. If label _type is not set, the Client entity type is
// presumed.
//
// Label _id cannot be set on insert. If set, Insert returns ErrIdSet. On update,
// label _id must be set; if not, Update returns ErrIdNotSet. _id corresponds to
// WriteResult.Id.
type Entity map[string]interface{}

func (e Entity) Id() string {
	return e["_id"].(string)
}

func (e Entity) Type() string {
	return e["_type"].(string)
}

// Has returns true of the entity has the label, regardless of its value.
func (e Entity) Has(label string) bool {
	_, ok := e[label]
	return ok
}

var metaLabels = map[string]bool{
	"_id":      true,
	"_rev":     true,
	"_setId":   true,
	"_setOp":   true,
	"_setSize": true,
	"_ts":      true,
	"_type":    true,
}

func (e Entity) Labels() []string {
	labels := make([]string, 0, len(e))
	for label := range e {
		if metaLabels[label] {
			continue
		}
		labels = append(labels, label)
	}
	return labels
}

// String returns the string value of the label. If the label is not set or
// its value is not a string, an empty string is returned.
func (e Entity) String(label string) string {
	v := e[label]
	switch v.(type) {
	case string:
		return v.(string)
	}
	return ""
}

type QueryFilter struct {
	// ReturnLabels defines labels included in matching entities. An empty map
	// returns all labels. Else, only labels in the map with a true value are
	// returned. The internal ID (_id) is always returned unless explicitly
	// excluded by being set in the map with a false value.
	ReturnLabels map[string]bool // keyed on label
}

// WriteResult represents the result of a write operation (insert, update, delete)
// for one entity. The write operation failed if Error is set. A write operation
// can succeed on some entities and fail on one, so the caller must check all write
// results.
type WriteResult struct {
	Id    string `json:"id"`              // internal _id of entity (all write ops)
	URI   string `json:"uri,omitempty"`   // fully-qualified address of new entity (insert)
	Diff  Entity `json:"diff,omitempty"`  // previous entity label values (update)
	Error string `json:"error,omitempty"` // human-readable error string
}

type CDCEvent struct {
	EventId    string  `json:"eventId" bson:"eventId"`
	EntityId   string  `json:"entityId" bson:"entityId"`     // _id of entity
	EntityType string  `json:"entityType" bson:"entityType"` // user-defined
	Rev        uint    `json:"rev" bson:"rev"`               // entity revision as of this op, 0 on insert
	Ts         int64   `json:"ts" bson:"ts"`                 // Unix nanoseconds
	User       string  `json:"user" bson:"user"`
	Op         string  `json:"op" bson:"op"`                       // i=insert, u=update, d=delete
	Old        *Entity `json:"old,omitempty" bson:"old,omitempty"` // old values of affected labels, null on insert
	New        *Entity `json:"new,omitempty" bson:"new,omitempty"` // new values of affected labels, null on delete

	// Set op fields are optional, copied from entity if set. The three
	// fields are all or nothing: all should be set, or none should be set.
	// Etre has no semantic awareness of set op values, nor does it validate
	// them. The caller is responsible for ensuring they're correct.
	SetId   string `json:"setId,omitempty" bson:"setId,omitempty"`
	SetOp   string `json:"setOp,omitempty" bson:"setOp,omitempty"`
	SetSize int    `json:"setSize,omitempty" bson:"setSize,omitempty"`
}

// Latency represents network latencies in milliseconds.
type Latency struct {
	Send int64 // client -> server
	Recv int64 // server -> client
	RTT  int64 // client -> server -> client
}

var Debug = false

func debug(fmt string, v ...interface{}) {
	if !Debug {
		return
	}
	log.Printf(fmt, v...)
}

// //////////////////////////////////////////////////////////////////////////
// Errors
// //////////////////////////////////////////////////////////////////////////

type Error struct {
	Message    string `json:"message"`
	Type       string `json:"type"`
	HTTPStatus int
}

func (e Error) New(msgFmt string, msgArgs ...interface{}) Error {
	if msgFmt != "" {
		e.Message = fmt.Sprintf(msgFmt, msgArgs...)
	}
	return e
}

func (e Error) String() string {
	return fmt.Sprintf("Etre error %s: %s", e.Type, e.Message)
}

func (e Error) Error() string {
	return e.String()
}

var (
	ErrTypeMismatch  = errors.New("entity _type and Client entity type are different")
	ErrIdSet         = errors.New("entity _id is set but not allowed on insert")
	ErrIdNotSet      = errors.New("entity _id is not set")
	ErrNoEntity      = errors.New("empty entity or id slice; at least one required")
	ErrNoLabel       = errors.New("empty label slice; at least one required")
	ErrNoQuery       = errors.New("empty query string")
	ErrBadData       = errors.New("data from CDC feed is not event or control")
	ErrCallerBlocked = errors.New("caller blocked")
)

// WriteError is a convenience function for returning the WriteResult error, if any,
// for the given entities that generated the write results. Canonical usage:
//
//   wr, err := ec.Insert(entities)
//   if err != nil {
//     return err
//   }
//   if err := etre.WriteError(wr, entities); err != nil {
//     return err
//   }
//
func WriteError(wr []WriteResult, entities []Entity) error {
	if len(wr) == 0 {
		return fmt.Errorf("no write results; check API logs for errors")
	}

	// If the number of write results = the number of entities _and_
	// the last write result is not an error, then no error occurred.
	if len(wr) == len(entities) && wr[len(wr)-1].Error == "" {
		return nil
	}

	// An error occurred
	wrLast := wr[len(wr)-1]
	return fmt.Errorf("error writing entity[%d]: %s", len(wr)-1, wrLast.Error)
}
