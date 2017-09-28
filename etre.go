// Package etre provides API clients and low-level primitive data types.
package etre

import (
	"errors"
	"fmt"
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

type QueryFilter struct {
	// ReturnLabels defines labels included in matching entities. An empty map
	// returns all labels. Else, only labels in the map with a true value are
	// returned. The internal ID (_id) is always returned unless explicitly
	// excluded by being set in the map with a false value.
	ReturnLabels map[string]bool // keyed on label
}

// WriteResult represents the result of a write operation (insert, update, delete)
// for one entity. The write operation failed if ErrorCode is non-zero (and Error
// will be set, too). A write operation can succeed on some entities and fail on
// others, so the caller must check all write results.
type WriteResult struct {
	Id        string      `json:"id"`              // internal _id of entity (all write ops)
	URI       string      `json:"uri,omitempty"`   // fully-qualified address of new entity (insert)
	Diff      interface{} `json:"diff,omitempty"`  // previous entity label values (update)
	ErrorCode byte        `json:"errorCode"`       // an ERRCODE const
	Error     string      `json:"error,omitempty"` // human-readable error string
}

type CDCEvent struct {
	EventId    string  `json:"eventId"`
	EntityId   string  `json:"entityId"`
	EntityType string  `json:"entityType"`
	Ts         int64   `json:"ts"` // Unix nanoseconds
	User       string  `json:"user"`
	Op         string  `json:"op"`  // i=insert, u=update, d=delete
	Old        *Entity `json:"old"` // old values of affected labels, null on insert
	New        *Entity `json:"new"` // new values of affected labels, null on delete
}

// Latency represents network latencies in milliseconds.
type Latency struct {
	Send int64 // client -> server
	Recv int64 // server -> client
	RTT  int64 // client -> server -> client
}

// //////////////////////////////////////////////////////////////////////////
// Errors
// //////////////////////////////////////////////////////////////////////////

type ErrorResponse struct {
	Message string `json:"message"`
	Type    string `json:"type"`
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

const (
	ERRCODE_NONE      byte = iota // 0 = no error
	ERRCODE_DUPLICATE             // duplicate entity on insert
	ERRCODE_NOT_FOUND             // entity not found on update or labels
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
	if len(wr) == len(entities) && wr[len(wr)-1].ErrorCode == 0 {
		return nil
	}

	// An error occurred
	wrLast := wr[len(wr)-1]
	return fmt.Errorf("write error: entity %d: %s (code %d)", len(wr)-1, wrLast.Error, wrLast.ErrorCode)
}
