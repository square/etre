// Copyright 2017-2018, Square, Inc.

// Package etre provides API clients and low-level primitive data types.
package etre

import (
	"errors"
	"fmt"
	"log"
	"sort"
)

const (
	API_ROOT          string = "/api/v1"
	META_LABEL_ID            = "_id"
	META_LABEL_TYPE          = "_type"
	VERSION                  = "0.9.0-alpha"
	CDC_WRITE_TIMEOUT int    = 5 // seconds
)

var (
	ErrTypeMismatch   = errors.New("entity _type and Client entity type are different")
	ErrIdSet          = errors.New("entity _id is set but not allowed on insert")
	ErrIdNotSet       = errors.New("entity _id is not set")
	ErrNoEntity       = errors.New("empty entity or id slice; at least one required")
	ErrNoLabel        = errors.New("empty label slice; at least one required")
	ErrNoQuery        = errors.New("empty query string")
	ErrBadData        = errors.New("data from CDC feed is not event or control")
	ErrCallerBlocked  = errors.New("caller blocked")
	ErrEntityNotFound = errors.New("entity not found")
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
// WriteResult.Writes[].Id.
type Entity map[string]interface{}

func (e Entity) Id() string {
	return e[META_LABEL_ID].(string)
}

func (e Entity) Type() string {
	return e[META_LABEL_TYPE].(string)
}

// Has returns true of the entity has the label, regardless of its value.
func (e Entity) Has(label string) bool {
	_, ok := e[label]
	return ok
}

// A Set is a user-defined logical grouping of writes (insert, update, delete).
type Set struct {
	Id   string
	Op   string
	Size int
}

func (e Entity) Set() Set {
	set := Set{}
	if _, ok := e["_setId"]; ok {
		set.Id = e["_setId"].(string)
	}
	if _, ok := e["_setOp"]; ok {
		set.Op = e["_setOp"].(string)
	}
	if _, ok := e["_setSize"]; ok {
		set.Size = e["_setSize"].(int)
	}
	return set
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

func IsMetalabel(label string) bool {
	return metaLabels[label]
}

// Labels returns all labels, sorted, including meta-labels (_id, _type, etc.)
func (e Entity) Labels() []string {
	labels := make([]string, len(e))
	i := 0
	for label := range e {
		labels[i] = label
		i++
	}
	sort.Strings(labels)
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

// QueryFilter represents filtering options for EntityClient.Query().
type QueryFilter struct {
	// ReturnLabels defines labels included in matching entities. An empty slice
	// returns all labels, including meta-labels. Else, only labels in the slice
	// are returned.
	ReturnLabels []string
}

// WriteResult represents the result of a write operation (insert, update delete).
// On success or failure, all write ops return a WriteResult.
//
// If Error is set (not nil), some or all writes failed. Writes stop on the first
// error, so len(Writes) = index into slice of entities sent by client that failed.
// For example, if the first entity causes an error, len(Writes) = 0. If the third
// entity fails, len(Writes) = 2 (zero indexed).
type WriteResult struct {
	Writes []Write `json:"writes"`          // successful writes
	Error  *Error  `json:"error,omitempty"` // error before, during, or after writes
}

func (wr WriteResult) IsZero() bool {
	return wr.Error == nil && len(wr.Writes) == 0
}

// Write represents the successful write of one entity.
type Write struct {
	Id    string `json:"id"`              // internal _id of entity (all write ops)
	URI   string `json:"uri,omitempty"`   // fully-qualified address of new entity (insert)
	Diff  Entity `json:"diff,omitempty"`  // previous entity label values (update)
	Error string `json:"error,omitempty"` // v0.8 backward-compatibility
}

type Error struct {
	Message    string `json:"message"`
	Type       string `json:"type"`
	EntityId   string `json:"entityId"`
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

func (e Error) IsZero() bool {
	return e.Message == "" && e.Type == ""
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
