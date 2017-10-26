// Copyright 2017, Square, Inc.

// Package entity is a connector to execute CRUD commands for a single entity and
// many entities on a DB instance.
//
// A note on querying limitations:
//
//   This package uses the Kubernetes Labels Selector syntax (KLS) for its
//   query language. There are some querying limitations with using KLS which
//   is explained below:
//
//     Querying many entities: there are some limitations.
//
//       Because of limitations in KLS, a caller can only query by field names
//       that are alphanumeric characters, '-', '_' or '.', and that start and
//       end with an alphanumeric character. The limitations also extend to
//       operators and values. Less than and greater than operators will
//       interpret their values as an integer.  All other operators will
//       interpret their values as a string. For example, caller cannot query
//       for all documents that have field name "x" with value 2.
//
//     Creating entities: there are no limitations.
//
//       CreateEntities will allow you to create any entity that's a map of a
//       string to interface{}. However, given the query limitations explained
//       above, if you expect to be able to query for a field/value, ensure you
//       only create an entity that would then satisfy a query.
//
package entity

import (
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/square/etre"
	"github.com/square/etre/cdc"
	"github.com/square/etre/db"
	"github.com/square/etre/query"

	"github.com/rs/xid"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// Store interface has methods needed to do CRUD operations on entities.
type Store interface {
	// Managing labels for a single entity
	DeleteEntityLabel(string, string, string) (etre.Entity, error)

	// Managing Multiple entities
	CreateEntities(string, []etre.Entity, string) ([]string, error)
	ReadEntities(string, query.Query) ([]etre.Entity, error)
	UpdateEntities(string, query.Query, etre.Entity, string) ([]etre.Entity, error)
	DeleteEntities(string, query.Query, string) ([]etre.Entity, error)
}

// store struct stores all info needed to connect and query a mongo instance. It
// implements the the Store interface.
type store struct {
	conn        db.Connector
	database    string
	entityTypes []string
	cdcs        cdc.Store
	d           cdc.Delayer
}

// Map of Kubernetes Selection Operator to mongoDB Operator.
//
// Relevant documentation:
//
//     https://github.com/kubernetes/apimachinery/blob/master/pkg/selection/operator.go
//     https://docs.mongodb.com/manual/reference/operator/query/#query-selectors
//
var operatorMap = map[string]string{
	"in":    "$in",
	"notin": "$nin",
	"=":     "$eq",
	"==":    "$eq",
	"!=":    "$ne",
	"lt":    "$lt",
	"gt":    "$gt",
}

var reservedNames = []string{"entity", "entities"}

// NewStore creates a Store.
func NewStore(conn db.Connector, database string, entityTypes []string, cdcs cdc.Store, d cdc.Delayer) (Store, error) {
	// Ensure no entityType name is a reserved word
	for _, r := range reservedNames {
		for _, c := range entityTypes {
			if r == c {
				return nil, errors.New(fmt.Sprintf("Entity type (%s) cannot be a reserved word (%s).", c, r))
			}
		}
	}

	return &store{
		conn:        conn,
		database:    database,
		entityTypes: entityTypes,
		cdcs:        cdcs,
		d:           d,
	}, nil
}

// DeleteEntityLabel deletes a label from an entity.
//
// Relevant documentation:
//
//     https://docs.mongodb.com/manual/reference/operator/update/unset/#up._S_unset
//
func (s *store) DeleteEntityLabel(entityType string, id string, label string) (etre.Entity, error) {
	// @todo: make sure this is properly creating CDC events once it starts getting used.
	return nil, fmt.Errorf("DeleteEntityLabel function needs more work before it can be used")

	if !validEntityType(s, entityType) {
		return nil, fmt.Errorf("Invalid entityType name: %s. Valid entityType names: %s.", entityType, s.entityTypes)
	}

	ms, err := s.conn.Connect()
	if err != nil {
		return nil, err
	}
	defer ms.Close()
	c := ms.DB(s.database).C(entityType)

	// ReturnNew: false is the default option. Will return the original document
	// before update was performed.
	change := mgo.Change{
		Update:    bson.M{"$unset": bson.M{label: ""}},
		ReturnNew: false,
	}

	var diff etre.Entity
	// We call Select so that Apply will return the orginal deleted label (and
	// "_id" field, which is included by default) rather than returning the
	// entire original document
	_, err = c.Find(bson.M{"_id": bson.ObjectIdHex(id)}).Select(bson.M{label: 1}).Apply(change, &diff)
	if err != nil {
		return nil, ErrDeleteLabel{DbError: err}
	}

	return diff, nil
}

// CreateEntities inserts many entities into DB. This method allows for partial
// success and failure which means the return value and error are _not_
// mutually exclusive. Caller should check and handle both.
//
// Mgo does not return a useful error when inserting many documents and a
// failure occurs (e.g. want to insert 4, 3 are inserted ok but the 4th fails).
// So, CreateEntities inserts entities one at a time in order to return known
// inserted entities to caller.
//
// If all entities are successfully inserted, a slice of their IDs are
// returned. If some inserts failed, a slice of the successfully inserted
// entities are returned alone with an error that contains the total number of
// entities inserted. Since the entities were inserted in order (guranteed by
// inserting one by one), caller should only return subset of entities that
// failed to be inserted.
func (s *store) CreateEntities(entityType string, entities []etre.Entity, user string) ([]string, error) {
	if !validEntityType(s, entityType) {
		return nil, errors.New(fmt.Sprintf("Invalid entityType name (%s). Valid entityType names: %s.", entityType, s.entityTypes))
	}

	for i, e := range entities {
		if _, ok := e["_id"]; ok {
			return nil, fmt.Errorf("_id set in entity[%d]; _id cannot be set on insert", i)
		}
		if _, ok := e["_type"]; ok {
			return nil, fmt.Errorf("_type set in entity[%d]; _type cannot be set on insert", i)
		}
		if _, ok := e["_rev"]; ok {
			return nil, fmt.Errorf("_rev set in entity[%d]; _rev cannot be set on insert", i)
		}
	}

	ms, err := s.conn.Connect()
	if err != nil {
		return nil, err
	}
	defer ms.Close()
	c := ms.DB(s.database).C(entityType)

	// Notify the delay manager that a change is starting, and then notify the
	// delay manager again when the change is done.
	changeId := xid.New().String()
	err = s.d.BeginChange(changeId)
	if err != nil {
		return nil, err
	}
	// @todo: don't ignore error...this should report an exception or something
	defer s.d.EndChange(changeId)

	// A slice of IDs we generate to insert along with entities into DB
	insertedObjectIds := make([]string, 0, len(entities))

	for _, e := range entities {
		// Mgo driver does not return the ObjectId that Mongo creates, so create it ourself.
		id := bson.NewObjectId()
		e["_id"] = id
		e["_type"] = entityType
		e["_rev"] = uint(0)

		// Remove the set* fields from the entity, but keep track of them because we
		// will need them for the CDC event.
		e, setId, setOp, setSize := removeSetFields(e)

		if err := c.Insert(e); err != nil {
			return insertedObjectIds, ErrCreate{DbError: err, N: len(insertedObjectIds)}
		}

		// bson.ObjectId.String() yields "ObjectId("abc")", but we need to report only "abc",
		// so re-encode the raw bytes to a hex string. This make GET /entity/{t}/abc work.
		insertedObjectIds = append(insertedObjectIds, hex.EncodeToString([]byte(id)))

		// Create a CDC event.
		eventId := bson.NewObjectId()
		event := etre.CDCEvent{
			EventId:    hex.EncodeToString([]byte(eventId)),
			EntityId:   hex.EncodeToString([]byte(id)),
			EntityType: entityType,
			Rev:        e["_rev"].(uint),
			Ts:         time.Now().UnixNano() / int64(time.Millisecond),
			User:       user,
			Op:         "i",
			Old:        nil,
			New:        &e,
			SetId:      setId,
			SetOp:      setOp,
			SetSize:    setSize,
		}
		err = s.cdcs.Write(event)
		if err != nil {
			return insertedObjectIds, err
		}
	}

	return insertedObjectIds, nil
}

// ReadEntities queries the db and returns a slice of Entity objects if
// something is found, a nil slice if nothing is found, and an error if one
// occurs.
func (s *store) ReadEntities(entityType string, q query.Query) ([]etre.Entity, error) {
	if !validEntityType(s, entityType) {
		return nil, errors.New(fmt.Sprintf("Invalid entityType name (%s). Valid entityType names: %s.", entityType, s.entityTypes))
	}

	ms, err := s.conn.Connect()
	if err != nil {
		return nil, err
	}
	defer ms.Close()
	c := ms.DB(s.database).C(entityType)

	mgoQuery := translateQuery(q)

	var entities []etre.Entity
	err = c.Find(mgoQuery).All(&entities)
	if err != nil {
		return nil, ErrRead{DbError: err}
	}

	return entities, nil
}

// UpdateEntities queries the db and updates all Entity matching that query.
// This method allows for partial success and failure which means the return
// value and error are _not_ mutually exclusive. Caller should check and handle
// both.
//
// Returns a slice of partial entities ("_id" field and changed fields only)
// for the ones updated and an error if there is one. For example, if 4
// entities were supposed to be updated and 3 are ok and the 4th fails, a slice
// with 3 updated entities and an error will be returned.
//
//   q, _ := query.Translate("y=foo")
//   update := db.Entity{"y": "bar"}
//
//   diffs, err := c.UpdateEntities(q, update)
//
func (s *store) UpdateEntities(t string, q query.Query, u etre.Entity, user string) ([]etre.Entity, error) {
	if !validEntityType(s, t) {
		return nil, errors.New(fmt.Sprintf("Invalid entityType name (%s). Valid entityType names: %s.", t, s.entityTypes))
	}

	mgoQuery := translateQuery(q)

	ms, err := s.conn.Connect()
	if err != nil {
		return nil, err
	}
	defer ms.Close()

	entityType := ms.DB(s.database).C(t)

	// We can only call update on one doc at a time, so we generate a slice of IDs
	// for docs to update.
	ids, err := idsForQuery(entityType, mgoQuery)
	if err != nil {
		return nil, ErrUpdate{DbError: err}
	}

	// Change to make
	change := mgo.Change{
		Update: bson.M{
			"$set": u,
			"$inc": bson.M{
				"_rev": 1, // increment the revision
			},
		},
		// Return the original doc before modifications. This is the default
		// option.
		ReturnNew: false,
	}

	// Notify the delay manager that a change is starting, and then notify the
	// delay manager again when the change is done.
	changeId := xid.New().String()
	err = s.d.BeginChange(changeId)
	if err != nil {
		return nil, err
	}
	// @todo: don't ignore error...this should report an exception or something
	defer func() { s.d.EndChange(changeId) }()

	// diffs is a slice made up of a diff for each doc updated
	diffs := make([]etre.Entity, 0, len(ids))

	// Query for each document and apply update
	for _, id := range ids {
		// Remove the set* fields from the entity, but keep track of them because we
		// will need them for the CDC event.
		u, setId, setOp, setSize := removeSetFields(u)

		var diff etre.Entity
		// We call Select so that Apply will return only the fields we select
		// ("_id" field and changed fields) rather than it returning the original
		// document.
		_, err := entityType.Find(bson.M{"_id": id}).Select(selectMap(u)).Apply(change, &diff)
		if err != nil {
			return diffs, ErrUpdate{DbError: err}
		}

		diffs = append(diffs, diff)

		// Add id and rev to the old entity for the CDC event.
		newRev := diff["_rev"].(uint) + 1 // +1 since we get the old document back
		u["_id"] = id
		u["_rev"] = newRev

		// Create a CDC event.
		eventId := bson.NewObjectId()
		event := etre.CDCEvent{
			EventId:    hex.EncodeToString([]byte(eventId)),
			EntityId:   hex.EncodeToString([]byte(id)),
			EntityType: t,
			Rev:        newRev,
			Ts:         time.Now().UnixNano() / int64(time.Millisecond),
			User:       user,
			Op:         "u",
			Old:        &diff,
			New:        &u,
			SetId:      setId,
			SetOp:      setOp,
			SetSize:    setSize,
		}
		err = s.cdcs.Write(event)
		if err != nil {
			return diffs, err
		}
	}

	return diffs, nil
}

// DeleteEntities queries the db and deletes all Entity matching that query.
// This method allows for partial success and failure which means the return
// value and error are _not_ mutually exclusive. Caller should check and handle
// both.
//
// Returns a slice of successfully deleted entities an error if there is one.
// For example, if 4 entities were supposed to be deleted and 3 are ok and the
// 4th fails, a slice with 3 deleted entities and an error will be returned.
func (s *store) DeleteEntities(t string, q query.Query, user string) ([]etre.Entity, error) {
	if !validEntityType(s, t) {
		return nil, errors.New(fmt.Sprintf("Invalid entityType name (%s). Valid entityType names: %s.", t, s.entityTypes))
	}

	mgoQuery := translateQuery(q)

	ms, err := s.conn.Connect()
	if err != nil {
		return nil, err
	}
	defer ms.Close()

	entityType := ms.DB(s.database).C(t)

	// List of IDs for docs to update
	ids, err := idsForQuery(entityType, mgoQuery)
	if err != nil {
		return nil, ErrDelete{DbError: err}
	}

	// Change to make
	change := mgo.Change{
		Remove: true,
		// Return the original doc before modifications. This is the default
		// option.
		ReturnNew: false,
	}

	// deletedEntities is a slice of entities that have been successfully deleted
	deletedEntities := make([]etre.Entity, 0, len(ids))

	// Notify the delay manager that a change is starting, and then notify the
	// delay manager again when the change is done.
	changeId := xid.New().String()
	err = s.d.BeginChange(changeId)
	if err != nil {
		return nil, err
	}
	// @todo: don't ignore error...this should report an exception or something
	defer func() { s.d.EndChange(changeId) }()

	// Query for each document and delete it
	for _, id := range ids {
		var deletedEntity etre.Entity
		_, err := entityType.FindId(id).Apply(change, &deletedEntity)
		if err != nil {
			switch err {
			case mgo.ErrNotFound:
				// ignore
			default:
				return deletedEntities, ErrDelete{DbError: err}
			}
		}

		deletedEntities = append(deletedEntities, deletedEntity)

		// Create a CDC event.
		eventId := bson.NewObjectId()
		event := etre.CDCEvent{
			EventId:    hex.EncodeToString([]byte(eventId)),
			EntityId:   hex.EncodeToString([]byte(id)),
			EntityType: t,
			Rev:        deletedEntity["_rev"].(uint) + 1, // +1 since we get the old document back
			Ts:         time.Now().UnixNano() / int64(time.Millisecond),
			User:       user,
			Op:         "d",
			Old:        &deletedEntity,
			New:        nil,
		}
		err = s.cdcs.Write(event)
		if err != nil {
			return deletedEntities, err
		}
	}

	return deletedEntities, nil
}

// Translates Query object to bson.M object, which will
// be used to query DB.
func translateQuery(q query.Query) bson.M {
	mgoQuery := make(bson.M)
	for _, p := range q.Predicates {
		switch p.Operator {
		case "exists":
			mgoQuery[p.Label] = bson.M{"$exists": true}
		case "!":
			mgoQuery[p.Label] = bson.M{"$exists": false}
		default:
			mgoQuery[p.Label] = bson.M{operatorMap[p.Operator]: p.Value}
		}
	}
	return mgoQuery
}

// idsForQuery gets all ids of docs that satisfy query
func idsForQuery(c *mgo.Collection, q bson.M) ([]bson.ObjectId, error) {
	var resultIds []map[string]bson.ObjectId
	// Select only "_id" field in returned results
	err := c.Find(q).Select(bson.M{"_id": 1}).All(&resultIds)
	if err != nil {
		// Return raw, low-level error to allow caller wrap into higher level
		// error.
		return nil, err
	}

	ids := make([]bson.ObjectId, len(resultIds))

	for i := range ids {
		ids[i] = resultIds[i]["_id"]
	}

	return ids, nil
}

// selectMap is a map of field name to int of value 1. It is used in Select
// query to tell DB which fields to return. "_id" field is included in return
// by default. We also want to always include "_rev".
//
// Relevant documentation:
//
//     https://docs.mongodb.com/v3.0/tutorial/project-fields-from-query-results/
//
func selectMap(e etre.Entity) map[string]int {
	selectMap := map[string]int{
		"_id":   1,
		"_type": 1,
		"_rev":  1,
	}
	for k, _ := range e {
		selectMap[k] = 1
	}
	selectMap["_rev"] = 1

	return selectMap
}

// validEntityType checks if the entityType passed in is in the entityType list of the connector
func validEntityType(s *store, c string) bool {
	for _, entityType := range s.entityTypes {
		if c == entityType {
			return true
		}
	}
	return false
}

func removeSetFields(e etre.Entity) (etre.Entity, string, string, int) {
	var setId, setOp string
	var setSize int
	if _, ok := e["setId"]; ok {
		setId = e["setId"].(string)
	}
	if _, ok := e["setOp"]; ok {
		setOp = e["setOp"].(string)
	}
	if _, ok := e["setSize"]; ok {
		setSize = e["setSize"].(int)
	}

	modified := etre.Entity{}
	for k, v := range e {
		if k == "setId" || k == "setOp" || k == "setSize" {
			continue
		}
		modified[k] = v
	}

	return modified, setId, setOp, setSize
}

// ErrCreate is a higher level error for caller to check against when calling
// CreateEntities. It holds the lower level error message from DB and the
// number of entities successfully inserted on create.
type ErrCreate struct {
	DbError error
	N       int // number of entities successfully created
}

func (e ErrCreate) Error() string {
	return e.DbError.Error()
}

// ErrRead is a higher level error for caller to check against when calling
// ReadEntities. It holds the lower level error message from DB.
type ErrRead struct {
	DbError error
}

func (e ErrRead) Error() string {
	return e.DbError.Error()
}

// ErrUpdate is a higher level error for caller to check against when calling
// UpdateEntities. It holds the lower level error message from DB.
type ErrUpdate struct {
	DbError error
}

func (e ErrUpdate) Error() string {
	return e.DbError.Error()
}

// ErrDelete is a higher level error for caller to check against when calling
// DeleteEntities. It holds the lower level error message from DB.
type ErrDelete struct {
	DbError error
}

func (e ErrDelete) Error() string {
	return e.DbError.Error()
}

// ErrDeleteLabel is a higher level error for caller to check against when calling
// DeleteEntityLabel. It holds the lower level error message from DB.
type ErrDeleteLabel struct {
	DbError error
}

func (e ErrDeleteLabel) Error() string {
	return e.DbError.Error()
}
