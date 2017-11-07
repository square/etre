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

// WriteOp represents common metadata for insert, update, and delete Store methods.
type WriteOp struct {
	User       string // required
	EntityType string // required
	EntityId   string // optional

	// Delete ops do not support set ops like insert and update because no
	// entities are sent by the client on delete. However, the caller can do
	// DELETE /entity/node?setOp=foo&setId=bar&setSize=2 and the controller
	// will pass along the set op values. This could (but not currently) also
	// be used to impose/inject a set op on a write op that doesn't specify
	// a set op.
	SetOp   string // optional
	SetId   string // optional
	SetSize int    // optional
}

// Store interface has methods needed to do CRUD operations on entities.
type Store interface {
	// Managing labels for a single entity
	DeleteEntityLabel(WriteOp, string) (etre.Entity, error)

	// Managing multiple entities
	ReadEntities(string, query.Query, etre.QueryFilter) ([]etre.Entity, error)
	CreateEntities(WriteOp, []etre.Entity) ([]string, error)
	UpdateEntities(WriteOp, query.Query, etre.Entity) ([]etre.Entity, error)
	DeleteEntities(WriteOp, query.Query) ([]etre.Entity, error)
}

// store struct stores all info needed to connect and query a mongo instance. It
// implements the the Store interface.
type store struct {
	conn        db.Connector
	database    string
	entityTypes []string
	cdcs        cdc.Store
	dm          cdc.Delayer
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
	"<":     "$lt",
	"<=":    "$lte",
	">":     "$gt",
	">=":    "$gte",
}

var reservedNames = []string{"entity", "entities"}

// NewStore creates a Store.
func NewStore(conn db.Connector, database string, entityTypes []string, cdcs cdc.Store, dm cdc.Delayer) (Store, error) {
	// Ensure no entityType name is a reserved word
	// @todo: move higher, in main.go, and expose (or document) which words are reserved
	for _, r := range reservedNames {
		for _, c := range entityTypes {
			if r == c {
				return nil, errors.New(fmt.Sprintf("entity type %s is a reserved word", c))
			}
		}
	}

	return &store{
		conn:        conn,
		database:    database,
		entityTypes: entityTypes,
		cdcs:        cdcs,
		dm:          dm,
	}, nil
}

// DeleteEntityLabel deletes a label from an entity.
//
// Relevant documentation:
//
//     https://docs.mongodb.com/manual/reference/operator/update/unset/#up._S_unset
//
func (s *store) DeleteEntityLabel(wo WriteOp, label string) (etre.Entity, error) {
	// @todo: make sure this is properly creating CDC events once it starts getting used.
	return nil, fmt.Errorf("DeleteEntityLabel function needs more work before it can be used")

	if err := s.validate(wo); err != nil {
		return nil, err
	}

	// Connect to Mongo collection for the entity type
	ms, err := s.conn.Connect()
	if err != nil {
		return nil, err
	}
	defer ms.Close()
	c := ms.DB(s.database).C(wo.EntityType)

	// ReturnNew: false is the default option. Will return the original document
	// before update was performed.
	change := mgo.Change{
		Update:    bson.M{"$unset": bson.M{label: ""}},
		ReturnNew: false,
	}

	diff := etre.Entity{}
	// We call Select so that Apply will return the orginal deleted label (and
	// "_id" field, which is included by default) rather than returning the
	// entire original document
	_, err = c.Find(bson.M{"_id": bson.ObjectIdHex(wo.EntityId)}).Select(bson.M{label: 1}).Apply(change, &diff)
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
func (s *store) CreateEntities(wo WriteOp, entities []etre.Entity) ([]string, error) {
	if err := s.validate(wo); err != nil {
		return nil, err
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

	// Connect to Mongo collection for the entity type
	ms, err := s.conn.Connect()
	if err != nil {
		return nil, err
	}
	defer ms.Close()
	c := ms.DB(s.database).C(wo.EntityType)

	// Notify the delay manager that a change is starting, and then notify the
	// delay manager again when the change is done.
	changeId := xid.New().String()
	err = s.dm.BeginChange(changeId)
	if err != nil {
		return nil, err
	}
	// @todo: don't ignore error...this should report an exception or something
	defer s.dm.EndChange(changeId)

	// A slice of IDs we generate to insert along with entities into DB
	insertedObjectIds := make([]string, 0, len(entities))

	for _, e := range entities {
		// Mgo driver does not return the ObjectId that Mongo creates, so create it ourself.
		id := bson.NewObjectId()
		e["_id"] = id
		e["_type"] = wo.EntityType
		e["_rev"] = 0

		if err := c.Insert(e); err != nil {
			return insertedObjectIds, ErrCreate{DbError: err, N: len(insertedObjectIds)}
		}

		// bson.ObjectId.String() yields "ObjectId("abc")", but we need to report only "abc",
		// so re-encode the raw bytes to a hex string. This make GET /entity/{t}/abc work.
		insertedObjectIds = append(insertedObjectIds, hex.EncodeToString([]byte(id)))

		// Get set op values from entity or wo, in that order.
		set := setValues(e, wo)

		// Create a CDC event.
		eventId := bson.NewObjectId()
		event := etre.CDCEvent{
			EventId:    hex.EncodeToString([]byte(eventId)),
			EntityId:   hex.EncodeToString([]byte(id)),
			EntityType: wo.EntityType,
			Rev:        uint(e["_rev"].(int)),
			Ts:         time.Now().UnixNano() / int64(time.Millisecond),
			User:       wo.User,
			Op:         "i",
			Old:        nil,
			New:        &e,
			SetId:      set.Id,
			SetOp:      set.Op,
			SetSize:    set.Size,
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
func (s *store) ReadEntities(entityType string, q query.Query, f etre.QueryFilter) ([]etre.Entity, error) {
	if err := s.validEntityType(entityType); err != nil {
		return nil, err
	}

	// Connect to Mongo collection for the entity type
	ms, err := s.conn.Connect()
	if err != nil {
		return nil, err
	}
	defer ms.Close()
	c := ms.DB(s.database).C(entityType)

	mgoQuery := translateQuery(q)

	entities := []etre.Entity{}
	if len(f.ReturnLabels) == 0 {
		err = c.Find(mgoQuery).All(&entities)
	} else {
		selectMap := map[string]int{}
		selectMap["_id"] = 0 // Mongo requires _id to be explicitly excluded
		for _, rl := range f.ReturnLabels {
			selectMap[rl] = 1
		}
		err = c.Find(mgoQuery).Select(selectMap).All(&entities)
	}
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
func (s *store) UpdateEntities(wo WriteOp, q query.Query, patch etre.Entity) ([]etre.Entity, error) {
	if err := s.validate(wo); err != nil {
		return nil, err
	}

	for label := range patch {
		if etre.IsMetalabel(label) {
			return nil, fmt.Errorf("updating metalabels is not allowed")
		}
	}

	mgoQuery := translateQuery(q)

	// Connect to Mongo collection for the entity type
	ms, err := s.conn.Connect()
	if err != nil {
		return nil, err
	}
	defer ms.Close()
	c := ms.DB(s.database).C(wo.EntityType)

	// We can only call update on one doc at a time, so we generate a slice of IDs
	// for docs to update.
	ids, err := idsForQuery(c, mgoQuery)
	if err != nil {
		return nil, ErrUpdate{DbError: err}
	}

	// Change to make
	change := mgo.Change{
		Update: bson.M{
			"$set": patch,
			"$inc": bson.M{
				"_rev": 1, // increment the revision
			},
		},
		// Return the original doc before modifications. This is the default
		// option.
		ReturnNew: false,
	}

	// Get set op values from patch or wo, in that order.
	set := setValues(patch, wo)

	// Notify the delay manager that a change is starting, and then notify the
	// delay manager again when the change is done.
	changeId := xid.New().String()
	err = s.dm.BeginChange(changeId)
	if err != nil {
		return nil, err
	}
	// @todo: don't ignore error...this should report an exception or something
	defer func() { s.dm.EndChange(changeId) }()

	// diffs is a slice made up of a diff for each doc updated
	diffs := make([]etre.Entity, 0, len(ids))

	// Query for each document and apply update
	affectedLabels := selectMap(patch)

	for _, id := range ids {
		// We call Select so that Apply will return only the fields we select
		// ("_id" field and changed fields) rather than it returning the original
		// document.
		var diff etre.Entity
		_, err := c.Find(bson.M{"_id": id}).Select(affectedLabels).Apply(change, &diff)
		if err != nil {
			return diffs, ErrUpdate{DbError: err}
		}

		diffs = append(diffs, diff)

		newRev := diff["_rev"].(int) + 1 // +1 since we get the old document back

		new := etre.Entity{
			"_id":   id,
			"_type": wo.EntityType,
			"_rev":  newRev,
		}
		for k, v := range patch {
			new[k] = v
		}

		// Create a CDC event.
		eventId := bson.NewObjectId()
		event := etre.CDCEvent{
			EventId:    hex.EncodeToString([]byte(eventId)),
			EntityId:   hex.EncodeToString([]byte(id)),
			EntityType: wo.EntityType,
			Rev:        uint(newRev),
			Ts:         time.Now().UnixNano() / int64(time.Millisecond),
			User:       wo.User,
			Op:         "u",
			Old:        &diff,
			New:        &new,
			SetId:      set.Id,
			SetOp:      set.Op,
			SetSize:    set.Size,
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
func (s *store) DeleteEntities(wo WriteOp, q query.Query) ([]etre.Entity, error) {
	if err := s.validate(wo); err != nil {
		return nil, err
	}

	mgoQuery := translateQuery(q)

	// Connect to Mongo collection for the entity type
	ms, err := s.conn.Connect()
	if err != nil {
		return nil, err
	}
	defer ms.Close()
	c := ms.DB(s.database).C(wo.EntityType)

	// List of IDs for docs to update
	ids, err := idsForQuery(c, mgoQuery)
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
	err = s.dm.BeginChange(changeId)
	if err != nil {
		return nil, err
	}
	// @todo: don't ignore error...this should report an exception or something
	defer func() { s.dm.EndChange(changeId) }()

	// Query for each document and delete it
	for _, id := range ids {
		var deletedEntity etre.Entity
		_, err := c.FindId(id).Apply(change, &deletedEntity)
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
			EntityType: wo.EntityType,
			Rev:        uint(deletedEntity["_rev"].(int)) + 1, // +1 since we get the old document back
			Ts:         time.Now().UnixNano() / int64(time.Millisecond),
			User:       wo.User,
			Op:         "d",
			Old:        &deletedEntity,
			New:        nil,
			SetId:      wo.SetId,
			SetOp:      wo.SetOp,
			SetSize:    wo.SetSize,
		}
		err = s.cdcs.Write(event)
		if err != nil {
			return deletedEntities, err
		}
	}

	return deletedEntities, nil
}

// //////////////////////////////////////////////////////////////////////////
// Private funcs
// //////////////////////////////////////////////////////////////////////////

func (s *store) validate(wo WriteOp) error {
	if err := s.validEntityType(wo.EntityType); err != nil {
		return err
	}

	if wo.User == "" {
		return fmt.Errorf("User not set. User is set by config.server.username_header.")
	}

	return nil
}

func (s *store) validEntityType(entityType string) error {
	for _, t := range s.entityTypes {
		if t == entityType {
			return nil
		}
	}
	return fmt.Errorf("Invalid entity type: %s. Valid entity types: %s. Entity types are set by config.entity.types.",
		entityType, s.entityTypes)
}

func setValues(e etre.Entity, wo WriteOp) etre.Set {
	set := e.Set()
	if set.Size == 0 && wo.SetSize > 0 {
		set.Op = wo.SetOp
		set.Id = wo.SetId
		set.Size = wo.SetSize
	}
	return set
}

// Translates Query object to bson.M object, which will
// be used to query DB.
func translateQuery(q query.Query) bson.M {
	mgoQuery := make(bson.M)
	for _, p := range q.Predicates {
		switch p.Operator {
		case "exists":
			mgoQuery[p.Label] = bson.M{"$exists": true}
		case "notexists":
			mgoQuery[p.Label] = bson.M{"$exists": false}
		default:
			if p.Label == etre.META_LABEL_ID {
				switch p.Value.(type) {
				case string:
					mgoQuery[p.Label] = bson.M{"$eq": bson.ObjectIdHex(p.Value.(string))}
				case bson.ObjectId:
					mgoQuery[p.Label] = bson.M{"$eq": p.Value}
				default:
					panic(fmt.Sprintf("invalid _id value type: %T", p.Value))
				}
			} else {
				mgoQuery[p.Label] = bson.M{operatorMap[p.Operator]: p.Value}
			}
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
	return selectMap
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
