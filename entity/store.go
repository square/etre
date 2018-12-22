// Copyright 2017-2018, Square, Inc.

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
	"fmt"
	"time"

	"github.com/square/etre"
	"github.com/square/etre/cdc"
	"github.com/square/etre/db"
	"github.com/square/etre/query"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/rs/xid"
)

type DbError struct {
	Err      error
	Type     string
	EntityId string
}

func (e DbError) Error() string {
	return e.Err.Error()
}

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
	DeleteLabel(WriteOp, string) (etre.Entity, error)

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

// cdcPartial represents part of a full etre.CDCEvent. It's passed to cdcWrite
// which makes a complete CDCEvent from the partial and a WriteOp.
type cdcPartial struct {
	op  string
	id  bson.ObjectId
	old *etre.Entity
	new *etre.Entity
	rev uint
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
func NewStore(conn db.Connector, database string, entityTypes []string, cdcs cdc.Store, dm cdc.Delayer) Store {
	return &store{
		conn:        conn,
		database:    database,
		entityTypes: entityTypes,
		cdcs:        cdcs,
		dm:          dm,
	}
}

// DeleteLabel deletes a label from an entity.
//
// Relevant documentation:
//
//     https://docs.mongodb.com/manual/reference/operator/update/unset/#up._S_unset
//
func (s *store) DeleteLabel(wo WriteOp, label string) (etre.Entity, error) {
	// Connect to Mongo collection for the entity type
	ms, err := s.conn.Connect()
	if err != nil {
		return nil, DbError{Err: err, Type: "db-connect"}
	}
	defer ms.Close()
	c := ms.DB(s.database).C(wo.EntityType)

	change := mgo.Change{
		Update: bson.M{
			"$unset": bson.M{label: ""}, // removes label, Mongo expects "" (see $unset docs)
			"$inc": bson.M{
				"_rev": 1, // increment the revision
			},
		},
		ReturnNew: false, // return doc before update
	}

	// We call Select so that Apply will return the orginal deleted label (and
	// "_id" field, which is included by default) rather than returning the
	// entire original document
	id := bson.ObjectIdHex(wo.EntityId)
	affectedLabels := selectMap(etre.Entity{label: ""})
	old := etre.Entity{}
	_, err = c.Find(bson.M{"_id": id}).Select(affectedLabels).Apply(change, &old)
	if err != nil {
		switch err {
		case mgo.ErrNotFound:
			return nil, etre.ErrEntityNotFound
		default:
			return nil, DbError{Err: err, Type: "db-find-apply", EntityId: wo.EntityId}
		}
	}

	newRev := old["_rev"].(int) + 1 // +1 since we get the old document back
	new := etre.Entity{
		"_id":   id,
		"_type": wo.EntityType,
		"_rev":  newRev,
	}
	cp := cdcPartial{
		op:  "u",
		id:  id,
		old: &old,
		new: &new,
		rev: uint(newRev),
	}
	if err := s.cdcWrite(etre.Entity{}, wo, cp); err != nil {
		return old, err
	}

	return old, nil
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
	// Connect to Mongo collection for the entity type
	ms, err := s.conn.Connect()
	if err != nil {
		return nil, DbError{Err: err, Type: "db-connect"}
	}
	defer ms.Close()
	c := ms.DB(s.database).C(wo.EntityType)

	// Notify the delay manager that a change is starting, and then notify the
	// delay manager again when the change is done.
	changeId := xid.New().String()
	err = s.dm.BeginChange(changeId)
	if err != nil {
		return nil, DbError{Err: err, Type: "cdc-begin"}
	}
	// @todo: don't ignore error...this should report an exception or something
	defer s.dm.EndChange(changeId)

	// A slice of IDs we generate to insert along with entities into DB
	insertedObjectIds := make([]string, 0, len(entities))

	for _, e := range entities {
		// Mgo driver does not return the ObjectId that Mongo creates, so create it ourself.
		id := bson.NewObjectId()
		idStr := hex.EncodeToString([]byte(id)) // id as string
		e["_id"] = id
		e["_type"] = wo.EntityType
		e["_rev"] = 0

		if err := c.Insert(e); err != nil {
			if mgo.IsDup(err) {
				return insertedObjectIds, DbError{Err: err, Type: "duplicate-entity", EntityId: idStr}
			}
			return insertedObjectIds, DbError{Err: err, Type: "db-insert", EntityId: idStr}
		}

		// bson.ObjectId.String() yields "ObjectId("abc")", but we need to report only "abc",
		// so re-encode the raw bytes to a hex string. This make GET /entity/{t}/abc work.
		insertedObjectIds = append(insertedObjectIds, idStr)

		// Create a CDC event.
		cp := cdcPartial{
			op:  "i",
			id:  id,
			new: &e,
			old: nil,
			rev: 0,
		}
		if err := s.cdcWrite(e, wo, cp); err != nil {
			return insertedObjectIds, err
		}
	}

	return insertedObjectIds, nil
}

// ReadEntities queries the db and returns a slice of Entity objects if
// something is found, a nil slice if nothing is found, and an error if one
// occurs.
func (s *store) ReadEntities(entityType string, q query.Query, f etre.QueryFilter) ([]etre.Entity, error) {
	// Connect to Mongo collection for the entity type
	ms, err := s.conn.Connect()
	if err != nil {
		return nil, DbError{Err: err, Type: "db-connect"}
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
		return nil, DbError{Err: err, Type: "db-find"}
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
	// Connect to Mongo collection for the entity type
	ms, err := s.conn.Connect()
	if err != nil {
		return nil, DbError{Err: err, Type: "db-connect"}
	}
	defer ms.Close()
	c := ms.DB(s.database).C(wo.EntityType)

	// We can only call update on one doc at a time, so we generate a slice of IDs
	// for docs to update.
	mgoQuery := translateQuery(q)
	ids, err := idsForQuery(c, mgoQuery)
	if err != nil {
		return nil, DbError{Err: err, Type: "db-find-ids"}
	}

	// Change to make
	change := mgo.Change{
		Update: bson.M{
			"$set": patch,
			"$inc": bson.M{
				"_rev": 1, // increment the revision
			},
		},
		ReturnNew: false, // return doc before update
	}

	// Notify the delay manager that a change is starting, and then notify the
	// delay manager again when the change is done.
	changeId := xid.New().String()
	err = s.dm.BeginChange(changeId)
	if err != nil {
		return nil, DbError{Err: err, Type: "cdc-begin"}
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
			idStr := hex.EncodeToString([]byte(id)) // id as string
			if mgo.IsDup(err) {
				return diffs, DbError{Err: err, Type: "duplicate-entity", EntityId: idStr}
			}
			return diffs, DbError{Err: err, Type: "db-find-apply", EntityId: idStr}
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
		cp := cdcPartial{
			op:  "u",
			id:  id,
			old: &diff,
			new: &new,
			rev: uint(newRev),
		}
		if err := s.cdcWrite(patch, wo, cp); err != nil {
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
	// Connect to Mongo collection for the entity type
	ms, err := s.conn.Connect()
	if err != nil {
		return nil, DbError{Err: err, Type: "db-connect"}
	}
	defer ms.Close()
	c := ms.DB(s.database).C(wo.EntityType)

	// List of IDs for docs to update
	mgoQuery := translateQuery(q)
	ids, err := idsForQuery(c, mgoQuery)
	if err != nil {
		return nil, DbError{Err: err, Type: "db-find-ids"}
	}

	// Change to make
	change := mgo.Change{
		Remove:    true,
		ReturnNew: false, // return doc before delete
	}

	// deletedEntities is a slice of entities that have been successfully deleted
	deletedEntities := make([]etre.Entity, 0, len(ids))

	// Notify the delay manager that a change is starting, and then notify the
	// delay manager again when the change is done.
	changeId := xid.New().String()
	err = s.dm.BeginChange(changeId)
	if err != nil {
		return nil, DbError{Err: err, Type: "cdc-begin"}
	}
	// @todo: don't ignore error...this should report an exception or something
	defer func() { s.dm.EndChange(changeId) }()

	// Query for each document and delete it
	for _, id := range ids {
		var deletedEntity etre.Entity
		if _, err := c.FindId(id).Apply(change, &deletedEntity); err != nil {
			idStr := hex.EncodeToString([]byte(id)) // id as string
			switch err {
			case mgo.ErrNotFound:
				// ignore
			default:
				return deletedEntities, DbError{Err: err, Type: "db-find-apply", EntityId: idStr}
			}
		}

		deletedEntities = append(deletedEntities, deletedEntity)

		// Create a CDC event.
		ce := cdcPartial{
			op:  "d",
			id:  id,
			old: &deletedEntity,
			new: nil,
			rev: uint(deletedEntity["_rev"].(int)) + 1, // +1 since we get the old document back
		}
		if err := s.cdcWrite(deletedEntity, wo, ce); err != nil {
			return deletedEntities, err
		}
	}

	return deletedEntities, nil
}

// //////////////////////////////////////////////////////////////////////////
// Private funcs
// //////////////////////////////////////////////////////////////////////////

func (s *store) cdcWrite(e etre.Entity, wo WriteOp, cp cdcPartial) error {
	ts := time.Now().UnixNano() / int64(time.Millisecond)
	// set op from entity or wo, in that order.
	set := e.Set()
	if set.Size == 0 && wo.SetSize > 0 {
		set.Op = wo.SetOp
		set.Id = wo.SetId
		set.Size = wo.SetSize
	}
	idStr := hex.EncodeToString([]byte(cp.id)) // id as string
	event := etre.CDCEvent{
		EventId:    hex.EncodeToString([]byte(bson.NewObjectId())),
		EntityId:   idStr,
		EntityType: wo.EntityType,
		Rev:        cp.rev,
		Ts:         ts,
		User:       wo.User,
		Op:         cp.op,
		Old:        cp.old,
		New:        cp.new,
		SetId:      set.Id,
		SetOp:      set.Op,
		SetSize:    set.Size,
	}
	if err := s.cdcs.Write(event); err != nil {
		return DbError{Err: err, Type: "cdc-write", EntityId: idStr}
	}
	return nil
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
		return nil, err // return raw error, let caller wrap
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
