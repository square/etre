// Copyright 2017-2020, Square, Inc.

// Package entity is a connector to execute CRUD commands for a single entity and
// many entities on a DB instance.
package entity

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/square/etre"
	"github.com/square/etre/cdc"
	"github.com/square/etre/query"
)

// Store interface has methods needed to do CRUD operations on entities.
type Store interface {
	WithContext(context.Context) Store

	ReadEntities(string, query.Query, etre.QueryFilter) ([]etre.Entity, error)

	CreateEntities(WriteOp, []etre.Entity) ([]string, error)

	UpdateEntities(WriteOp, query.Query, etre.Entity) ([]etre.Entity, error)

	DeleteEntities(WriteOp, query.Query) ([]etre.Entity, error)

	DeleteLabel(WriteOp, string) (etre.Entity, error)
}

type store struct {
	coll map[string]*mongo.Collection
	cdcs cdc.Store
	ctx  context.Context
}

// NewStore creates a Store.
func NewStore(entities map[string]*mongo.Collection, cdcStore cdc.Store) store {
	return store{
		coll: entities,
		cdcs: cdcStore,
		ctx:  context.Background(),
	}
}

func (s store) WithContext(ctx context.Context) Store {
	s.ctx = ctx
	return s
}

// ReadEntities queries the db and returns a slice of Entity objects if
// something is found, a nil slice if nothing is found, and an error if one
// occurs.
func (s store) ReadEntities(entityType string, q query.Query, f etre.QueryFilter) ([]etre.Entity, error) {
	c, ok := s.coll[entityType]
	if !ok {
		panic("invalid entity type passed to ReadEntities: " + entityType)
	}

	// Distinct optimizaiton: unique values for the one return label. For example,
	// "es -u node.metacluster zone=pd" returns a list of unique metacluster names.
	// This is 10x faster than "es node.metacluster zone=pd | sort -u".
	if len(f.ReturnLabels) == 1 && f.Distinct {
		values, err := c.Distinct(s.ctx, f.ReturnLabels[0], Filter(q))
		if err != nil {
			return nil, DbError{Err: err, Type: "db-read-distinct"}
		}
		entities := make([]etre.Entity, len(values))
		for i, v := range values {
			entities[i] = etre.Entity{f.ReturnLabels[0]: v}
		}
		return entities, nil
	}

	// Find and return all matching entities
	p := bson.M{}
	if len(f.ReturnLabels) > 0 {
		for _, label := range f.ReturnLabels {
			p[label] = 1
		}
		p["_id"] = 0
	}

	opts := options.Find().SetProjection(p)
	cursor, err := c.Find(s.ctx, Filter(q), opts)
	if err != nil {
		if ctxErr := s.ctx.Err(); ctxErr != nil {
			return nil, DbError{Err: ctxErr, Type: "db-read-query"}
		}
		return nil, DbError{Err: err, Type: "db-read-query"}
	}
	entities := []etre.Entity{}
	if err := cursor.All(s.ctx, &entities); err != nil {
		return nil, DbError{Err: err, Type: "db-read-cursor"}
	}
	return entities, nil
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
func (s store) CreateEntities(wo WriteOp, entities []etre.Entity) ([]string, error) {
	c, ok := s.coll[wo.EntityType]
	if !ok {
		panic("invalid entity type passed to CreateEntities: " + wo.EntityType)
	}

	// A slice of IDs we generate to insert along with entities into DB
	newIds := make([]string, 0, len(entities))

	for i := range entities {
		entities[i]["_id"] = primitive.NewObjectID()
		entities[i]["_type"] = wo.EntityType
		entities[i]["_rev"] = int64(0)

		res, err := c.InsertOne(s.ctx, entities[i])
		if err != nil {
			if dupe := IsDupeKeyError(err); dupe != nil {
				return newIds, DbError{Err: dupe, Type: "duplicate-entity"}
			}
			return newIds, DbError{Err: err, Type: "db-insert"}
		}
		id := res.InsertedID.(primitive.ObjectID)
		newIds = append(newIds, id.Hex())

		// Create a CDC event.
		cp := cdcPartial{
			op:  "i",
			id:  id,
			new: &entities[i],
			old: nil,
			rev: int64(0),
		}
		if err := s.cdcWrite(entities[i], wo, cp); err != nil {
			return newIds, err
		}
	}

	return newIds, nil
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
func (s store) UpdateEntities(wo WriteOp, q query.Query, patch etre.Entity) ([]etre.Entity, error) {
	c, ok := s.coll[wo.EntityType]
	if !ok {
		panic("invalid entity type passed to UpdateEntities: " + wo.EntityType)
	}

	fopts := options.Find().SetProjection(bson.M{"_id": 1})
	cursor, err := c.Find(s.ctx, Filter(q), fopts)
	if err != nil {
		return nil, DbError{Err: err, Type: "db-read-query"}
	}
	defer cursor.Close(s.ctx)

	// diffs is a slice made up of a diff for each doc updated
	diffs := []etre.Entity{}

	updates := bson.M{
		"$set": patch,
		"$inc": bson.M{
			"_rev": 1, // increment the revision
		},
	}

	p := bson.M{"_id": 1, "_type": 1, "_rev": 1}
	for label := range patch {
		p[label] = 1
	}
	opts := options.FindOneAndUpdate().SetProjection(p)

	nextId := map[string]primitive.ObjectID{}
	for cursor.Next(s.ctx) {
		if err := cursor.Decode(&nextId); err != nil {
			return nil, err
		}
		uq, _ := query.Translate("_id=" + nextId["_id"].Hex())

		var orig etre.Entity
		err := c.FindOneAndUpdate(s.ctx, Filter(uq), updates, opts).Decode(&orig)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				break
			}
			if dupe := IsDupeKeyError(err); dupe != nil {
				return diffs, DbError{Err: dupe, Type: "duplicate-entity"}
			}
			return diffs, DbError{Err: err, Type: "db-update"}
		}
		diffs = append(diffs, orig)

		old := etre.Entity{}
		for k, v := range orig {
			if k == "_id" || k == "_type" || k == "_rev" {
				continue
			}
			old[k] = v
		}

		cp := cdcPartial{
			op:  "u",
			id:  orig["_id"].(primitive.ObjectID),
			rev: orig["_rev"].(int64) + 1,
			old: &old,
			new: &patch,
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
func (s store) DeleteEntities(wo WriteOp, q query.Query) ([]etre.Entity, error) {
	c, ok := s.coll[wo.EntityType]
	if !ok {
		panic("invalid entity type passed to DeleteEntities: " + wo.EntityType)
	}

	deleted := []etre.Entity{}
	for {
		var old etre.Entity
		err := c.FindOneAndDelete(s.ctx, Filter(q)).Decode(&old)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				break
			}
			return deleted, DbError{Err: err, Type: "db-delete"}
		}
		deleted = append(deleted, old)
		ce := cdcPartial{
			op:  "d",
			id:  old["_id"].(primitive.ObjectID),
			old: &old,
			new: nil,
			rev: old["_rev"].(int64) + 1, // because we have old rev
		}
		if err := s.cdcWrite(old, wo, ce); err != nil {
			return deleted, err
		}
	}

	return deleted, nil
}

// DeleteLabel deletes a label from an entity.
func (s store) DeleteLabel(wo WriteOp, label string) (etre.Entity, error) {
	c, ok := s.coll[wo.EntityType]
	if !ok {
		panic("invalid entity type passed to DeleteLabel: " + wo.EntityType)
	}

	id, _ := primitive.ObjectIDFromHex(wo.EntityId)
	filter := bson.M{"_id": id}
	update := bson.M{
		"$unset": bson.M{label: ""}, // removes label, Mongo expects "" (see $unset docs)
		"$inc":   bson.M{"_rev": 1}, // increment the revision
	}
	opts := options.FindOneAndUpdate().
		SetProjection(bson.M{"_id": 1, "_type": 1, "_rev": 1, label: 1}).
		SetReturnDocument(options.Before)
	var old etre.Entity
	err := c.FindOneAndUpdate(s.ctx, filter, update, opts).Decode(&old)
	if err != nil {
		switch err {
		case mongo.ErrNoDocuments:
			return nil, etre.ErrEntityNotFound
		default:
			return nil, DbError{Err: err, Type: "db-deletel-label", EntityId: wo.EntityId}
		}
	}
	cp := cdcPartial{
		op:  "u",
		id:  old["_id"].(primitive.ObjectID),
		new: nil, // not on delete label
		old: &old,
		rev: old["_rev"].(int64) + 1, // because we have old rev
	}
	if err := s.cdcWrite(etre.Entity{}, wo, cp); err != nil {
		return old, err
	}

	return old, nil
}

// --------------------------------------------------------------------------
// CDC write
// --------------------------------------------------------------------------

// cdcPartial represents part of a full etre.CDCEvent. It's passed to cdcWrite
// which makes a complete CDCEvent from the partial and a WriteOp.
type cdcPartial struct {
	op  string
	id  primitive.ObjectID
	old *etre.Entity
	new *etre.Entity
	rev int64
}

func (s store) cdcWrite(e etre.Entity, wo WriteOp, cp cdcPartial) error {
	// set op from entity or wo, in that order.
	set := e.Set()
	if set.Size == 0 && wo.SetSize > 0 {
		set.Op = wo.SetOp
		set.Id = wo.SetId
		set.Size = wo.SetSize
	}
	event := etre.CDCEvent{
		Ts:     time.Now().UnixNano() / int64(time.Millisecond),
		Op:     cp.op,
		Caller: wo.Caller,

		EntityId:   cp.id.Hex(),
		EntityType: wo.EntityType,
		EntityRev:  cp.rev,
		Old:        cp.old,
		New:        cp.new,

		SetId:   set.Id,
		SetOp:   set.Op,
		SetSize: set.Size,
	}
	if err := s.cdcs.Write(s.ctx, event); err != nil {
		return DbError{Err: err, Type: "cdc-write", EntityId: cp.id.Hex()}
	}
	return nil
}
