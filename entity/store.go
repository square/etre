// Copyright 2017-2020, Square, Inc.

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
	"github.com/square/etre/config"
	"github.com/square/etre/query"
)

// Store interface has methods needed to do CRUD operations on entities.
type Store interface {
	ReadEntities(context.Context, string, query.Query, etre.QueryFilter) ([]etre.Entity, error)

	CreateEntities(context.Context, WriteOp, []etre.Entity) ([]string, error)

	UpdateEntities(context.Context, WriteOp, query.Query, etre.Entity) ([]etre.Entity, error)

	DeleteEntities(context.Context, WriteOp, query.Query) ([]etre.Entity, error)

	DeleteLabel(context.Context, WriteOp, string) (etre.Entity, error)
}

type store struct {
	coll   map[string]*mongo.Collection
	cdcs   cdc.Store
	config config.EntityConfig
}

// NewStore creates a Store.
func NewStore(entities map[string]*mongo.Collection, cdcStore cdc.Store, cfg config.EntityConfig) store {
	return store{
		coll:   entities,
		cdcs:   cdcStore,
		config: cfg,
	}
}

// ReadEntities queries the db and returns a slice of Entity objects if
// something is found, a nil slice if nothing is found, and an error if one
// occurs.
func (s store) ReadEntities(ctx context.Context, entityType string, q query.Query, f etre.QueryFilter) ([]etre.Entity, error) {
	c, ok := s.coll[entityType]
	if !ok {
		panic("invalid entity type passed to ReadEntities: " + entityType)
	}

	// Distinct optimizaiton: unique values for the one return label. For example,
	// "es -u node.metacluster zone=pd" returns a list of unique metacluster names.
	// This is 10x faster than "es node.metacluster zone=pd | sort -u".
	if len(f.ReturnLabels) == 1 && f.Distinct {
		values, err := c.Distinct(ctx, f.ReturnLabels[0], Filter(q))
		if err != nil {
			return nil, s.dbError(ctx, err, "db-read-distinct")
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
		// Only include _id if explicitly set in f.ReturnLabels. If not,
		// ok is false and we must explicitly exlude it because MongoDB
		// returns it by default.
		if _, ok := p["_id"]; !ok {
			p["_id"] = 0
		}
	}

	// Set batch size and projection
	opts := options.Find().SetProjection(p).SetBatchSize(int32(s.config.BatchSize))
	cursor, err := c.Find(ctx, Filter(q), opts)
	if err != nil {
		return nil, s.dbError(ctx, err, "db-query")
	}
	entities := []etre.Entity{}
	if err := cursor.All(ctx, &entities); err != nil {
		return nil, s.dbError(ctx, err, "db-read-cursor")
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
func (s store) CreateEntities(ctx context.Context, wo WriteOp, entities []etre.Entity) ([]string, error) {
	c, ok := s.coll[wo.EntityType]
	if !ok {
		panic("invalid entity type passed to CreateEntities: " + wo.EntityType)
	}

	// A slice of IDs we generate to insert along with entities into DB
	newIds := make([]string, 0, len(entities))

	now := time.Now().UnixNano()
	for i := range entities {
		entities[i]["_id"] = primitive.NewObjectID()
		entities[i]["_type"] = wo.EntityType
		entities[i]["_rev"] = int64(0)
		entities[i]["_created"] = now
		entities[i]["_updated"] = now

		res, err := c.InsertOne(ctx, entities[i])
		if err != nil {
			return newIds, s.dbError(ctx, err, "db-insert")
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
		if err := s.cdcWrite(ctx, entities[i], wo, cp); err != nil {
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
//	q, _ := query.Translate("y=foo")
//	update := db.Entity{"y": "bar"}
//
//	diffs, err := c.UpdateEntities(q, update)
func (s store) UpdateEntities(ctx context.Context, wo WriteOp, q query.Query, patch etre.Entity) ([]etre.Entity, error) {
	c, ok := s.coll[wo.EntityType]
	if !ok {
		panic("invalid entity type passed to UpdateEntities: " + wo.EntityType)
	}

	fopts := options.Find().SetProjection(bson.M{"_id": 1})
	cursor, err := c.Find(ctx, Filter(q), fopts)
	if err != nil {
		return nil, s.dbError(ctx, err, "db-query")
	}
	defer cursor.Close(ctx)

	// diffs is a slice made up of a diff for each doc updated
	diffs := []etre.Entity{}

	patch["_updated"] = time.Now().UnixNano()
	updates := bson.M{
		"$set": patch,
		"$inc": bson.M{
			"_rev": 1, // increment the revision
		},
	}

	p := bson.M{"_id": 1, "_type": 1, "_rev": 1, "_updated": 1}
	for label := range patch {
		p[label] = 1
	}
	opts := options.FindOneAndUpdate().SetProjection(p)

	nextId := map[string]primitive.ObjectID{}
	for cursor.Next(ctx) {
		if err := cursor.Decode(&nextId); err != nil {
			return diffs, s.dbError(ctx, err, "db-cursor-decode")
		}
		uq, _ := query.Translate("_id=" + nextId["_id"].Hex())

		var orig etre.Entity
		err := c.FindOneAndUpdate(ctx, Filter(uq), updates, opts).Decode(&orig)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				break
			}
			return diffs, s.dbError(ctx, err, "db-update")
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
			rev: orig.Rev() + 1,
			old: &old,
			new: &patch,
		}
		if err := s.cdcWrite(ctx, patch, wo, cp); err != nil {
			return diffs, err
		}
	}

	if err := cursor.Err(); err != nil {
		return diffs, s.dbError(ctx, err, "db-cursor-next")
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
func (s store) DeleteEntities(ctx context.Context, wo WriteOp, q query.Query) ([]etre.Entity, error) {
	c, ok := s.coll[wo.EntityType]
	if !ok {
		panic("invalid entity type passed to DeleteEntities: " + wo.EntityType)
	}

	deleted := []etre.Entity{}
	for {
		var old etre.Entity
		err := c.FindOneAndDelete(ctx, Filter(q)).Decode(&old)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				break
			}
			return deleted, s.dbError(ctx, err, "db-delete")
		}
		deleted = append(deleted, old)
		ce := cdcPartial{
			op:  "d",
			id:  old["_id"].(primitive.ObjectID),
			old: &old,
			new: nil,
			rev: old.Rev() + 1,
		}
		if err := s.cdcWrite(ctx, old, wo, ce); err != nil {
			return deleted, err
		}
	}

	return deleted, nil
}

// DeleteLabel deletes a label from an entity.
func (s store) DeleteLabel(ctx context.Context, wo WriteOp, label string) (etre.Entity, error) {
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
	err := c.FindOneAndUpdate(ctx, filter, update, opts).Decode(&old)
	if err != nil {
		return nil, s.dbError(ctx, err, "db-update")
	}

	// Make the new Entity by copying the old and deleting the label
	new := etre.Entity{}
	for k, v := range old {
		new[k] = v
	}
	delete(new, label)
	// We need to increment "_rev" by one, but the type needs to match
	// what it was on "old"
	switch old["_rev"].(type) {
	case int:
		new["_rev"] = old["_rev"].(int) + 1
	case int32:
		new["_rev"] = old["_rev"].(int32) + 1
	case int64:
		new["_rev"] = old["_rev"].(int64) + 1
	default:
		new["_rev"] = old.Rev() + 1
	}

	cp := cdcPartial{
		op:  "u",
		id:  old["_id"].(primitive.ObjectID),
		new: &new,
		old: &old,
		rev: old.Rev() + 1,
	}
	if err := s.cdcWrite(ctx, etre.Entity{}, wo, cp); err != nil {
		return old, err
	}

	return old, nil
}

func (s store) dbError(ctx context.Context, err error, errType string) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return DbError{Err: ctxErr, Type: errType}
	}
	if dupe := IsDupeKeyError(err); dupe != nil {
		return DbError{Err: dupe, Type: "duplicate-entity"}
	}
	if err == mongo.ErrNoDocuments {
		return etre.ErrEntityNotFound
	}
	return DbError{Err: err, Type: errType}
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

func (s store) cdcWrite(ctx context.Context, e etre.Entity, wo WriteOp, cp cdcPartial) error {
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
	if err := s.cdcs.Write(ctx, event); err != nil {
		return DbError{Err: err, Type: "cdc-write", EntityId: cp.id.Hex()}
	}
	return nil
}
