// Copyright 2020, Square, Inc.

package entity_test

import (
	"context"
	"testing"

	"github.com/go-test/deep"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/square/etre"
	"github.com/square/etre/entity"
	"github.com/square/etre/query"
	"github.com/square/etre/test"
	"github.com/square/etre/test/mock"
)

var v09testNodes []etre.Entity
var v09testNodes_int32 []etre.Entity

func setupV09(t *testing.T, cdcm *mock.CDCStore) entity.Store {
	if coll == nil {
		var err error
		client, coll, err = test.DbCollections(entityTypes)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Reset the collection: delete all entities and insert the standard test entities
	nodesColl := coll[entityType]
	_, err := nodesColl.DeleteMany(context.TODO(), bson.D{{}})
	if err != nil {
		t.Fatal(err)
	}

	// First time, create unique index on "x"
	if coll == nil {
		iv := nodesColl.Indexes()
		if _, err := iv.DropAll(context.TODO()); err != nil {
			t.Fatal(err)
		}
		idx := mongo.IndexModel{
			Keys:    bson.D{{"x", 1}},
			Options: options.Index().SetUnique(true),
		}
		if _, err := iv.CreateOne(context.TODO(), idx); err != nil {
			t.Fatal(err)
		}
	}

	v09testNodes = []etre.Entity{
		etre.Entity{"_type": entityType, "_rev": int(0), "x": "a", "y": "a"},
		etre.Entity{"_type": entityType, "_rev": int(0), "x": "b", "y": "a"},
		etre.Entity{"_type": entityType, "_rev": int(0), "x": "c", "y": "a"},
	}
	v09testNodes_int32 = []etre.Entity{
		etre.Entity{"_type": entityType, "_rev": int32(0), "x": "a", "y": "a"},
		etre.Entity{"_type": entityType, "_rev": int32(0), "x": "b", "y": "a"},
		etre.Entity{"_type": entityType, "_rev": int32(0), "x": "c", "y": "a"},
	}
	res, err := nodesColl.InsertMany(context.TODO(), docs(v09testNodes))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.InsertedIDs) != len(v09testNodes) {
		t.Fatalf("mongo-driver returned %d doc ids, expected %d", len(res.InsertedIDs), len(v09testNodes))
	}
	for i, id := range res.InsertedIDs {
		v09testNodes[i]["_id"] = id.(primitive.ObjectID)
		v09testNodes_int32[i]["_id"] = id.(primitive.ObjectID)
	}

	return entity.NewStore(coll, cdcm)
}

// --------------------------------------------------------------------------

func TestV09CreateEntitiesMultiple(t *testing.T) {
	gotEvents := []etre.CDCEvent{}
	cdcm := &mock.CDCStore{
		WriteFunc: func(ctx context.Context, e etre.CDCEvent) error {
			gotEvents = append(gotEvents, e)
			return nil
		},
	}
	store := setupV09(t, cdcm)

	testData := []etre.Entity{
		etre.Entity{"x": "d"},
		etre.Entity{"x": "e"},
		etre.Entity{"x": "f", "_setId": "343", "_setOp": "something", "_setSize": 1},
	}
	ids, err := store.CreateEntities(wo, testData)
	if err != nil {
		t.Error(err)
	}

	if len(ids) != len(testData) {
		t.Errorf("got %d ids, expected %d", len(ids), len(testData))
	}

	// Verify that the last CDC event we create is as expected.
	id1, _ := primitive.ObjectIDFromHex(ids[0])
	id2, _ := primitive.ObjectIDFromHex(ids[1])
	id3, _ := primitive.ObjectIDFromHex(ids[2])
	expectEvents := []etre.CDCEvent{
		{
			Id:         gotEvents[0].Id, // non-deterministic
			EntityId:   id1.Hex(),
			EntityType: entityType,
			EntityRev:  int64(0),
			Ts:         gotEvents[0].Ts, // non-deterministic
			Caller:     username,
			Op:         "i",
			Old:        nil,
			New:        &etre.Entity{"_id": id1, "_type": entityType, "_rev": int64(0), "x": "d"},
		},
		{
			Id:         gotEvents[1].Id, // non-deterministic
			EntityId:   id2.Hex(),
			EntityType: entityType,
			EntityRev:  int64(0),
			Ts:         gotEvents[1].Ts, // non-deterministic
			Caller:     username,
			Op:         "i",
			Old:        nil,
			New:        &etre.Entity{"_id": id2, "_type": entityType, "_rev": int64(0), "x": "e"},
		},
		{
			Id:         gotEvents[2].Id, // non-deterministic
			EntityId:   id3.Hex(),
			EntityType: entityType,
			EntityRev:  int64(0),
			Ts:         gotEvents[2].Ts, // non-deterministic
			Caller:     username,
			Op:         "i",
			Old:        nil,
			New:        &etre.Entity{"_id": id3, "_type": entityType, "_rev": int64(0), "x": "f", "_setId": "343", "_setOp": "something", "_setSize": 1},
			SetId:      "343",
			SetOp:      "something",
			SetSize:    1,
		},
	}
	if diff := deep.Equal(gotEvents, expectEvents); diff != nil {
		t.Error(diff)
	}
}

func TestV09UpdateEntities(t *testing.T) {
	gotEvents := []etre.CDCEvent{}
	cdcm := &mock.CDCStore{
		WriteFunc: func(ctx context.Context, e etre.CDCEvent) error {
			gotEvents = append(gotEvents, e)
			return nil
		},
	}
	store := setupV09(t, cdcm)

	// This matches first test node
	q, err := query.Translate("x=a")
	if err != nil {
		t.Error(err)
	}
	patch := etre.Entity{"y": "y"} // y=a -> y=y
	wo1 := entity.WriteOp{
		EntityType: entityType,
		Caller:     username,
		SetOp:      "update-y1",
		SetId:      "111",
		SetSize:    1,
	}
	gotDiffs, err := store.UpdateEntities(wo1, q, patch)
	if err != nil {
		t.Fatal(err)
	}
	if len(gotDiffs) != 1 {
		t.Errorf("got %d diffs, expected 1", len(gotDiffs))
	}
	expectDiffs := []etre.Entity{
		{
			"_id":   v09testNodes[0]["_id"],
			"_type": entityType,
			"_rev":  int32(0),
			"y":     "a",
		},
	}
	if diff := deep.Equal(gotDiffs, expectDiffs); diff != nil {
		t.Logf("got: %+v", gotDiffs)
		t.Error(diff)
	}

	for i := range gotEvents {
		gotEvents[i].Id = ""
		gotEvents[i].Ts = 0
	}
	id1, _ := v09testNodes[0]["_id"].(primitive.ObjectID)
	expectEvent := []etre.CDCEvent{
		{
			EntityId:   id1.Hex(),
			EntityType: entityType,
			EntityRev:  int64(1),
			Caller:     username,
			Op:         "u",
			Old:        &etre.Entity{"y": "a"},
			New:        &etre.Entity{"y": "y"},
			SetId:      "111",
			SetOp:      "update-y1",
			SetSize:    1,
		},
	}
	if diff := deep.Equal(gotEvents, expectEvent); diff != nil {
		t.Error(diff)
	}
}

func TestV09DeleteEntities(t *testing.T) {
	gotEvents := []etre.CDCEvent{}
	cdcm := &mock.CDCStore{
		WriteFunc: func(ctx context.Context, e etre.CDCEvent) error {
			gotEvents = append(gotEvents, e)
			return nil
		},
	}
	store := setupV09(t, cdcm)

	// Match one first test node
	q, err := query.Translate("x == a")
	if err != nil {
		t.Error(err)
	}
	gotOld, err := store.DeleteEntities(wo, q)
	if err != nil {
		t.Error(err)
	}
	if diff := deep.Equal(gotOld, v09testNodes_int32[:1]); diff != nil {
		t.Error(diff)
	}

	// Match last two test nodes
	q, err = query.Translate("x in (b,c)")
	if err != nil {
		t.Error(err)
	}
	gotOld, err = store.DeleteEntities(wo, q)
	if err != nil {
		t.Error(err)
	}
	if diff := deep.Equal(gotOld, v09testNodes_int32[1:]); diff != nil {
		t.Error(diff)
	}

	for i := range gotEvents {
		gotEvents[i].Id = ""
		gotEvents[i].Ts = 0
	}
	id1, _ := v09testNodes[0]["_id"].(primitive.ObjectID)
	id2, _ := v09testNodes[1]["_id"].(primitive.ObjectID)
	id3, _ := v09testNodes[2]["_id"].(primitive.ObjectID)
	expectEvent := []etre.CDCEvent{
		{
			EntityId:   id1.Hex(),
			EntityType: entityType,
			EntityRev:  int64(1),
			Caller:     username,
			Op:         "d",
			Old:        &v09testNodes_int32[0],
		},
		{
			EntityId:   id2.Hex(),
			EntityType: entityType,
			EntityRev:  int64(1),
			Caller:     username,
			Op:         "d",
			Old:        &v09testNodes_int32[1],
		},
		{
			EntityId:   id3.Hex(),
			EntityType: entityType,
			EntityRev:  int64(1),
			Caller:     username,
			Op:         "d",
			Old:        &v09testNodes_int32[2],
		},
	}
	if diff := deep.Equal(gotEvents, expectEvent); diff != nil {
		t.Error(diff)
	}
}

func TestV09DeleteLabel(t *testing.T) {
	gotEvents := []etre.CDCEvent{}
	cdcm := &mock.CDCStore{
		WriteFunc: func(ctx context.Context, e etre.CDCEvent) error {
			gotEvents = append(gotEvents, e)
			return nil
		},
	}
	store := setupV09(t, cdcm)

	wo := entity.WriteOp{
		EntityType: entityType,
		EntityId:   v09testNodes[0]["_id"].(primitive.ObjectID).Hex(),
		Caller:     username,
	}
	gotOld, err := store.DeleteLabel(wo, "y")
	if err != nil {
		t.Error(err)
	}
	expectOld := etre.Entity{
		"_id":   v09testNodes[0]["_id"],
		"_type": v09testNodes[0]["_type"],
		"_rev":  v09testNodes_int32[0]["_rev"],
		"y":     "a",
	}
	if diff := deep.Equal(gotOld, expectOld); diff != nil {
		t.Error(diff)
	}

	// The foo label should no longer be set on the entity
	q, _ := query.Translate("x=a")
	gotNew, err := store.ReadEntities(entityType, q, etre.QueryFilter{})
	if err != nil {
		t.Error(err)
	}
	e := etre.Entity{}
	for k, v := range v09testNodes[0] {
		e[k] = v
	}
	delete(e, "y")       // because we deleted the label
	e["_rev"] = int32(1) // because we deleted the label
	expectNew := []etre.Entity{e}
	if diff := deep.Equal(gotNew, expectNew); diff != nil {
		t.Logf("got: %+v", gotNew)
		t.Error(diff)
	}

	for i := range gotEvents {
		gotEvents[i].Id = ""
		gotEvents[i].Ts = 0
	}
	id1, _ := v09testNodes[0]["_id"].(primitive.ObjectID)
	expectedEventNew := etre.Entity{
		"_id":   v09testNodes[0]["_id"],
		"_type": v09testNodes[0]["_type"],
		"_rev":  e["_rev"],
	}
	expectEvent := []etre.CDCEvent{
		{
			EntityId:   id1.Hex(),
			EntityType: entityType,
			EntityRev:  int64(1),
			Caller:     username,
			Op:         "u",
			Old:        &expectOld,
			New:        &expectedEventNew,
		},
	}
	if diff := deep.Equal(gotEvents, expectEvent); diff != nil {
		t.Error(diff)
	}
}
