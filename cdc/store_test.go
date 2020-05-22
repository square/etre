// Copyright 2017-2020, Square, Inc.

package cdc_test

import (
	"context"
	"sort"
	"testing"

	"github.com/go-test/deep"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/square/etre"
	"github.com/square/etre/cdc"
	"github.com/square/etre/test"
	"github.com/square/etre/test/mock"
)

var (
	client    *mongo.Client
	coll      map[string]*mongo.Collection
	testNodes []etre.Entity

	username    = "test_user"
	entityType  = "cdc"
	entityTypes = []string{entityType}
)

func setup(t *testing.T, fallbackFile string, wrp cdc.RetryPolicy) cdc.Store {
	if coll == nil {
		var err error
		client, coll, err = test.DbCollections(entityTypes)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Reset the collection: delete all cdc events and insert the standard cdc events
	cdcColl := coll[entityType]
	_, err := cdcColl.DeleteMany(context.TODO(), bson.D{{}})
	if err != nil {
		t.Fatal(err)
	}

	// First time, create unique index on "x"
	if coll == nil {
		iv := cdcColl.Indexes()
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

	docs := make([]interface{}, len(mock.CDCEvents))
	for i := range mock.CDCEvents {
		docs[i] = mock.CDCEvents[i]
	}
	if _, err := cdcColl.InsertMany(context.TODO(), docs); err != nil {
		t.Fatal(err)
	}

	return cdc.NewStore(cdcColl, fallbackFile, wrp)
}

// --------------------------------------------------------------------------

func TestRead(t *testing.T) {
	cdcs := setup(t, "", cdc.NoRetryPolicy)

	// Filter #1.
	filter := cdc.Filter{
		SinceTs: 13,
		UntilTs: 35,
		Order:   cdc.ByEntityIdRevAsc{},
	}
	events, err := cdcs.Read(filter)
	if err != nil {
		t.Error(err)
	}

	actualIds := []string{}
	for _, event := range events {
		actualIds = append(actualIds, event.Id)
	}

	expectedIds := []string{"p34", "vno", "4pi"} // order matters
	if diff := deep.Equal(actualIds, expectedIds); diff != nil {
		t.Error(diff)
	}

	// Filter #2.
	filter = cdc.Filter{
		SinceTs: 10,
		UntilTs: 43,
		Order:   cdc.ByEntityIdRevAsc{},
	}
	events, err = cdcs.Read(filter)
	if err != nil {
		t.Error(err)
	}

	actualIds = []string{}
	for _, event := range events {
		actualIds = append(actualIds, event.Id)
	}

	expectedIds = []string{"nru", "p34", "61p", "qwp", "vno", "4pi", "vb0", "bnu"} // order matters
	if diff := deep.Equal(actualIds, expectedIds); diff != nil {
		t.Logf("expected: %v", expectedIds)
		t.Logf("     got: %v", actualIds)
		t.Error(diff)
	}
}

func TestWriteSuccess(t *testing.T) {
	cdcs := setup(t, "", cdc.NoRetryPolicy)

	event := etre.CDCEvent{
		Id:         "abc",
		Ts:         54,
		Op:         "i",
		Caller:     "mike",
		EntityId:   "e13",
		EntityType: "node",
		EntityRev:  int64(7),
		New: &etre.Entity{
			"_id":   "e13",
			"_type": "node",
			"_rev":  int64(7),
			"foo":   "bar",
		},
	}

	err := cdcs.Write(context.TODO(), event)
	if err != nil {
		t.Error(err)
	}

	// Get the event we just created.
	filter := cdc.Filter{
		SinceTs: 54,
		UntilTs: 55,
	}
	actualEvents, err := cdcs.Read(filter)
	if err != nil {
		t.Error(err)
	}

	if len(actualEvents) != 1 {
		t.Errorf("got back %d events, expected 1", len(actualEvents))
	}

	if diff := deep.Equal(event, actualEvents[0]); diff != nil {
		t.Error(diff)
	}
}

func TestWriteFailure(t *testing.T) {
	t.Skip("@todo: make db fail")
	wrp := cdc.RetryPolicy{
		RetryCount: 3,
		RetryWait:  0,
	}
	cdcs := setup(t, "", wrp)

	tries := 0

	event := etre.CDCEvent{Id: "abc", EntityId: "e13", EntityRev: 7, Ts: 54}
	err := cdcs.Write(context.TODO(), event)
	if err == nil {
		t.Error("expected an error but did not get one")
	}
	if tries != 4 {
		t.Errorf("create tries = %d, expected 4", tries)
	}
}

func TestWriteFallbackFile(t *testing.T) {
	// @todo: test writing to the fallback file as well
}

type sortTest struct {
	rand []etre.CDCEvent // random
	wro  []etre.CDCEvent // write order
}

func TestSortByEntityIdRev(t *testing.T) {
	sortTests := []sortTest{
		{
			rand: []etre.CDCEvent{
				etre.CDCEvent{Id: "a", EntityId: "e1", EntityRev: 1, Ts: 1}, // written second (rev=1) but logged first at ts=1
				etre.CDCEvent{Id: "b", EntityId: "e1", EntityRev: 0, Ts: 2}, // written first (rev=0) but logged second at ts=2
			},
			wro: []etre.CDCEvent{
				etre.CDCEvent{Id: "b", EntityId: "e1", EntityRev: 0, Ts: 2}, // rev 0 must be before
				etre.CDCEvent{Id: "a", EntityId: "e1", EntityRev: 1, Ts: 1}, // rev 1 regardless of ts
			},
		},
		{
			rand: []etre.CDCEvent{
				etre.CDCEvent{Id: "a", EntityId: "e1", EntityRev: 1, Ts: 1}, // written second, logged at same time
				etre.CDCEvent{Id: "b", EntityId: "e1", EntityRev: 0, Ts: 1}, // written first, logged at same time
			},
			wro: []etre.CDCEvent{
				etre.CDCEvent{Id: "b", EntityId: "e1", EntityRev: 0, Ts: 1}, // rev 0 must be before
				etre.CDCEvent{Id: "a", EntityId: "e1", EntityRev: 1, Ts: 1}, // rev 1 regardless of ts
			},
		},
		{
			rand: []etre.CDCEvent{
				etre.CDCEvent{Id: "a", EntityId: "e1", EntityRev: 3, Ts: 1}, // same entity badly out of order
				etre.CDCEvent{Id: "b", EntityId: "e1", EntityRev: 2, Ts: 1},
				etre.CDCEvent{Id: "c", EntityId: "e1", EntityRev: 0, Ts: 2},
				etre.CDCEvent{Id: "d", EntityId: "e1", EntityRev: 1, Ts: 3},
			},
			wro: []etre.CDCEvent{
				etre.CDCEvent{Id: "c", EntityId: "e1", EntityRev: 0, Ts: 2}, // rev order matters for same entity
				etre.CDCEvent{Id: "d", EntityId: "e1", EntityRev: 1, Ts: 3},
				etre.CDCEvent{Id: "b", EntityId: "e1", EntityRev: 2, Ts: 1},
				etre.CDCEvent{Id: "a", EntityId: "e1", EntityRev: 3, Ts: 1},
			},
		},
		{
			rand: []etre.CDCEvent{
				etre.CDCEvent{Id: "a", EntityId: "e1", EntityRev: 3, Ts: 1}, // two different entities badly out of order
				etre.CDCEvent{Id: "b", EntityId: "e1", EntityRev: 1, Ts: 3}, // ts also out of order, which could happen
				etre.CDCEvent{Id: "c", EntityId: "e9", EntityRev: 0, Ts: 2}, // because we don't ask db to sort by anything
				etre.CDCEvent{Id: "d", EntityId: "e1", EntityRev: 0, Ts: 2},
				etre.CDCEvent{Id: "e", EntityId: "e9", EntityRev: 1, Ts: 1},
				etre.CDCEvent{Id: "f", EntityId: "e1", EntityRev: 2, Ts: 1},
				etre.CDCEvent{Id: "g", EntityId: "e9", EntityRev: 2, Ts: 3},
			},
			wro: []etre.CDCEvent{
				etre.CDCEvent{Id: "d", EntityId: "e1", EntityRev: 0, Ts: 2},
				etre.CDCEvent{Id: "b", EntityId: "e1", EntityRev: 1, Ts: 3},
				etre.CDCEvent{Id: "f", EntityId: "e1", EntityRev: 2, Ts: 1},
				etre.CDCEvent{Id: "a", EntityId: "e1", EntityRev: 3, Ts: 1},
				etre.CDCEvent{Id: "c", EntityId: "e9", EntityRev: 0, Ts: 2},
				etre.CDCEvent{Id: "e", EntityId: "e9", EntityRev: 1, Ts: 1},
				etre.CDCEvent{Id: "g", EntityId: "e9", EntityRev: 2, Ts: 3},
			},
		},
	}

	for i, s := range sortTests {
		sort.Sort(cdc.ByEntityIdRevAsc(s.rand)) // sorts s.rand in place
		if diff := deep.Equal(s.rand, s.wro); diff != nil {
			t.Logf("expected: %v", s.wro)
			t.Logf("     got: %v", s.rand)
			t.Errorf("sort test %d failed, see output above: %v", i, diff)
		}
	}
}
