// Copyright 2017-2020, Square, Inc.

package cdc_test

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

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
		require.NoError(t, err)
	}

	// Reset the collection: delete all cdc events and insert the standard cdc events
	cdcColl := coll[entityType]
	_, err := cdcColl.DeleteMany(context.TODO(), bson.D{{}})
	require.NoError(t, err)

	// First time, create unique index on "x"
	if coll == nil {
		iv := cdcColl.Indexes()
		if err := iv.DropAll(context.TODO()); err != nil {
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
	require.NoError(t, err)

	actualIds := []string{}
	for _, event := range events {
		actualIds = append(actualIds, event.Id)
	}

	expectedIds := []string{"p34", "vno", "4pi"} // order matters
	assert.Equal(t, expectedIds, actualIds)

	// Filter #2.
	filter = cdc.Filter{
		SinceTs: 10,
		UntilTs: 43,
		Order:   cdc.ByEntityIdRevAsc{},
	}
	events, err = cdcs.Read(filter)
	require.NoError(t, err)

	actualIds = []string{}
	for _, event := range events {
		actualIds = append(actualIds, event.Id)
	}

	expectedIds = []string{"nru", "p34", "61p", "qwp", "vno", "4pi", "vb0", "bnu"} // order matters
	assert.Equal(t, expectedIds, actualIds)
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
	require.NoError(t, err)

	// Get the event we just created.
	filter := cdc.Filter{
		SinceTs: 54,
		UntilTs: 55,
	}
	actualEvents, err := cdcs.Read(filter)
	require.NoError(t, err)
	assert.Len(t, actualEvents, 1)
	assert.Equal(t, event, actualEvents[0])
}

func TestWriteFallbackFile(t *testing.T) {
	fallbackFile, err := ioutil.TempFile("", "etre-cdc-test.json")
	require.NoError(t, err)
	defer os.Remove(fallbackFile.Name())

	fallbackFile.Close()

	cdcs := setup(t, fallbackFile.Name(), cdc.NoRetryPolicy)

	// Write an event...
	event := etre.CDCEvent{Id: "abc", EntityId: "e13", EntityRev: 7, Ts: 54}
	err = cdcs.Write(context.TODO(), event)
	require.NoError(t, err)

	// Then write the same event which causes a duplice key error and triggers
	// a write to the fallback file...
	err = cdcs.Write(context.TODO(), event)
	require.Error(t, err)

	bytes, err := ioutil.ReadFile(fallbackFile.Name())
	var gotEvent etre.CDCEvent
	if err := json.Unmarshal(bytes, &gotEvent); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, event, gotEvent)
}

type sortTest struct {
	rand []etre.CDCEvent // random
	wro  []etre.CDCEvent // write order
}

func TestSortByEntityIdRev(t *testing.T) {
	sortTests := []sortTest{
		{
			rand: []etre.CDCEvent{
				{Id: "a", EntityId: "e1", EntityRev: 1, Ts: 1}, // written second (rev=1) but logged first at ts=1
				{Id: "b", EntityId: "e1", EntityRev: 0, Ts: 2}, // written first (rev=0) but logged second at ts=2
			},
			wro: []etre.CDCEvent{
				{Id: "b", EntityId: "e1", EntityRev: 0, Ts: 2}, // rev 0 must be before
				{Id: "a", EntityId: "e1", EntityRev: 1, Ts: 1}, // rev 1 regardless of ts
			},
		},
		{
			rand: []etre.CDCEvent{
				{Id: "a", EntityId: "e1", EntityRev: 1, Ts: 1}, // written second, logged at same time
				{Id: "b", EntityId: "e1", EntityRev: 0, Ts: 1}, // written first, logged at same time
			},
			wro: []etre.CDCEvent{
				{Id: "b", EntityId: "e1", EntityRev: 0, Ts: 1}, // rev 0 must be before
				{Id: "a", EntityId: "e1", EntityRev: 1, Ts: 1}, // rev 1 regardless of ts
			},
		},
		{
			rand: []etre.CDCEvent{
				{Id: "a", EntityId: "e1", EntityRev: 3, Ts: 1}, // same entity badly out of order
				{Id: "b", EntityId: "e1", EntityRev: 2, Ts: 1},
				{Id: "c", EntityId: "e1", EntityRev: 0, Ts: 2},
				{Id: "d", EntityId: "e1", EntityRev: 1, Ts: 3},
			},
			wro: []etre.CDCEvent{
				{Id: "c", EntityId: "e1", EntityRev: 0, Ts: 2}, // rev order matters for same entity
				{Id: "d", EntityId: "e1", EntityRev: 1, Ts: 3},
				{Id: "b", EntityId: "e1", EntityRev: 2, Ts: 1},
				{Id: "a", EntityId: "e1", EntityRev: 3, Ts: 1},
			},
		},
		{
			rand: []etre.CDCEvent{
				{Id: "a", EntityId: "e1", EntityRev: 3, Ts: 1}, // two different entities badly out of order
				{Id: "b", EntityId: "e1", EntityRev: 1, Ts: 3}, // ts also out of order, which could happen
				{Id: "c", EntityId: "e9", EntityRev: 0, Ts: 2}, // because we don't ask db to sort by anything
				{Id: "d", EntityId: "e1", EntityRev: 0, Ts: 2},
				{Id: "e", EntityId: "e9", EntityRev: 1, Ts: 1},
				{Id: "f", EntityId: "e1", EntityRev: 2, Ts: 1},
				{Id: "g", EntityId: "e9", EntityRev: 2, Ts: 3},
			},
			wro: []etre.CDCEvent{
				{Id: "d", EntityId: "e1", EntityRev: 0, Ts: 2},
				{Id: "b", EntityId: "e1", EntityRev: 1, Ts: 3},
				{Id: "f", EntityId: "e1", EntityRev: 2, Ts: 1},
				{Id: "a", EntityId: "e1", EntityRev: 3, Ts: 1},
				{Id: "c", EntityId: "e9", EntityRev: 0, Ts: 2},
				{Id: "e", EntityId: "e9", EntityRev: 1, Ts: 1},
				{Id: "g", EntityId: "e9", EntityRev: 2, Ts: 3},
			},
		},
	}

	for i, s := range sortTests {
		sort.Sort(cdc.ByEntityIdRevAsc(s.rand)) // sorts s.rand in place
		assert.Equal(t, s.wro, s.rand, "sort test %d failed", i)
	}
}
