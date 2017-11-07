// Copyright 2017, Square, Inc.

package entity_test

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/square/etre"
	"github.com/square/etre/db"
	"github.com/square/etre/entity"
	"github.com/square/etre/query"
	"github.com/square/etre/test/mock"

	"github.com/go-test/deep"
	"gopkg.in/mgo.v2/bson"
)

var seedEntities []etre.Entity
var seedIds []string

// @todo: make the host/port configurable
var url = "localhost:3000"
var database = "etre_test"
var entityType = "nodes"
var username = "kate"
var entityTypes = []string{entityType}
var timeout = 5
var conn db.Connector
var wo = entity.WriteOp{
	EntityType: entityType,
	User:       username,
}

func setup(t *testing.T, cdcm *mock.CDCStore, d *mock.Delayer) entity.Store {
	conn = db.NewConnector(url, timeout, nil, nil)
	_, err := conn.Connect()
	if err != nil {
		t.Fatal(err)
	}

	es, err := entity.NewStore(conn, database, entityTypes, cdcm, d)

	// Create test data. c.CreateEntities() modfies seedEntities: it sets
	// _id, _type, and _rev. So reset the slice for every test.
	seedEntities = []etre.Entity{
		etre.Entity{"x": 2, "y": "hello", "z": []interface{}{"foo", "bar"}},
	}
	seedIds, err = es.CreateEntities(wo, seedEntities)
	if err != nil {
		if _, ok := err.(entity.ErrCreate); ok {
			t.Fatalf("Error creating entities: %s", err)
		} else {
			t.Fatalf("Uknown error when creating entities: %s", err)
		}
	}

	return es
}

func teardown(t *testing.T, es entity.Store) {
	// Delete all data in DB/Collection (empty query matches everything).
	q, err := query.Translate("")
	if err != nil {
		t.Error(err)
	}
	_, err = es.DeleteEntities(wo, q)
	if err != nil {
		if _, ok := err.(entity.ErrDelete); ok {
			t.Errorf("Error deleting entities: %s", err)
		} else {
			t.Errorf("Uknown error when deleting entities: %s", err)
		}
	}

	// Close db connection.
	conn.Close()
}

func TestCreateNewStoreError(t *testing.T) {
	// This is invalid because it's a reserved name
	invalidEntityType := "entities"
	_, err := entity.NewStore(&mock.Connector{}, database, []string{invalidEntityType}, &mock.CDCStore{}, &mock.Delayer{})
	if !strings.Contains(err.Error(), "reserved word") {
		t.Errorf("err = %s, expected to contain 'reserved word'", err)
	}
}

func TestCreateEntitiesMultiple(t *testing.T) {
	var lastEvent etre.CDCEvent
	cdcm := &mock.CDCStore{
		WriteFunc: func(e etre.CDCEvent) error {
			lastEvent = e
			return nil
		},
	}

	es := setup(t, cdcm, &mock.Delayer{})
	defer teardown(t, es)

	testData := []etre.Entity{
		etre.Entity{"x": 0},
		etre.Entity{"y": 1},
		etre.Entity{"z": 2, "_setId": "343", "_setOp": "something", "_setSize": 1},
	}
	// Note: teardown will delete this test data
	ids, err := es.CreateEntities(wo, testData)
	if err != nil {
		if _, ok := err.(entity.ErrCreate); ok {
			t.Errorf("Error creating entities: %s", err)
		} else {
			t.Errorf("Uknown error when creating entities: %s", err)
		}
	}

	actual := len(ids)
	expect := len(testData)

	if actual != expect {
		t.Errorf("Actual num entities inserted: %v, Expected num entities inserted: %v", actual, expect)
	}

	// Verify that the last CDC event we create is as expected.
	expectedEvent := etre.CDCEvent{
		EventId:    lastEvent.EventId, // can't get this anywhere else
		EntityId:   ids[len(ids)-1],
		EntityType: entityType,
		Rev:        0,
		Ts:         lastEvent.Ts, // can't get this anywhere else
		User:       username,
		Op:         "i",
		Old:        nil,
		New:        &etre.Entity{"_id": bson.ObjectIdHex(ids[len(ids)-1]), "_type": entityType, "_rev": 0, "z": 2, "_setId": "343", "_setOp": "something", "_setSize": 1},
		SetId:      "343",
		SetOp:      "something",
		SetSize:    1,
	}
	if diff := deep.Equal(lastEvent, expectedEvent); diff != nil {
		t.Logf("got: %#v", lastEvent)
		t.Error(diff)
	}
}

func TestCreateEntitiesMultiplePartialSuccess(t *testing.T) {
	t.Skip("need to create unique index on z on to make this fail again")

	es := setup(t, &mock.CDCStore{}, &mock.Delayer{})
	defer teardown(t, es)

	// Expect first two documents to be inserted and third to fail
	testData := []etre.Entity{
		etre.Entity{"_id": "foo", "x": 0},
		etre.Entity{"_id": "bar", "y": 1},
		etre.Entity{"_id": "bar", "z": 2},
	}
	// Note: teardown will delete this test data
	actual, err := es.CreateEntities(wo, testData)

	expect := []string{"foo", "bar"}

	if diff := deep.Equal(actual, expect); diff != nil {
		t.Error(diff)
	}
	if err == nil {
		t.Errorf("Expected error but got no error")
	} else {
		if _, ok := err.(entity.ErrCreate); ok {
			actual := err.(entity.ErrCreate).N
			expect := len(expect)
			if actual != expect {
				t.Errorf("Actual number of inserted entities in error: %v, expected: %v", actual, expect)
			}
		} else {
			t.Errorf("Uknown error when creating entities: %s", err)
		}
	}
}

func TestCreateEntitiesInvalidEntityType(t *testing.T) {
	es := setup(t, &mock.CDCStore{}, &mock.Delayer{})
	defer teardown(t, es)

	testData := []etre.Entity{
		etre.Entity{"x": 0},
		etre.Entity{"y": 1},
		etre.Entity{"z": 2},
	}

	// This is invalid because it's a reserved name
	invalidEntityType := "entities"
	wo := entity.WriteOp{
		EntityType: invalidEntityType,
		User:       username,
	}
	_, err := es.CreateEntities(wo, testData)

	expectedErrMsg := "Invalid entity type: " + invalidEntityType
	if !strings.Contains(err.Error(), expectedErrMsg) {
		t.Errorf("err = %s, expected to contain: %s", err, expectedErrMsg)
	}
}

func TestReadEntitiesWithAllOperators(t *testing.T) {
	es := setup(t, &mock.CDCStore{}, &mock.Delayer{})
	defer teardown(t, es)

	// List of label selctors to build queries from
	labelSelectors := []string{
		"y in (hello, goodbye)",
		"y notin (morning, night)",
		"y = hello",
		"y == hello",
		"y != goodbye",
		"y",
		"!a",
		"x > 1",
		"x < 3",
	}

	// There's strategically only one Entity we expect to return to make testing easier
	expect := seedEntities

	for i, l := range labelSelectors {
		q, err := query.Translate(l)
		if err != nil {
			t.Fatalf("cannot translate '%s': %s", l, err)
		}
		actual, err := es.ReadEntities(entityType, q, etre.QueryFilter{})
		if err != nil {
			if _, ok := err.(entity.ErrRead); ok {
				t.Errorf("%d Error reading entities: %s", i, err)
			} else {
				t.Errorf("%d: Unknown error when reading entities: %s", i, err)
			}
		}

		if diff := deep.Equal(actual, expect); diff != nil {
			t.Errorf("%d: %+v", i, diff)
		}
	}
}

func TestReadEntitiesWithComplexQuery(t *testing.T) {
	es := setup(t, &mock.CDCStore{}, &mock.Delayer{})
	defer teardown(t, es)

	q, err := query.Translate("y, !a, x>1")
	if err != nil {
		t.Error(err)
	}

	expect := seedEntities

	actual, err := es.ReadEntities(entityType, q, etre.QueryFilter{})
	if err != nil {
		if _, ok := err.(entity.ErrRead); ok {
			t.Errorf("Error reading entities: %s", err)
		} else {
			t.Errorf("Uknown error when reading entities: %s", err)
		}
	}

	if diff := deep.Equal(actual, expect); diff != nil {
		t.Error(diff)
	}
}

func TestReadEntitiesMultipleFound(t *testing.T) {
	es := setup(t, &mock.CDCStore{}, &mock.Delayer{})
	defer teardown(t, es)

	// Each Entity has "a" in it so we can query for documents with "a" and
	// delete them
	testData := []etre.Entity{
		etre.Entity{"a": 1},
		etre.Entity{"a": 1, "b": 2},
		etre.Entity{"a": 1, "b": 2, "c": 3},
	}
	// Note: teardown will delete this test data
	_, err := es.CreateEntities(wo, testData)
	if err != nil {
		if _, ok := err.(entity.ErrCreate); ok {
			t.Errorf("Error creating entities: %s", err)
		} else {
			t.Errorf("Uknown error when creating entities: %s", err)
		}
	}

	q, err := query.Translate("a > 0")
	if err != nil {
		t.Error(err)
	}
	entities, err := es.ReadEntities(entityType, q, etre.QueryFilter{})
	if err != nil {
		if _, ok := err.(entity.ErrRead); ok {
			t.Errorf("Error reading entities: %s", err)
		} else {
			t.Errorf("Uknown error when reading entities: %s", err)
		}
	}

	actual := len(entities)
	expect := len(testData)

	if diff := deep.Equal(actual, expect); diff != nil {
		t.Error(diff)
	}
}

func TestReadEntitiesNotFound(t *testing.T) {
	es := setup(t, &mock.CDCStore{}, &mock.Delayer{})
	defer teardown(t, es)

	q, err := query.Translate("a=b")
	if err != nil {
		t.Error(err)
	}
	actual, err := es.ReadEntities(entityType, q, etre.QueryFilter{})

	if len(actual) != 0 {
		t.Errorf("An empty list was expected, actual: %v", actual)
	}
	if err != nil {
		if _, ok := err.(entity.ErrRead); ok {
			t.Errorf("Error reading entities: %s", err)
		} else {
			t.Errorf("Uknown error when reading entities: %s", err)
		}
	}
}

func TestReadEntitiesInvalidEntityType(t *testing.T) {
	es := setup(t, &mock.CDCStore{}, &mock.Delayer{})
	defer teardown(t, es)

	q, err := query.Translate("a=b")
	if err != nil {
		t.Error(err)
	}

	// This is invalid because it's a reserved name
	invalidEntityType := "entities"
	_, err = es.ReadEntities(invalidEntityType, q, etre.QueryFilter{})

	expectedErrMsg := "Invalid entity type: " + invalidEntityType
	if !strings.Contains(err.Error(), expectedErrMsg) {
		t.Errorf("err = %s, expected to contain: %s", err, expectedErrMsg)
	}
}

func TestUpdateEntities(t *testing.T) {
	var lastEvent etre.CDCEvent
	cdcm := &mock.CDCStore{
		WriteFunc: func(e etre.CDCEvent) error {
			lastEvent = e
			return nil
		},
	}

	es := setup(t, cdcm, &mock.Delayer{})
	defer teardown(t, es)

	// Create another entity to test we can update multiple documents
	// Note: teardown will delete this data
	testData := []etre.Entity{
		etre.Entity{"x": 3, "y": "hello"},
	}
	_, err := es.CreateEntities(wo, testData)
	if err != nil {
		t.Fatal(err)
	}

	q, err := query.Translate("y=hello")
	if err != nil {
		t.Error(err)
	}
	u := etre.Entity{"y": "goodbye"}

	wo := entity.WriteOp{
		EntityType: entityType,
		User:       username,
		SetOp:      "something",
		SetId:      "343",
		SetSize:    1,
	}

	diff, err := es.UpdateEntities(wo, q, u)
	if err != nil {
		t.Fatal(err)
	}

	// Test number of entities updated
	actualNumUpdated := len(diff)
	expectNumUpdated := 2
	if actualNumUpdated != expectNumUpdated {
		t.Errorf("Actual num updated: %v, Expect num updated: %v", actualNumUpdated, expectNumUpdated)
	}

	// Test old values were returned
	var actualOldValue string
	expectOldValue := "hello"
	for _, d := range diff {
		actualOldValue = d["y"].(string)
		if actualOldValue != expectOldValue {
			t.Errorf("Actual old y value: %v, Expect old y value: %v", actualOldValue, expectOldValue)
		}

	}

	// Verify that the last CDC event we create is as expected.
	lastEntityId := diff[len(diff)-1]["_id"].(bson.ObjectId)
	expectedEvent := etre.CDCEvent{
		EventId:    lastEvent.EventId, // can't get this anywhere else
		EntityId:   hex.EncodeToString([]byte(lastEntityId)),
		EntityType: entityType,
		Rev:        1,
		Ts:         lastEvent.Ts, // can't get this anywhere else
		User:       username,
		Op:         "u",
		Old:        &etre.Entity{"_id": lastEntityId, "_type": entityType, "_rev": 0, "y": "hello"},
		New:        &etre.Entity{"_id": lastEntityId, "_type": entityType, "_rev": 1, "y": "goodbye"},
		SetId:      "343",
		SetOp:      "something",
		SetSize:    1,
	}
	if diff := deep.Equal(lastEvent, expectedEvent); diff != nil {
		t.Error(diff)
	}
}

func TestUpdateEntitiesInvalidEntityType(t *testing.T) {
	es := setup(t, &mock.CDCStore{}, &mock.Delayer{})
	defer teardown(t, es)

	q, err := query.Translate("y=hello")
	if err != nil {
		t.Error(err)
	}
	u := etre.Entity{"y": "goodbye"}

	// This is invalid because it's a reserved name
	invalidEntityType := "entities"
	wo := entity.WriteOp{
		EntityType: invalidEntityType,
		User:       username,
	}
	_, err = es.UpdateEntities(wo, q, u)

	expectedErrMsg := "Invalid entity type: " + invalidEntityType
	if !strings.Contains(err.Error(), expectedErrMsg) {
		t.Errorf("err = %s, expected to contain: %s", err, expectedErrMsg)
	}
}

func TestDeleteEntities(t *testing.T) {
	var lastEvent etre.CDCEvent
	cdcm := &mock.CDCStore{
		WriteFunc: func(e etre.CDCEvent) error {
			lastEvent = e
			return nil
		},
	}

	es := setup(t, cdcm, &mock.Delayer{})
	defer teardown(t, es)

	// Each entity has "a" in it so we can query for documents with "a" and
	// delete them
	testData := []etre.Entity{
		etre.Entity{"a": 1},
		etre.Entity{"a": 1, "b": 2},
		etre.Entity{"a": 1, "b": 2, "c": 3},
	}
	ids, err := es.CreateEntities(wo, testData)
	if err != nil {
		if _, ok := err.(entity.ErrCreate); ok {
			t.Errorf("Error creating entities: %s", err)
		} else {
			t.Errorf("Uknown error when creating entities: %s", err)
		}
	}

	q, err := query.Translate("a > 0")
	if err != nil {
		t.Error(err)
	}
	actualDeletedEntities, err := es.DeleteEntities(wo, q)
	if err != nil {
		if _, ok := err.(entity.ErrDelete); ok {
			t.Errorf("Error deleting entities: %s", err)
		} else {
			t.Errorf("Uknown error when deleting entities: %s", err)
		}
	}

	// Test correct number of entities were deleted
	actualNumDeleted := len(actualDeletedEntities)
	expectNumDeleted := len(testData)
	if actualNumDeleted != expectNumDeleted {
		t.Errorf("Actual num entities deleted: %v, Expected num entities deleted: %v", actualNumDeleted, expectNumDeleted)
	}

	// Test correct entities were deleted
	expect := make([]etre.Entity, len(testData))
	for i, id := range ids {
		expect[i] = testData[i]
		// These were set by Etre on insert:
		expect[i]["_id"] = bson.ObjectIdHex(id)
		expect[i]["_rev"] = 0
		expect[i]["_type"] = entityType
	}
	if diff := deep.Equal(actualDeletedEntities, expect); diff != nil {
		t.Error(diff)
	}

	// Verify that the last CDC event we create is as expected.
	lastEntityId := actualDeletedEntities[len(actualDeletedEntities)-1]["_id"].(bson.ObjectId)
	expectedEvent := etre.CDCEvent{
		EventId:    lastEvent.EventId, // can't get this anywhere else
		EntityId:   hex.EncodeToString([]byte(lastEntityId)),
		EntityType: entityType,
		Rev:        uint(1),
		Ts:         lastEvent.Ts, // can't get this anywhere else
		User:       username,
		Op:         "d",
		Old:        &etre.Entity{"_id": lastEntityId, "_type": entityType, "_rev": 0, "a": 1, "b": 2, "c": 3},
		New:        nil,
	}
	if diff := deep.Equal(lastEvent, expectedEvent); diff != nil {
		t.Error(diff)
	}
}

func TestDeleteEntitiesInvalidEntityType(t *testing.T) {
	es := setup(t, &mock.CDCStore{}, &mock.Delayer{})
	defer teardown(t, es)

	q, err := query.Translate("a > 0")
	if err != nil {
		t.Error(err)
	}

	// This is invalid because it's a reserved name
	invalidEntityType := "entities"
	wo := entity.WriteOp{
		EntityType: invalidEntityType,
		User:       username,
	}
	_, err = es.DeleteEntities(wo, q)

	expectedErrMsg := "Invalid entity type: " + invalidEntityType
	if !strings.Contains(err.Error(), expectedErrMsg) {
		t.Errorf("err = %s, expected to contain: %s", err, expectedErrMsg)
	}
}
