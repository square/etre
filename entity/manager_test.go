// Copyright 2017, Square, Inc.

package entity_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/square/etre"
	"github.com/square/etre/db"
	"github.com/square/etre/entity"
	"github.com/square/etre/query"
	"github.com/square/etre/test/mock"

	"github.com/go-test/deep"
)

var seedEntities = []etre.Entity{
	etre.Entity{"_id": "aaa", "x": 2, "y": "hello", "z": []interface{}{"foo", "bar"}},
}

// @todo: make the host/port configurable
var url = "localhost:3000"
var database = "etre_test"
var entityType = "nodes"
var username = "kate"
var entityTypes = []string{entityType}
var timeout = 5
var conn db.Connector

func setup(t *testing.T, cdcm *mock.CDCManager, dm *mock.DelayManager) entity.Manager {
	conn = db.NewConnector(url, timeout, nil, nil)
	_, err := conn.Connect()
	if err != nil {
		t.Error(err)
	}

	em, err := entity.NewManager(conn, database, entityTypes, cdcm, dm)

	// Create test data
	_, err = em.CreateEntities(entityType, seedEntities, username)
	if err != nil {
		if _, ok := err.(entity.ErrCreate); ok {
			t.Errorf("Error creating entities: %s", err)
		} else {
			t.Errorf("Uknown error when creating entities: %s", err)
		}
	}

	return em
}

func teardown(t *testing.T, em entity.Manager) {
	// Delete all data in DB/Collection (empty query matches everything).
	q, err := query.Translate("")
	if err != nil {
		t.Error(err)
	}
	_, err = em.DeleteEntities(entityType, q, username)
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

func TestCreateNewManagerError(t *testing.T) {
	// This is invalid because it's a reserved name
	invalidEntityType := "entities"
	expectedErrMsg := fmt.Sprintf("Entity type (%s) cannot be a reserved word", invalidEntityType)
	_, err := entity.NewManager(&mock.Connector{}, database, []string{invalidEntityType}, &mock.CDCManager{}, &mock.DelayManager{})
	if !strings.Contains(err.Error(), expectedErrMsg) {
		t.Errorf("err = %s, expected to contain: %s", err, expectedErrMsg)
	}
}

func TestCreateEntitiesMultiple(t *testing.T) {
	var lastEvent etre.CDCEvent
	cdcm := &mock.CDCManager{
		CreateEventFunc: func(e etre.CDCEvent) error {
			lastEvent = e
			return nil
		},
	}

	em := setup(t, cdcm, &mock.DelayManager{})
	defer teardown(t, em)

	testData := []etre.Entity{
		etre.Entity{"_id": "tjf", "x": 0},
		etre.Entity{"_id": "abc", "y": 1},
		etre.Entity{"_id": "fes", "z": 2, "setId": "343", "setOp": "something", "setSize": 1},
	}
	// Note: teardown will delete this test data
	ids, err := em.CreateEntities(entityType, testData, username)
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
		EntityId:   "fes",
		EntityType: entityType,
		Rev:        0,
		Ts:         lastEvent.Ts, // can't get this anywhere else
		User:       username,
		Op:         "i",
		Old:        nil,
		New:        &etre.Entity{"_id": "fes", "_rev": 0, "z": 2},
		SetId:      "343",
		SetOp:      "something",
		SetSize:    1,
	}
	if diff := deep.Equal(lastEvent, expectedEvent); diff != nil {
		t.Error(diff)
	}
}

func TestCreateEntitiesMultiplePartialSuccess(t *testing.T) {
	em := setup(t, &mock.CDCManager{}, &mock.DelayManager{})
	defer teardown(t, em)

	// Expect first two documents to be inserted and third to fail
	testData := []etre.Entity{
		etre.Entity{"_id": "foo", "x": 0},
		etre.Entity{"_id": "bar", "y": 1},
		etre.Entity{"_id": "bar", "z": 2},
	}
	// Note: teardown will delete this test data
	actual, err := em.CreateEntities(entityType, testData, username)

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
	em := setup(t, &mock.CDCManager{}, &mock.DelayManager{})
	defer teardown(t, em)

	testData := []etre.Entity{
		etre.Entity{"x": 0},
		etre.Entity{"y": 1},
		etre.Entity{"z": 2},
	}

	expectedErrMsg := "Invalid entityType name"
	// This is invalid because it's a reserved name
	invalidEntityType := "entities"
	_, err := em.CreateEntities(invalidEntityType, testData, username)
	if !strings.Contains(err.Error(), expectedErrMsg) {
		t.Errorf("err = %s, expected to contain: %s", err, expectedErrMsg)
	}
}

func TestReadEntitiesWithAllOperators(t *testing.T) {
	em := setup(t, &mock.CDCManager{}, &mock.DelayManager{})
	defer teardown(t, em)

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

	for _, l := range labelSelectors {
		q, err := query.Translate(l)
		if err != nil {
			t.Error(err)
		}
		actual, err := em.ReadEntities(entityType, q)
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
}

func TestReadEntitiesWithComplexQuery(t *testing.T) {
	em := setup(t, &mock.CDCManager{}, &mock.DelayManager{})
	defer teardown(t, em)

	q, err := query.Translate("y, !a, x>1")
	if err != nil {
		t.Error(err)
	}

	expect := seedEntities

	actual, err := em.ReadEntities(entityType, q)
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
	em := setup(t, &mock.CDCManager{}, &mock.DelayManager{})
	defer teardown(t, em)

	// Each Entity has "a" in it so we can query for documents with "a" and
	// delete them
	testData := []etre.Entity{
		etre.Entity{"a": 1},
		etre.Entity{"a": 1, "b": 2},
		etre.Entity{"a": 1, "b": 2, "c": 3},
	}
	// Note: teardown will delete this test data
	_, err := em.CreateEntities(entityType, testData, username)
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
	entities, err := em.ReadEntities(entityType, q)
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
	em := setup(t, &mock.CDCManager{}, &mock.DelayManager{})
	defer teardown(t, em)

	q, err := query.Translate("a=b")
	if err != nil {
		t.Error(err)
	}
	actual, err := em.ReadEntities(entityType, q)

	if actual != nil {
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
	em := setup(t, &mock.CDCManager{}, &mock.DelayManager{})
	defer teardown(t, em)

	q, err := query.Translate("a=b")
	if err != nil {
		t.Error(err)
	}

	expectedErrMsg := "Invalid entityType name"
	// This is invalid because it's a reserved name
	invalidEntityType := "entities"
	_, err = em.ReadEntities(invalidEntityType, q)
	if !strings.Contains(err.Error(), expectedErrMsg) {
		t.Errorf("err = %s, expected to contain: %s", err, expectedErrMsg)
	}
}

func TestUpdateEntities(t *testing.T) {
	var lastEvent etre.CDCEvent
	cdcm := &mock.CDCManager{
		CreateEventFunc: func(e etre.CDCEvent) error {
			lastEvent = e
			return nil
		},
	}

	em := setup(t, cdcm, &mock.DelayManager{})
	defer teardown(t, em)

	// Create another entity to test we can update multiple documents
	testData := []etre.Entity{
		etre.Entity{"_id": "zzz", "x": 3, "y": "hello"},
	}
	// Note: teardown will delete this data
	_, err := em.CreateEntities(entityType, testData, username)
	if err != nil {
		if _, ok := err.(entity.ErrCreate); ok {
			t.Errorf("Error creating entities: %s", err)
		} else {
			t.Errorf("Uknown error when creating entities: %s", err)
		}
	}

	q, err := query.Translate("y=hello")
	if err != nil {
		t.Error(err)
	}
	u := etre.Entity{"y": "goodbye", "setId": "343", "setOp": "something", "setSize": 1}

	expectNumUpdated := 2

	diff, err := em.UpdateEntities(entityType, q, u, username)
	if err != nil {
		if _, ok := err.(entity.ErrUpdate); ok {
			t.Errorf("Error updating entities: %s", err)
		} else {
			t.Errorf("Uknown error when updating entities: %s", err)
		}
	}

	// Test number of entities updated
	actualNumUpdated := len(diff)
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
	expectedEvent := etre.CDCEvent{
		EventId:    lastEvent.EventId, // can't get this anywhere else
		EntityId:   "zzz",
		EntityType: entityType,
		Rev:        1,
		Ts:         lastEvent.Ts, // can't get this anywhere else
		User:       username,
		Op:         "u",
		Old:        &etre.Entity{"_id": "zzz", "_rev": 0, "y": "hello"},
		New:        &etre.Entity{"_id": "zzz", "_rev": 1, "y": "goodbye"},
		SetId:      "343",
		SetOp:      "something",
		SetSize:    1,
	}
	if diff := deep.Equal(lastEvent, expectedEvent); diff != nil {
		t.Error(diff)
	}
}

func TestUpdateEntitiesInvalidEntityType(t *testing.T) {
	em := setup(t, &mock.CDCManager{}, &mock.DelayManager{})
	defer teardown(t, em)

	q, err := query.Translate("y=hello")
	if err != nil {
		t.Error(err)
	}
	u := etre.Entity{"y": "goodbye"}

	expectedErrMsg := "Invalid entityType name"
	// This is invalid because it's a reserved name
	invalidEntityType := "entities"
	_, err = em.UpdateEntities(invalidEntityType, q, u, username)
	if !strings.Contains(err.Error(), expectedErrMsg) {
		t.Errorf("err = %s, expected to contain: %s", err, expectedErrMsg)
	}
}

func TestDeleteEntities(t *testing.T) {
	var lastEvent etre.CDCEvent
	cdcm := &mock.CDCManager{
		CreateEventFunc: func(e etre.CDCEvent) error {
			lastEvent = e
			return nil
		},
	}

	em := setup(t, cdcm, &mock.DelayManager{})
	defer teardown(t, em)

	// Each entity has "a" in it so we can query for documents with "a" and
	// delete them
	testData := []etre.Entity{
		etre.Entity{"_id": "ddd", "a": 1},
		etre.Entity{"_id": "eee", "a": 1, "b": 2},
		etre.Entity{"_id": "fff", "a": 1, "b": 2, "c": 3},
	}
	_, err := em.CreateEntities(entityType, testData, username)
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
	actualDeletedEntities, err := em.DeleteEntities(entityType, q, username)
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
	if diff := deep.Equal(actualDeletedEntities, testData); diff != nil {
		t.Error(diff)
	}

	// Verify that the last CDC event we create is as expected.
	expectedEvent := etre.CDCEvent{
		EventId:    lastEvent.EventId, // can't get this anywhere else
		EntityId:   "fff",
		EntityType: entityType,
		Rev:        1,
		Ts:         lastEvent.Ts, // can't get this anywhere else
		User:       username,
		Op:         "d",
		Old:        &etre.Entity{"_id": "fff", "_rev": 0, "a": 1, "b": 2, "c": 3},
		New:        nil,
	}
	if diff := deep.Equal(lastEvent, expectedEvent); diff != nil {
		t.Error(diff)
	}
}

func TestDeleteEntitiesInvalidEntityType(t *testing.T) {
	em := setup(t, &mock.CDCManager{}, &mock.DelayManager{})
	defer teardown(t, em)

	q, err := query.Translate("a > 0")
	if err != nil {
		t.Error(err)
	}

	expectedErrMsg := "Invalid entityType name"
	// This is invalid because it's a reserved name
	invalidEntityType := "entities"
	_, err = em.DeleteEntities(invalidEntityType, q, username)
	if !strings.Contains(err.Error(), expectedErrMsg) {
		t.Errorf("err = %s, expected to contain: %s", err, expectedErrMsg)
	}
}
