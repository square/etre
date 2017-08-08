package db_test

import (
	"reflect"
	"testing"

	"github.com/square/etre/db"
	"github.com/square/etre/query"
)

var seedEntities = []db.Entity{
	db.Entity{"x": 2, "y": "hello", "z": []interface{}{"foo", "bar"}},
}

const (
	URL        = "localhost"
	Database   = "etre_test"
	Collection = "entities"
	Timeout    = 5
)

func setup(t *testing.T) db.Connector {
	var err error

	c := db.NewConnector(URL, Database, Collection, Timeout, nil)
	err = c.Connect()
	if err != nil {
		t.Error(err)
	}

	// Create test data
	_, err = c.CreateEntities(seedEntities)
	if err != nil {
		if _, ok := err.(db.ErrCreate); ok {
			t.Errorf("Error creating entities: %s", err)
		} else {
			t.Errorf("Uknown error when creating entities: %s", err)
		}
	}

	return c
}

func teardown(t *testing.T, c db.Connector) {
	// Delete all data in DB/Collection (empty query matches everything).
	q, err := query.Translate("")
	if err != nil {
		t.Error(err)
	}
	_, err = c.DeleteEntities(q)
	if err != nil {
		if _, ok := err.(db.ErrDelete); ok {
			t.Errorf("Error deleting entities: %s", err)
		} else {
			t.Errorf("Uknown error when deleting entities: %s", err)
		}
	}

	// Disconnect from DB
	c.Disconnect()
}

func TestConnect(t *testing.T) {
	c := setup(t)
	defer teardown(t, c)

	// Although a query in setup() was ran and proves we could connect to DB, run
	// another query to make this test explicit.
	q, err := query.Translate("")
	if err != nil {
		t.Error(err)
	}
	_, err = c.ReadEntities(q)
	if err != nil {
		if _, ok := err.(db.ErrRead); ok {
			t.Errorf("Error reading entities: %s", err)
		} else {
			t.Errorf("Uknown error when reading entities: %s", err)
		}
	}
}

func TestDisconnect(t *testing.T) {
	c := setup(t)
	teardown(t, c)
	t.SkipNow()

	// Ensure running a query fails
	q, err := query.Translate("")
	if err != nil {
		t.Error(err)
	}
	_, err = c.ReadEntities(q)
	if err == nil {
		t.Error("Still able to connect to DB. Expected to be disconnected.")
	}
}

func DeleteEntityLabel(t *testing.T) {
	c := setup(t)
	defer teardown(t, c)

	entity := seedEntities[0]
	id := entity["_id"].(string)
	label := "x"
	expect := db.Entity{"_id": id, label: entity[label]}

	actual, err := c.DeleteEntityLabel(id, label)
	if err != nil {
		if _, ok := err.(db.ErrDeleteLabel); ok {
			t.Errorf("Error deleting label: %s", err)
		} else {
			t.Errorf("Uknown error when deleting label: %s", err)
		}
	}

	if !reflect.DeepEqual(actual, expect) {
		t.Errorf("Actual: %v, Expect: %v", actual, expect)
	}
}

func TestCreateEntitiesMultiple(t *testing.T) {
	c := setup(t)
	defer teardown(t, c)

	testData := []db.Entity{
		db.Entity{"x": 0},
		db.Entity{"y": 1},
		db.Entity{"z": 2},
	}
	// Note: teardown will delete this test data
	ids, err := c.CreateEntities(testData)
	if err != nil {
		if _, ok := err.(db.ErrCreate); ok {
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
}

func TestCreateEntitiesMultiplePartialSuccess(t *testing.T) {
	c := setup(t)
	defer teardown(t, c)

	// Expect first two documents to be inserted and third to fail
	testData := []db.Entity{
		db.Entity{"_id": "foo", "x": 0},
		db.Entity{"_id": "bar", "y": 1},
		db.Entity{"_id": "bar", "z": 2},
	}
	// Note: teardown will delete this test data
	actual, err := c.CreateEntities(testData)

	expect := []string{"foo", "bar"}

	if !reflect.DeepEqual(actual, expect) {
		t.Errorf("Actual ids for entities inserted: %v, Expected: %v", actual, expect)
	}
	if err == nil {
		t.Errorf("Expected error but got no error")
	} else {
		if _, ok := err.(db.ErrCreate); ok {
			actual := err.(db.ErrCreate).N
			expect := len(expect)
			if actual != expect {
				t.Errorf("Actual number of inserted entities in error: %v, expected: %v", actual, expect)
			}
		} else {
			t.Errorf("Uknown error when creating entities: %s", err)
		}
	}
}

func TestReadEntitiesWithAllOperators(t *testing.T) {
	c := setup(t)
	defer teardown(t, c)

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
		actual, err := c.ReadEntities(q)
		if err != nil {
			if _, ok := err.(db.ErrRead); ok {
				t.Errorf("Error reading entities: %s", err)
			} else {
				t.Errorf("Uknown error when reading entities: %s", err)
			}
		}

		if !reflect.DeepEqual(actual, expect) {
			t.Errorf("actual: %v, expect:  %v, query: %v", actual, expect, q)
		}
	}
}

func TestReadEntitiesWithComplexQuery(t *testing.T) {
	c := setup(t)
	defer teardown(t, c)

	q, err := query.Translate("y, !a, x>1")
	if err != nil {
		t.Error(err)
	}

	expect := seedEntities

	actual, err := c.ReadEntities(q)
	if err != nil {
		if _, ok := err.(db.ErrRead); ok {
			t.Errorf("Error reading entities: %s", err)
		} else {
			t.Errorf("Uknown error when reading entities: %s", err)
		}
	}

	if !reflect.DeepEqual(actual, expect) {
		t.Errorf("actual: %v, expect:  %v, query: %v", actual, expect, q)
	}
}
func TestReadEntitiesMultipleFound(t *testing.T) {
	c := setup(t)
	defer teardown(t, c)

	// Each Entity has "a" in it so we can query for documents with "a" and
	// delete them
	testData := []db.Entity{
		db.Entity{"a": 1},
		db.Entity{"a": 1, "b": 2},
		db.Entity{"a": 1, "b": 2, "c": 3},
	}
	// Note: teardown will delete this test data
	_, err := c.CreateEntities(testData)
	if err != nil {
		if _, ok := err.(db.ErrCreate); ok {
			t.Errorf("Error creating entities: %s", err)
		} else {
			t.Errorf("Uknown error when creating entities: %s", err)
		}
	}

	q, err := query.Translate("a > 0")
	if err != nil {
		t.Error(err)
	}
	entities, err := c.ReadEntities(q)
	if err != nil {
		if _, ok := err.(db.ErrRead); ok {
			t.Errorf("Error reading entities: %s", err)
		} else {
			t.Errorf("Uknown error when reading entities: %s", err)
		}
	}

	actual := len(entities)
	expect := len(testData)

	if actual != expect {
		t.Errorf("Actual num entities read: %v, Expected num entities read: %v", actual, expect)
	}
}

func TestReadEntitiesNotFound(t *testing.T) {
	c := setup(t)
	defer teardown(t, c)

	q, err := query.Translate("a=b")
	if err != nil {
		t.Error(err)
	}
	actual, err := c.ReadEntities(q)

	if actual != nil {
		t.Errorf("An empty list was expected, actual: %v", actual)
	}
	if err != nil {
		if _, ok := err.(db.ErrRead); ok {
			t.Errorf("Error reading entities: %s", err)
		} else {
			t.Errorf("Uknown error when reading entities: %s", err)
		}
	}
}

func TestUpdateEntities(t *testing.T) {
	c := setup(t)
	defer teardown(t, c)

	// Create another entity to test we can update multiple documents
	testData := []db.Entity{
		db.Entity{"x": 3, "y": "hello"},
	}
	// Note: teardown will delete this data
	_, err := c.CreateEntities(testData)
	if err != nil {
		if _, ok := err.(db.ErrCreate); ok {
			t.Errorf("Error creating entities: %s", err)
		} else {
			t.Errorf("Uknown error when creating entities: %s", err)
		}
	}

	q, err := query.Translate("y=hello")
	if err != nil {
		t.Error(err)
	}
	u := db.Entity{"y": "goodbye"}

	expectNumUpdated := 2

	diff, err := c.UpdateEntities(q, u)
	if err != nil {
		if _, ok := err.(db.ErrUpdate); ok {
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

}

func TestDeleteEntities(t *testing.T) {
	c := setup(t)
	defer teardown(t, c)

	// Each Entity has "a" in it so we can query for documents with "a" and
	// delete them
	testData := []db.Entity{
		db.Entity{"a": 1},
		db.Entity{"a": 1, "b": 2},
		db.Entity{"a": 1, "b": 2, "c": 3},
	}
	_, err := c.CreateEntities(testData)
	if err != nil {
		if _, ok := err.(db.ErrCreate); ok {
			t.Errorf("Error creating entities: %s", err)
		} else {
			t.Errorf("Uknown error when creating entities: %s", err)
		}
	}

	q, err := query.Translate("a > 0")
	if err != nil {
		t.Error(err)
	}
	actualDeletedEntities, err := c.DeleteEntities(q)
	if err != nil {
		if _, ok := err.(db.ErrDelete); ok {
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
	if !reflect.DeepEqual(actualDeletedEntities, testData) {
		t.Errorf("actual: %v, expect:  %v", actualDeletedEntities, testData)
	}
}
