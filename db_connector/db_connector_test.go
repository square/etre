package db_connector_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/square/etre/db_connector"
	"github.com/square/etre/query"
	"gopkg.in/mgo.v2/bson"
)

const (
	URL        = "localhost"
	Database   = "etre_test"
	Collection = "entities"
)

func setup(t *testing.T) *db_connector.DBConnector {
	dbc, err := db_connector.NewDBConnector(URL, Database, Collection)
	if err != nil {
		t.Error(err)
	}
	return dbc
}

func seedData(dbc *db_connector.DBConnector) {
	c := dbc.Session.DB(dbc.Database).C(dbc.Collection)
	c.Insert(bson.M{"x": 2, "y": "hello", "z": []string{"foo", "bar"}})
}

func teardown(t *testing.T, dbc *db_connector.DBConnector) {
	// Drop test DB
	err := dbc.Session.DB(dbc.Database).DropDatabase()
	if err != nil {
		t.Error(err)
	}

	// Disconnect from DB
	dbc.CloseSession()
}

// /////////////////////////////////////////////////////////////////////////
// Tests
// //////////////////////////////////////////////////////////////////////////

// Ensure NewDBConnector is created with correct data
func TestNewDBConnector(t *testing.T) {
	dbc := setup(t)
	defer teardown(t, dbc)

	if dbc.URL != URL {
		t.Errorf("expected=%v", dbc.URL, "got=%v", URL)
	}

	if dbc.Database != Database {
		t.Errorf("expected=%v", dbc.Database, "got=%v", Database)
	}

	if dbc.Collection != Collection {
		t.Errorf("expected=%v", dbc.Collection, "got=%v", Collection)
	}

	if dbc.Session == nil {
		t.Error("Expected dbc.Session to be set, but it is not.")
	}
}

func TestCloseSession(t *testing.T) {
	t.SkipNow()

	// TODO: Check that dbc.Session receieved Close()
}

func TestQueryFound(t *testing.T) {
	dbc := setup(t)
	seedData(dbc)
	defer teardown(t, dbc)

	expectedEntity := map[string]interface{}{
		"x": 2,
		"y": "hello",
		"z": []interface{}{"foo", "bar"},
	}

	// Each predicate will be part of a different query, which we all expect the
	// same entity back
	predicates := []query.Predicate{
		query.Predicate{
			Label:    "y",
			Operator: "in",
			Values:   []string{"hello", "goodbye"},
		},
		query.Predicate{
			Label:    "y",
			Operator: "nin",
			Values:   []string{"good morning", "good night"},
		},
		query.Predicate{
			Label:    "y",
			Operator: "=",
			Values:   []string{"hello"},
		},
		query.Predicate{
			Label:    "y",
			Operator: "==",
			Values:   []string{"hello"},
		},
		query.Predicate{
			Label:    "y",
			Operator: "!=",
			Values:   []string{"goodbye"},
		},
		query.Predicate{
			Label:    "y",
			Operator: "exists",
			Values:   []string{},
		},
		query.Predicate{
			Label:    "a",
			Operator: "!",
			Values:   []string{},
		},
		query.Predicate{
			Label:    "x",
			Operator: "gt",
			Values:   []string{"1"},
		},
		query.Predicate{
			Label:    "x",
			Operator: "lt",
			Values:   []string{"3"},
		},
	}

	for _, p := range predicates {
		q := query.Query{[]query.Predicate{p}}
		actualEntity, err := dbc.Query(q)
		if err != nil {
			t.Error(err)
		}

		for _, key := range []string{"x", "y", "z"} {
			if !reflect.DeepEqual(actualEntity[key], expectedEntity[key]) {
				t.Errorf("%v=%v, expected %v=%v", key, actualEntity[key], key, expectedEntity[key])
			}
		}
	}
}

func TestQueryNotFound(t *testing.T) {
	dbc := setup(t)
	seedData(dbc)
	defer teardown(t, dbc)

	expectedErr := errors.New("not found")

	q := query.Query{
		[]query.Predicate{

			query.Predicate{
				Label:    "a",
				Operator: "=",
				Values:   []string{"b"},
			},
		},
	}
	_, actualErr := dbc.Query(q)

	if !reflect.DeepEqual(actualErr, expectedErr) {
		t.Errorf("error=%v, expected error=%v", actualErr, expectedErr)
	}
}
