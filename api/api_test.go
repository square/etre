// Copyright 2017, Square, Inc.

package api_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"

	"github.com/go-test/deep"
	"github.com/square/etre/api"
	"github.com/square/etre/db"
	"github.com/square/etre/query"
	"github.com/square/etre/router"
	"github.com/square/etre/test"
	"gopkg.in/mgo.v2/bson"
)

var defaultAPI *api.API
var defaultServer *httptest.Server
var seedEntity0 = db.Entity{"x": 0, "foo": "bar"}
var seedEntity1 = db.Entity{"x": 1, "foo": "bar"}
var seedEntity2 = db.Entity{"x": 2, "foo": "bar"}
var seedId0 string
var seedEntities = []db.Entity{seedEntity0, seedEntity1, seedEntity2}
var entityType = "nodes"

func setup(t *testing.T) {
	c, err := db.NewConnector("localhost", "etre", []string{entityType}, 5, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = c.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defaultAPI = api.NewAPI(&router.Router{}, c)
	defaultServer = httptest.NewServer(defaultAPI.Router)

	// Add test data
	url := defaultServer.URL + api.API_ROOT + "entities/" + entityType + "/"
	payload, err := json.Marshal(seedEntities)
	if err != nil {
		t.Fatal(err)
	}

	var ids []string
	statusCode, err := test.MakeHTTPRequest("POST", url, payload, &ids)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusOK {
		t.Errorf("Error inserting seed data. Status code= %d, expected %d", statusCode, http.StatusOK)
	}

	if len(ids) == 0 {
		t.Fatal("got zero new IDs")
	}
	seedId0 = ids[0]
}

func teardown(t *testing.T) {
	// Delete all data in DB/Collection (empty query matches everything).
	q, err := query.Translate("")
	if err != nil {
		t.Fatal(err)
	}

	// Must make call through dbconnector as API does not support empty queries
	// as a safety guard
	_, err = defaultAPI.DbConnector.DeleteEntities(entityType, q)
	if err != nil {
		if _, ok := err.(db.ErrDelete); ok {
			t.Fatalf("Error deleting entities: %s", err)
		} else {
			t.Fatalf("Uknown error when deleting entities: %s", err)
		}
	}

	// Close all connections
	defaultServer.CloseClientConnections()
	defaultServer.Close()
	defaultAPI.DbConnector.Disconnect()
}

// //////////////////////////////////////////////////////////////////////////
// Single Entity Management Handler Tests
// //////////////////////////////////////////////////////////////////////////

func TestPostEntityHandlerSuccessful(t *testing.T) {
	setup(t)
	defer teardown(t)

	entity := db.Entity{"x": 3.0}

	url := defaultServer.URL + api.API_ROOT + "entity/" + entityType
	payload, err := json.Marshal(entity)
	if err != nil {
		t.Fatal(err)
	}

	var id string
	statusCode, err := test.MakeHTTPRequest("POST", url, payload, &id)
	if err != nil {
		t.Fatal(err)
	}

	if id == "" {
		t.Error("no return entity ID, expected one")
	}

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}
}

func TestPostEntityHandlerPayloadError(t *testing.T) {
	setup(t)
	defer teardown(t)

	url := defaultServer.URL + api.API_ROOT + "entity/" + entityType
	// db.Entity type is expected to be in the payload, so passing in an empty
	// payload will trigger an error.
	var payload []byte
	var respErr map[string]string

	statusCode, err := test.MakeHTTPRequest("POST", url, payload, &respErr)
	if err != nil {
		t.Fatal(err)
	}

	actual := respErr["message"]
	expect := "Can't decode request body (error: EOF)"

	if actual != expect {
		t.Errorf("response error message = %s, expected %s", actual, expect)
	}

	if statusCode != http.StatusInternalServerError {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusInternalServerError)
	}
}

func TestPostEntityHandlerInvalidValueTypeError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// arrays are not supported value types
	x := []int{0, 1, 2}
	entity := db.Entity{"x": x}

	url := defaultServer.URL + api.API_ROOT + "entity/" + entityType
	payload, err := json.Marshal(entity)
	if err != nil {
		t.Fatal(err)
	}
	var respErr map[string]string

	statusCode, err := test.MakeHTTPRequest("POST", url, payload, &respErr)
	if err != nil {
		t.Fatal(err)
	}

	actual := respErr["message"]
	expect := "Key (x) has value ([0 1 2]) with invalid type ([]interface {}). Type of value must be a string or int."

	if actual != expect {
		t.Errorf("response error message = %s, expected %s", actual, expect)
	}

	if statusCode != http.StatusBadRequest {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusBadRequest)
	}
}

func TestGetEntityHandlerSuccessful(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Copy seedEntity0
	expect := db.Entity{
		"_id":   seedId0,
		"_type": entityType,
		"_rev":  0,
	}
	for k, v := range seedEntity0 {
		expect[k] = v
	}

	url := defaultServer.URL + api.API_ROOT + "entity/" + entityType + "/" + seedId0
	var actual db.Entity

	statusCode, err := test.MakeHTTPRequest("GET", url, nil, &actual)
	if err != nil {
		t.Fatal(err)
	}

	api.ConvertFloat64ToInt(actual)

	if !reflect.DeepEqual(actual, expect) {
		t.Errorf("Actual: %v, Expect: %v", actual, expect)
	}

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}
}

func TestGetEntityHandlerMissingIDError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Omit ID from URL
	url := defaultServer.URL + api.API_ROOT + "entity/" + entityType + "/"
	expectErr := "Missing params"
	testBadRequestError(t, "GET", url, expectErr)
}

func TestGetEntityHandlerNotFoundError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// id that we have not inserted into db
	id := "59ee2d725669fcc51f62aaaa"

	url := defaultServer.URL + api.API_ROOT + "entity/" + entityType + "/" + id
	var respErr map[string]string

	statusCode, err := test.MakeHTTPRequest("GET", url, nil, &respErr)
	if err != nil {
		t.Fatal(err)
	}

	actual := respErr["message"]
	expect := fmt.Sprintf("No entity with id: %s", id)

	if actual != expect {
		t.Errorf("response error message = %s, expected %s", actual, expect)
	}

	if statusCode != http.StatusNotFound {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusNotFound)
	}
}

func TestPutEntityHandlerSuccessful(t *testing.T) {
	setup(t)
	defer teardown(t)

	id := seedId0
	update := db.Entity{"foo": "baz"}
	// We expect the previous values (i.e. rev=0):
	expect := db.Entity{"_id": seedId0, "_type": entityType, "_rev": float64(0), "foo": seedEntity0["foo"]}

	url := defaultServer.URL + api.API_ROOT + "entity/" + entityType + "/" + id
	payload, err := json.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	var actual db.Entity

	statusCode, err := test.MakeHTTPRequest("PUT", url, payload, &actual)
	if err != nil {
		t.Fatal(err)
	}

	if diffs := deep.Equal(actual, expect); diffs != nil {
		t.Error(diffs)
	}

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}
}

func TestPutEntityHandlerMissingIDError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Omit ID from URL
	url := defaultServer.URL + api.API_ROOT + "entity/" + entityType + "/"
	expectErr := "Missing params"
	testBadRequestError(t, "PUT", url, expectErr)
}

func TestPutEntityHandlerPayloadError(t *testing.T) {
	setup(t)
	defer teardown(t)

	id := seedId0

	url := defaultServer.URL + api.API_ROOT + "entity/" + entityType + "/" + id
	// db.Entity type is expected to be in the payload, so passing in an empty
	// payload will trigger an error.
	var payload []byte
	var respErr map[string]string

	statusCode, err := test.MakeHTTPRequest("PUT", url, payload, &respErr)
	if err != nil {
		t.Fatal(err)
	}

	actual := respErr["message"]
	expect := "Can't decode request body (error: EOF)"

	if actual != expect {
		t.Errorf("response error message = %s, expected %s", actual, expect)
	}

	if statusCode != http.StatusInternalServerError {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusInternalServerError)
	}
}

func TestDeleteEntityHandlerSuccessful(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Copy seedEntity0
	expect := db.Entity{
		"_id":   seedId0,
		"_type": entityType,
		"_rev":  0,
	}
	for k, v := range seedEntity0 {
		expect[k] = v
	}

	url := defaultServer.URL + api.API_ROOT + "entity/" + entityType + "/" + seedId0
	var actual db.Entity

	statusCode, err := test.MakeHTTPRequest("DELETE", url, nil, &actual)
	if err != nil {
		t.Fatal(err)
	}

	api.ConvertFloat64ToInt(actual)

	if !reflect.DeepEqual(actual, expect) {
		t.Errorf("Actual: %v, Expect: %v", actual, expect)
	}

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}
}

func TestDeleteEntityHandlerMissingIDError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Omit ID from URL
	url := defaultServer.URL + api.API_ROOT + "entity/" + entityType + "/"
	expectErr := "Missing params"
	testBadRequestError(t, "DELETE", url, expectErr)
}

// //////////////////////////////////////////////////////////////////////////
// Multiple Entity Management Handler Tests
// //////////////////////////////////////////////////////////////////////////

func TestPostEntitiesHandlerSuccessful(t *testing.T) {
	setup(t)
	defer teardown(t)

	// seedEntities are id{0,1,2}
	entity0 := db.Entity{"x": 3}
	entity1 := db.Entity{"x": 4}
	entities := []db.Entity{entity0, entity1}

	url := defaultServer.URL + api.API_ROOT + "entities/" + entityType
	payload, err := json.Marshal(entities)
	if err != nil {
		t.Fatal(err)
	}
	var actual []string

	statusCode, err := test.MakeHTTPRequest("POST", url, payload, &actual)
	if err != nil {
		t.Fatal(err)
	}

	if len(actual) != 2 {
		t.Errorf("got %d ids, expected 2", len(actual))
	}
	for _, id := range actual {
		if !bson.IsObjectIdHex(id) {
			t.Errorf("got invalid id: %s", id)
		}
	}

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}
}

func TestPostEntitiesHandlerPayloadError(t *testing.T) {
	setup(t)
	defer teardown(t)

	url := defaultServer.URL + api.API_ROOT + "entities/" + entityType
	// db.Entity type is expected to be in the payload, so passing in an empty
	// payload will trigger an error.
	var payload []byte
	var respErr map[string]string

	statusCode, err := test.MakeHTTPRequest("POST", url, payload, &respErr)
	if err != nil {
		t.Fatal(err)
	}

	actual := respErr["message"]
	expect := "Can't decode request body (error: EOF)"

	if actual != expect {
		t.Errorf("response error message = %s, expected %s", actual, expect)
	}

	if statusCode != http.StatusInternalServerError {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusInternalServerError)
	}
}

func TestPostEntitiesHandlerInvalidValueTypeError(t *testing.T) {
	setup(t)
	defer teardown(t)

	yArr := []string{"foo", "bar", "baz"}

	entity2 := db.Entity{"x": 2}
	// Arrays are not supported values types
	entity3 := db.Entity{"y": yArr}
	entities := []db.Entity{entity2, entity3}

	url := defaultServer.URL + api.API_ROOT + "entities/" + entityType
	payload, err := json.Marshal(entities)
	if err != nil {
		t.Fatal(err)
	}
	var respErr map[string]string

	statusCode, err := test.MakeHTTPRequest("POST", url, payload, &respErr)
	if err != nil {
		t.Fatal(err)
	}

	actual := respErr["message"]
	expect := "Key (y) has value ([foo bar baz]) with invalid type ([]interface {}). Type of value must be a string or int."

	if actual != expect {
		t.Errorf("response error message = %s, expected %s", actual, expect)
	}

	if statusCode != http.StatusBadRequest {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusBadRequest)
	}
}

func TestGetEntitiesHandlerSuccessful(t *testing.T) {
	setup(t)
	defer teardown(t)

	expect := seedEntities

	query := url.QueryEscape("foo=bar")
	url := defaultServer.URL + api.API_ROOT + "entities/" + entityType + "?query=" + query
	var actual []db.Entity

	statusCode, err := test.MakeHTTPRequest("GET", url, nil, &actual)
	if err != nil {
		t.Fatal(err)
	}

	for _, e := range actual {
		api.ConvertFloat64ToInt(e)
		delete(e, "_id")
		delete(e, "_rev")
		delete(e, "_type")
	}

	if !reflect.DeepEqual(actual, expect) {
		t.Errorf("Actual: %v, Expect: %v", actual, expect)
	}

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}
}

func TestGetEntitiesHandlerMissingQueryError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Omit query param from URL
	url := defaultServer.URL + api.API_ROOT + "entities/" + entityType + "?"
	expectErr := "Missing param: query"
	testBadRequestError(t, "GET", url, expectErr)
}

func TestGetEntitiesHandlerEmptyQueryError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Omit query string from URL
	url := defaultServer.URL + api.API_ROOT + "entities/" + entityType + "?query"

	expectErr := "Missing param: query string is empty"
	testBadRequestError(t, "GET", url, expectErr)
}

func TestGetEntitiesHandlerNotFoundError(t *testing.T) {
	setup(t)
	defer teardown(t)

	labelSelector := "x=9999"
	query := url.QueryEscape(labelSelector)
	url := defaultServer.URL + api.API_ROOT + "entities/" + entityType + "?query=" + query

	var actual []db.Entity
	statusCode, err := test.MakeHTTPRequest("GET", url, nil, &actual)
	if err != nil {
		t.Fatal(err)
	}
	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}

	if len(actual) != 0 {
		t.Errorf("got response, expected empty slice: %v", actual)
	}
}

func TestPutEntitiesHandlerSuccessful(t *testing.T) {
	setup(t)
	defer teardown(t)

	expect := []db.Entity{
		db.Entity{"_type": entityType, "_rev": float64(0), "foo": seedEntity1["foo"]},
		db.Entity{"_type": entityType, "_rev": float64(0), "foo": seedEntity2["foo"]},
	}

	query := url.QueryEscape("x>0")
	update := db.Entity{"foo": "baz"}

	url := defaultServer.URL + api.API_ROOT + "entities/" + entityType + "?query=" + query
	payload, err := json.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	var actual []db.Entity

	statusCode, err := test.MakeHTTPRequest("PUT", url, payload, &actual)
	if err != nil {
		t.Fatal(err)
	}

	// Don't care about the ids
	for _, e := range actual {
		delete(e, "_id")
	}

	if !reflect.DeepEqual(actual, expect) {
		t.Errorf("Actual: %v, Expect: %v", actual, expect)
	}

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}
}

func TestPutEntitiesHandlerMissingQueryError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Omit query param from URL
	url := defaultServer.URL + api.API_ROOT + "entities/" + entityType + "?"
	expectErr := "Missing param: query"
	testBadRequestError(t, "PUT", url, expectErr)
}

func TestPutEntitiesHandlerEmptyQueryError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Omit query string from URL
	url := defaultServer.URL + api.API_ROOT + "entities/" + entityType + "?query"
	expectErr := "Missing param: query string is empty"
	testBadRequestError(t, "PUT", url, expectErr)
}

func TestPutEntitiesHandlerPayloadError(t *testing.T) {
	setup(t)
	defer teardown(t)

	query := url.QueryEscape("x>0")

	url := defaultServer.URL + api.API_ROOT + "entities/" + entityType + "?query=" + query
	// db.Entity type is expected to be in the payload, so passing in an empty
	// payload will trigger an error.
	var payload []byte
	var respErr map[string]string

	statusCode, err := test.MakeHTTPRequest("PUT", url, payload, &respErr)
	if err != nil {
		t.Fatal(err)
	}

	actual := respErr["message"]
	expect := "Can't decode request body (error: EOF)"

	if actual != expect {
		t.Errorf("response error message = %s, expected %s", actual, expect)
	}

	if statusCode != http.StatusInternalServerError {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusInternalServerError)
	}
}

func TestDeleteEntitiesHandlerSuccessful(t *testing.T) {
	setup(t)
	defer teardown(t)

	expect := seedEntities
	query := url.QueryEscape("foo=bar")

	url := defaultServer.URL + api.API_ROOT + "entities/" + entityType + "?query=" + query
	var actual []db.Entity

	statusCode, err := test.MakeHTTPRequest("DELETE", url, nil, &actual)
	if err != nil {
		t.Fatal(err)
	}

	for _, e := range actual {
		api.ConvertFloat64ToInt(e)
		delete(e, "_id")
		delete(e, "_rev")
		delete(e, "_type")
	}

	if !reflect.DeepEqual(actual, expect) {
		t.Errorf("Actual: %v, Expect: %v", actual, expect)
	}

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}
}

func TestDeleteEntitiesHandlerMissingQueryError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Omit query param from URL
	url := defaultServer.URL + api.API_ROOT + "entities/" + entityType + "?"
	expectErr := "Missing param: query"
	testBadRequestError(t, "DELETE", url, expectErr)
}

func TestDeleteEntitiesHandlerEmptyQueryError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Omit query string from URL
	url := defaultServer.URL + api.API_ROOT + "entities/" + entityType + "?query"
	expectErr := "Missing param: query string is empty"
	testBadRequestError(t, "DELETE", url, expectErr)
}

////////////////////////////////////////////////////////////////////////////
// Helper Functions
////////////////////////////////////////////////////////////////////////////

func testBadRequestError(t *testing.T, httpVerb string, url string, expectErr string) {
	var respErr map[string]string

	statusCode, err := test.MakeHTTPRequest(httpVerb, url, nil, &respErr)
	if err != nil {
		t.Fatal(err)
	}

	actual := respErr["message"]
	expect := expectErr

	if actual != expect {
		t.Errorf("response error message = %s, expected %s", actual, expect)
	}

	if statusCode != http.StatusBadRequest {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusBadRequest)
	}
}
