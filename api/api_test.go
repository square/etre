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
)

var defaultAPI *api.API
var defaultServer *httptest.Server
var seedEntity0 = db.Entity{"_id": "id0", "x": 0, "foo": "bar"}
var seedEntity1 = db.Entity{"_id": "id1", "x": 1, "foo": "bar"}
var seedEntity2 = db.Entity{"_id": "id2", "x": 2, "foo": "bar"}
var seedEntities = []db.Entity{seedEntity0, seedEntity1, seedEntity2}

func setup(t *testing.T) {
	c := db.NewConnector("localhost", "etre", "entities", 5, nil)
	err := c.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defaultAPI = api.NewAPI(&router.Router{}, c)
	defaultServer = httptest.NewServer(defaultAPI.Router)

	// Add test data
	url := defaultServer.URL + api.API_ROOT + "entities"
	payload, err := json.Marshal(seedEntities)
	if err != nil {
		t.Fatal(err)
	}

	statusCode, err := test.MakeHTTPRequest("POST", url, payload, nil)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusOK {
		t.Errorf("Error inserting seed data. Status code= %d, expected %d", statusCode, http.StatusOK)
	}
}

func teardown(t *testing.T) {
	// Delete all data in DB/Collection (empty query matches everything).
	q, err := query.Translate("")
	if err != nil {
		t.Fatal(err)
	}

	// Must make call through dbconnector as API does not support empty queries
	// as a safety guard
	_, err = defaultAPI.DbConnector.DeleteEntities(q)
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

	expect := "id3"
	entity := db.Entity{"_id": expect, "x": 3.0}

	url := defaultServer.URL + api.API_ROOT + "entity"
	payload, err := json.Marshal(entity)
	if err != nil {
		t.Fatal(err)
	}
	var actual string

	statusCode, err := test.MakeHTTPRequest("POST", url, payload, &actual)
	if err != nil {
		t.Fatal(err)
	}

	if diff := deep.Equal(actual, expect); diff != nil {
		t.Error(diff)
	}

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}
}

func TestPostEntityHandlerPayloadError(t *testing.T) {
	setup(t)
	defer teardown(t)

	url := defaultServer.URL + api.API_ROOT + "entity"
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
	entity := db.Entity{"_id": "id3", "x": x}

	url := defaultServer.URL + api.API_ROOT + "entity"
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

	expect := seedEntity0
	id := expect["_id"].(string)

	url := defaultServer.URL + api.API_ROOT + "entity/" + id
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
	url := defaultServer.URL + api.API_ROOT + "entity/"
	expectErr := "Missing param: id"
	testBadRequestError(t, "GET", url, expectErr)
}

func TestGetEntityHandlerNotFoundError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// id that we have not inserted into db
	id := "id4"

	url := defaultServer.URL + api.API_ROOT + "entity/" + id
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

	id := seedEntity0["_id"].(string)
	update := db.Entity{"foo": "baz"}
	expect := db.Entity{"_id": seedEntity0["_id"], "foo": seedEntity0["foo"]}

	url := defaultServer.URL + api.API_ROOT + "entity/" + id
	payload, err := json.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	var actual db.Entity

	statusCode, err := test.MakeHTTPRequest("PUT", url, payload, &actual)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(actual, expect) {
		t.Errorf("Actual: %v, Expect: %v", actual, expect)
	}

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}
}

func TestPutEntityHandlerMissingIDError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Omit ID from URL
	url := defaultServer.URL + api.API_ROOT + "entity/"
	expectErr := "Missing param: id"
	testBadRequestError(t, "PUT", url, expectErr)
}

func TestPutEntityHandlerPayloadError(t *testing.T) {
	setup(t)
	defer teardown(t)

	id := seedEntity0["_id"].(string)

	url := defaultServer.URL + api.API_ROOT + "entity/" + id
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

	expect := seedEntity0
	id := expect["_id"].(string)

	url := defaultServer.URL + api.API_ROOT + "entity/" + id
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
	url := defaultServer.URL + api.API_ROOT + "entity/"
	expectErr := "Missing param: id"
	testBadRequestError(t, "DELETE", url, expectErr)
}

// //////////////////////////////////////////////////////////////////////////
// Multiple Entity Management Handler Tests
// //////////////////////////////////////////////////////////////////////////

func TestPostEntitiesHandlerSuccessful(t *testing.T) {
	setup(t)
	defer teardown(t)

	// seedEntities are id{0,1,2}
	expect := []string{"id3", "id4"}
	entity0 := db.Entity{"_id": expect[0], "x": 3}
	entity1 := db.Entity{"_id": expect[1], "x": 4}
	entities := []db.Entity{entity0, entity1}

	url := defaultServer.URL + api.API_ROOT + "entities"
	payload, err := json.Marshal(entities)
	if err != nil {
		t.Fatal(err)
	}
	var actual []string

	statusCode, err := test.MakeHTTPRequest("POST", url, payload, &actual)
	if err != nil {
		t.Fatal(err)
	}

	if diff := deep.Equal(actual, expect); diff != nil {
		t.Error(diff)
	}

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}
}

func TestPostEntitiesHandlerPayloadError(t *testing.T) {
	setup(t)
	defer teardown(t)

	url := defaultServer.URL + api.API_ROOT + "entities"
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

	entity2 := db.Entity{"_id": "id3", "x": 2}
	// Arrays are not supported values types
	entity3 := db.Entity{"_id": "id4", "y": yArr}
	entities := []db.Entity{entity2, entity3}

	url := defaultServer.URL + api.API_ROOT + "entities"
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
	url := defaultServer.URL + api.API_ROOT + "entities?query=" + query
	var actual []db.Entity

	statusCode, err := test.MakeHTTPRequest("GET", url, nil, &actual)
	if err != nil {
		t.Fatal(err)
	}

	for _, e := range actual {
		api.ConvertFloat64ToInt(e)
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
	url := defaultServer.URL + api.API_ROOT + "entities?"
	expectErr := "Missing param: query"
	testBadRequestError(t, "GET", url, expectErr)
}

func TestGetEntitiesHandlerEmptyQueryError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Omit query string from URL
	url := defaultServer.URL + api.API_ROOT + "entities?query"
	expectErr := "Missing param: query string is empty"
	testBadRequestError(t, "GET", url, expectErr)
}

func TestGetEntitiesHandlerNotFoundError(t *testing.T) {
	setup(t)
	defer teardown(t)

	labelSelector := "x=9999"
	query := url.QueryEscape(labelSelector)
	url := defaultServer.URL + api.API_ROOT + "entities?query=" + query
	var respErr map[string]string

	statusCode, err := test.MakeHTTPRequest("GET", url, nil, &respErr)
	if err != nil {
		t.Fatal(err)
	}

	actual := respErr["message"]
	expect := fmt.Sprintf("No entities match query: %s", labelSelector)

	if actual != expect {
		t.Errorf("response error message = %s, expected %s", actual, expect)
	}

	if statusCode != http.StatusNotFound {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusNotFound)
	}
}

func TestPutEntitiesHandlerSuccessful(t *testing.T) {
	setup(t)
	defer teardown(t)

	expect := []db.Entity{
		db.Entity{"_id": seedEntity1["_id"], "foo": seedEntity1["foo"]},
		db.Entity{"_id": seedEntity2["_id"], "foo": seedEntity2["foo"]},
	}

	query := url.QueryEscape("x>0")
	update := db.Entity{"foo": "baz"}

	url := defaultServer.URL + api.API_ROOT + "entities?query=" + query
	payload, err := json.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	var actual []db.Entity

	statusCode, err := test.MakeHTTPRequest("PUT", url, payload, &actual)
	if err != nil {
		t.Fatal(err)
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
	url := defaultServer.URL + api.API_ROOT + "entities?"
	expectErr := "Missing param: query"
	testBadRequestError(t, "PUT", url, expectErr)
}

func TestPutEntitiesHandlerEmptyQueryError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Omit query string from URL
	url := defaultServer.URL + api.API_ROOT + "entities?query"
	expectErr := "Missing param: query string is empty"
	testBadRequestError(t, "PUT", url, expectErr)
}

func TestPutEntitiesHandlerPayloadError(t *testing.T) {
	setup(t)
	defer teardown(t)

	query := url.QueryEscape("x>0")

	url := defaultServer.URL + api.API_ROOT + "entities?query=" + query
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

	url := defaultServer.URL + api.API_ROOT + "entities?query=" + query
	var actual []db.Entity

	statusCode, err := test.MakeHTTPRequest("DELETE", url, nil, &actual)
	if err != nil {
		t.Fatal(err)
	}

	for _, e := range actual {
		api.ConvertFloat64ToInt(e)
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
	url := defaultServer.URL + api.API_ROOT + "entities?"
	expectErr := "Missing param: query"
	testBadRequestError(t, "DELETE", url, expectErr)
}

func TestDeleteEntitiesHandlerEmptyQueryError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Omit query string from URL
	url := defaultServer.URL + api.API_ROOT + "entities?query"
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
