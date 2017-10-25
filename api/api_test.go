// Copyright 2017, Square, Inc.

package api_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/square/etre"
	"github.com/square/etre/api"
	"github.com/square/etre/query"
	"github.com/square/etre/router"
	"github.com/square/etre/test"
	"github.com/square/etre/test/mock"

	"github.com/go-test/deep"
)

var seedEntity0 = etre.Entity{"_id": "id0", "x": 0, "foo": "bar"}
var seedEntity1 = etre.Entity{"_id": "id1", "x": 1, "foo": "bar"}
var seedEntity2 = etre.Entity{"_id": "id2", "x": 2, "foo": "bar"}
var seedEntities = []etre.Entity{seedEntity0, seedEntity1, seedEntity2}
var entityType = "nodes"

// //////////////////////////////////////////////////////////////////////////
// Single Entity Management Handler Tests
// //////////////////////////////////////////////////////////////////////////

func TestPostEntityHandlerSuccessful(t *testing.T) {
	es := &mock.EntityStore{
		CreateEntitiesFunc: func(entityType string, entities []etre.Entity, username string) ([]string, error) {
			for _, entity := range entities {
				if entity["_id"].(string) == "id3" {
					return []string{"id3"}, nil
				}
			}
			return []string{}, nil
		},
	}
	defaultAPI := api.NewAPI(&router.Router{}, es, &mock.FeedFactory{})
	defaultServer := httptest.NewServer(defaultAPI.Router)

	expect := "id3"
	entity := etre.Entity{"_id": expect, "x": 3.0}

	url := defaultServer.URL + api.API_ROOT + "entity/" + entityType
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
	defaultAPI := api.NewAPI(&router.Router{}, &mock.EntityStore{}, &mock.FeedFactory{})
	defaultServer := httptest.NewServer(defaultAPI.Router)

	url := defaultServer.URL + api.API_ROOT + "entity/" + entityType
	// etre.Entity type is expected to be in the payload, so passing in an empty
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
	defaultAPI := api.NewAPI(&router.Router{}, &mock.EntityStore{}, &mock.FeedFactory{})
	defaultServer := httptest.NewServer(defaultAPI.Router)

	// arrays are not supported value types
	x := []int{0, 1, 2}
	entity := etre.Entity{"_id": "id3", "x": x}

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
	var actualQuery query.Query
	es := &mock.EntityStore{
		ReadEntitiesFunc: func(entityType string, q query.Query) ([]etre.Entity, error) {
			actualQuery = q
			return []etre.Entity{seedEntity0}, nil
		},
	}
	defaultAPI := api.NewAPI(&router.Router{}, es, &mock.FeedFactory{})
	defaultServer := httptest.NewServer(defaultAPI.Router)

	expect := seedEntity0
	id := expect["_id"].(string)

	url := defaultServer.URL + api.API_ROOT + "entity/" + entityType + "/" + id
	var actual etre.Entity

	statusCode, err := test.MakeHTTPRequest("GET", url, nil, &actual)
	if err != nil {
		t.Fatal(err)
	}

	api.ConvertFloat64ToInt(actual)

	if diff := deep.Equal(actual, expect); diff != nil {
		t.Error(diff)
	}

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}

	expectedQuery := query.Query{
		[]query.Predicate{
			query.Predicate{
				Label:    "_id",
				Operator: "=",
				Value:    seedEntity0["_id"].(string),
			},
		},
	}
	if diff := deep.Equal(actualQuery, expectedQuery); diff != nil {
		t.Error(diff)
	}
}

func TestGetEntityHandlerMissingIDError(t *testing.T) {
	defaultAPI := api.NewAPI(&router.Router{}, &mock.EntityStore{}, &mock.FeedFactory{})
	defaultServer := httptest.NewServer(defaultAPI.Router)

	// Omit ID from URL
	url := defaultServer.URL + api.API_ROOT + "entity/" + entityType + "/"
	expectErr := "Missing params"
	testBadRequestError(t, "GET", url, expectErr)
}

func TestGetEntityHandlerNotFoundError(t *testing.T) {
	es := &mock.EntityStore{
		ReadEntitiesFunc: func(entityType string, q query.Query) ([]etre.Entity, error) {
			return nil, nil
		},
	}
	defaultAPI := api.NewAPI(&router.Router{}, es, &mock.FeedFactory{})
	defaultServer := httptest.NewServer(defaultAPI.Router)

	// id that we have not inserted into db
	id := "id4"

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
	es := &mock.EntityStore{
		UpdateEntitiesFunc: func(t string, q query.Query, u etre.Entity, user string) ([]etre.Entity, error) {
			return []etre.Entity{etre.Entity{"_id": seedEntity0["_id"], "foo": seedEntity0["foo"]}}, nil
		},
	}
	defaultAPI := api.NewAPI(&router.Router{}, es, &mock.FeedFactory{})
	defaultServer := httptest.NewServer(defaultAPI.Router)

	id := seedEntity0["_id"].(string)
	update := etre.Entity{"foo": "baz"}
	expect := etre.Entity{"_id": seedEntity0["_id"], "foo": seedEntity0["foo"]}

	url := defaultServer.URL + api.API_ROOT + "entity/" + entityType + "/" + id
	payload, err := json.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	var actual etre.Entity

	statusCode, err := test.MakeHTTPRequest("PUT", url, payload, &actual)
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

func TestPutEntityHandlerMissingIDError(t *testing.T) {
	defaultAPI := api.NewAPI(&router.Router{}, &mock.EntityStore{}, &mock.FeedFactory{})
	defaultServer := httptest.NewServer(defaultAPI.Router)

	// Omit ID from URL
	url := defaultServer.URL + api.API_ROOT + "entity/" + entityType + "/"
	expectErr := "Missing params"
	testBadRequestError(t, "PUT", url, expectErr)
}

func TestPutEntityHandlerPayloadError(t *testing.T) {
	defaultAPI := api.NewAPI(&router.Router{}, &mock.EntityStore{}, &mock.FeedFactory{})
	defaultServer := httptest.NewServer(defaultAPI.Router)

	id := seedEntity0["_id"].(string)

	url := defaultServer.URL + api.API_ROOT + "entity/" + entityType + "/" + id
	// etre.Entity type is expected to be in the payload, so passing in an empty
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
	es := &mock.EntityStore{
		DeleteEntitiesFunc: func(t string, q query.Query, user string) ([]etre.Entity, error) {
			return []etre.Entity{seedEntity0}, nil
		},
	}
	defaultAPI := api.NewAPI(&router.Router{}, es, &mock.FeedFactory{})
	defaultServer := httptest.NewServer(defaultAPI.Router)

	expect := seedEntity0
	id := expect["_id"].(string)

	url := defaultServer.URL + api.API_ROOT + "entity/" + entityType + "/" + id
	var actual etre.Entity

	statusCode, err := test.MakeHTTPRequest("DELETE", url, nil, &actual)
	if err != nil {
		t.Fatal(err)
	}

	api.ConvertFloat64ToInt(actual)

	if diff := deep.Equal(actual, expect); diff != nil {
		t.Error(diff)
	}

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}
}

func TestDeleteEntityHandlerMissingIDError(t *testing.T) {
	defaultAPI := api.NewAPI(&router.Router{}, &mock.EntityStore{}, &mock.FeedFactory{})
	defaultServer := httptest.NewServer(defaultAPI.Router)

	// Omit ID from URL
	url := defaultServer.URL + api.API_ROOT + "entity/" + entityType + "/"
	expectErr := "Missing params"
	testBadRequestError(t, "DELETE", url, expectErr)
}

// //////////////////////////////////////////////////////////////////////////
// Multiple Entity Management Handler Tests
// //////////////////////////////////////////////////////////////////////////

func TestPostEntitiesHandlerSuccessful(t *testing.T) {
	es := &mock.EntityStore{
		CreateEntitiesFunc: func(entityType string, entities []etre.Entity, username string) ([]string, error) {
			var r1Found bool
			var r2Found bool
			for _, entity := range entities {
				if entity["_id"].(string) == "id3" {
					r1Found = true
				}
				if entity["_id"].(string) == "id4" {
					r2Found = true
				}
			}
			if r1Found && r2Found {
				return []string{"id3", "id4"}, nil
			}
			return []string{}, nil
		},
	}
	defaultAPI := api.NewAPI(&router.Router{}, es, &mock.FeedFactory{})
	defaultServer := httptest.NewServer(defaultAPI.Router)

	// seedEntities are id{0,1,2}
	expect := []string{"id3", "id4"}
	entity0 := etre.Entity{"_id": expect[0], "x": 3}
	entity1 := etre.Entity{"_id": expect[1], "x": 4}
	entities := []etre.Entity{entity0, entity1}

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

	if diff := deep.Equal(actual, expect); diff != nil {
		t.Error(diff)
	}
	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}
}

func TestPostEntitiesHandlerPayloadError(t *testing.T) {
	defaultAPI := api.NewAPI(&router.Router{}, &mock.EntityStore{}, &mock.FeedFactory{})
	defaultServer := httptest.NewServer(defaultAPI.Router)

	url := defaultServer.URL + api.API_ROOT + "entities/" + entityType
	// etre.Entity type is expected to be in the payload, so passing in an empty
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
	defaultAPI := api.NewAPI(&router.Router{}, &mock.EntityStore{}, &mock.FeedFactory{})
	defaultServer := httptest.NewServer(defaultAPI.Router)

	yArr := []string{"foo", "bar", "baz"}

	entity2 := etre.Entity{"_id": "id3", "x": 2}
	// Arrays are not supported values types
	entity3 := etre.Entity{"_id": "id4", "y": yArr}
	entities := []etre.Entity{entity2, entity3}

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
	var actualQuery query.Query
	es := &mock.EntityStore{
		ReadEntitiesFunc: func(entityType string, q query.Query) ([]etre.Entity, error) {
			actualQuery = q
			return seedEntities, nil
		},
	}
	defaultAPI := api.NewAPI(&router.Router{}, es, &mock.FeedFactory{})
	defaultServer := httptest.NewServer(defaultAPI.Router)

	expect := seedEntities

	q := url.QueryEscape("foo=bar")
	url := defaultServer.URL + api.API_ROOT + "entities/" + entityType + "?query=" + q
	var actual []etre.Entity

	statusCode, err := test.MakeHTTPRequest("GET", url, nil, &actual)
	if err != nil {
		t.Fatal(err)
	}

	for _, e := range actual {
		api.ConvertFloat64ToInt(e)
	}

	if diff := deep.Equal(actual, expect); diff != nil {
		t.Error(diff)
	}

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}

	expectedQuery := query.Query{
		[]query.Predicate{
			query.Predicate{
				Label:    "foo",
				Operator: "=",
				Value:    "bar",
			},
		},
	}
	if diff := deep.Equal(actualQuery, expectedQuery); diff != nil {
		t.Error(diff)
	}
}

func TestGetEntitiesHandlerMissingQueryError(t *testing.T) {
	defaultAPI := api.NewAPI(&router.Router{}, &mock.EntityStore{}, &mock.FeedFactory{})
	defaultServer := httptest.NewServer(defaultAPI.Router)

	// Omit query param from URL
	url := defaultServer.URL + api.API_ROOT + "entities/" + entityType + "?"
	expectErr := "Missing param: query"
	testBadRequestError(t, "GET", url, expectErr)
}

func TestGetEntitiesHandleresptyQueryError(t *testing.T) {
	defaultAPI := api.NewAPI(&router.Router{}, &mock.EntityStore{}, &mock.FeedFactory{})
	defaultServer := httptest.NewServer(defaultAPI.Router)

	// Omit query string from URL
	url := defaultServer.URL + api.API_ROOT + "entities/" + entityType + "?query"

	expectErr := "Missing param: query string is empty"
	testBadRequestError(t, "GET", url, expectErr)
}

func TestGetEntitiesHandlerNotFoundError(t *testing.T) {
	es := &mock.EntityStore{
		ReadEntitiesFunc: func(entityType string, q query.Query) ([]etre.Entity, error) {
			return nil, nil
		},
	}
	defaultAPI := api.NewAPI(&router.Router{}, es, &mock.FeedFactory{})
	defaultServer := httptest.NewServer(defaultAPI.Router)

	labelSelector := "x=9999"
	query := url.QueryEscape(labelSelector)
	url := defaultServer.URL + api.API_ROOT + "entities/" + entityType + "?query=" + query
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
	expect := []etre.Entity{
		etre.Entity{"_id": seedEntity1["_id"], "foo": seedEntity1["foo"]},
		etre.Entity{"_id": seedEntity2["_id"], "foo": seedEntity2["foo"]},
	}

	es := &mock.EntityStore{
		UpdateEntitiesFunc: func(t string, q query.Query, u etre.Entity, user string) ([]etre.Entity, error) {
			return expect, nil
		},
	}
	defaultAPI := api.NewAPI(&router.Router{}, es, &mock.FeedFactory{})
	defaultServer := httptest.NewServer(defaultAPI.Router)

	query := url.QueryEscape("x>0")
	update := etre.Entity{"foo": "baz"}

	url := defaultServer.URL + api.API_ROOT + "entities/" + entityType + "?query=" + query
	payload, err := json.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	var actual []etre.Entity

	statusCode, err := test.MakeHTTPRequest("PUT", url, payload, &actual)
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

func TestPutEntitiesHandlerMissingQueryError(t *testing.T) {
	defaultAPI := api.NewAPI(&router.Router{}, &mock.EntityStore{}, &mock.FeedFactory{})
	defaultServer := httptest.NewServer(defaultAPI.Router)

	// Omit query param from URL
	url := defaultServer.URL + api.API_ROOT + "entities/" + entityType + "?"
	expectErr := "Missing param: query"
	testBadRequestError(t, "PUT", url, expectErr)
}

func TestPutEntitiesHandlerEmptyQueryError(t *testing.T) {
	defaultAPI := api.NewAPI(&router.Router{}, &mock.EntityStore{}, &mock.FeedFactory{})
	defaultServer := httptest.NewServer(defaultAPI.Router)

	// Omit query string from URL
	url := defaultServer.URL + api.API_ROOT + "entities/" + entityType + "?query"
	expectErr := "Missing param: query string is empty"
	testBadRequestError(t, "PUT", url, expectErr)
}

func TestPutEntitiesHandlerPayloadError(t *testing.T) {
	defaultAPI := api.NewAPI(&router.Router{}, &mock.EntityStore{}, &mock.FeedFactory{})
	defaultServer := httptest.NewServer(defaultAPI.Router)

	query := url.QueryEscape("x>0")

	url := defaultServer.URL + api.API_ROOT + "entities/" + entityType + "?query=" + query
	// etre.Entity type is expected to be in the payload, so passing in an empty
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
	es := &mock.EntityStore{
		DeleteEntitiesFunc: func(t string, q query.Query, user string) ([]etre.Entity, error) {
			return seedEntities, nil
		},
	}
	defaultAPI := api.NewAPI(&router.Router{}, es, &mock.FeedFactory{})
	defaultServer := httptest.NewServer(defaultAPI.Router)

	expect := seedEntities
	query := url.QueryEscape("foo=bar")

	url := defaultServer.URL + api.API_ROOT + "entities/" + entityType + "?query=" + query
	var actual []etre.Entity

	statusCode, err := test.MakeHTTPRequest("DELETE", url, nil, &actual)
	if err != nil {
		t.Fatal(err)
	}

	for _, e := range actual {
		api.ConvertFloat64ToInt(e)
	}

	if diff := deep.Equal(actual, expect); diff != nil {
		t.Error(diff)
	}

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}
}

func TestDeleteEntitiesHandlerMissingQueryError(t *testing.T) {
	defaultAPI := api.NewAPI(&router.Router{}, &mock.EntityStore{}, &mock.FeedFactory{})
	defaultServer := httptest.NewServer(defaultAPI.Router)

	// Omit query param from URL
	url := defaultServer.URL + api.API_ROOT + "entities/" + entityType + "?"
	expectErr := "Missing param: query"
	testBadRequestError(t, "DELETE", url, expectErr)
}

func TestDeleteEntitiesHandleresptyQueryError(t *testing.T) {
	defaultAPI := api.NewAPI(&router.Router{}, &mock.EntityStore{}, &mock.FeedFactory{})
	defaultServer := httptest.NewServer(defaultAPI.Router)

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
