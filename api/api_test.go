// Copyright 2017, Square, Inc.

package api_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	"github.com/square/etre"
	"github.com/square/etre/api"
	"github.com/square/etre/entity"
	"github.com/square/etre/query"
	"github.com/square/etre/test"
	"github.com/square/etre/test/mock"

	"github.com/go-test/deep"
	"gopkg.in/mgo.v2/bson"
)

var (
	createIds      []string
	updateEntities []etre.Entity
	deleteEntities []etre.Entity

	readErr   error
	createErr error
	updateErr error
	deleteErr error

	seedId0, seedId1                      string
	seedEntity0, seedEntity1, seedEntity2 etre.Entity
	seedEntities                          []etre.Entity
)

var addr = "http://localhost"
var entityType = "nodes"

var es *mock.EntityStore
var defaultServer *httptest.Server
var mu = &sync.Mutex{}

func setup(t *testing.T) {
	if defaultServer == nil {
		es := &mock.EntityStore{
			CreateEntitiesFunc: func(entity.WriteOp, []etre.Entity) ([]string, error) {
				return createIds, createErr
			},
			ReadEntitiesFunc: func(string, query.Query, etre.QueryFilter) ([]etre.Entity, error) {
				return seedEntities, readErr
			},
			UpdateEntitiesFunc: func(entity.WriteOp, query.Query, etre.Entity) ([]etre.Entity, error) {
				return updateEntities, updateErr
			},
			DeleteEntitiesFunc: func(entity.WriteOp, query.Query) ([]etre.Entity, error) {
				return deleteEntities, deleteErr
			},
		}
		defaultAPI := api.NewAPI(addr, es, &mock.FeedFactory{})
		defaultServer = httptest.NewServer(defaultAPI)
		t.Logf("started test HTTP server: %s\n", defaultServer.URL)
	}

	createIds = nil
	updateEntities = nil
	deleteEntities = nil

	readErr = nil
	createErr = nil
	updateErr = nil
	deleteErr = nil

	seedEntity0 = etre.Entity{"x": 0, "foo": "bar"}
	seedEntity1 = etre.Entity{"x": 1, "foo": "bar"}
	seedEntity2 = etre.Entity{"x": 2, "foo": "bar"}
	seedEntities = []etre.Entity{seedEntity0, seedEntity1, seedEntity2}
	seedId0 = "59f10d2a5669fc79103a0000"
	seedId1 = "59f10d2a5669fc7910310001"
}

func teardown(t *testing.T) {
}

// //////////////////////////////////////////////////////////////////////////
// Single Entity Management Handler Tests
// //////////////////////////////////////////////////////////////////////////

func TestPostEntityHandlerSuccessful(t *testing.T) {
	setup(t)
	defer teardown(t)

	createIds = []string{"id1"}

	entity := etre.Entity{"x": 3.0}
	payload, err := json.Marshal(entity)
	if err != nil {
		t.Fatal(err)
	}

	url := defaultServer.URL + etre.API_ROOT + "/entity/" + entityType
	var actual etre.WriteResult
	statusCode, err := test.MakeHTTPRequest("POST", url, payload, &actual)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusCreated {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}

	if actual.Id != "id1" {
		t.Error("WriteResult.Id = %s, expected id1", actual.Id)
	}
	expect := etre.WriteResult{
		Id:  actual.Id,
		URI: addr + etre.API_ROOT + "/entity/" + actual.Id,
	}
	if diffs := deep.Equal(actual, expect); diffs != nil {
		t.Error(diffs)
	}
}

func TestPostEntityHandlerPayloadError(t *testing.T) {
	setup(t)
	defer teardown(t)

	url := defaultServer.URL + etre.API_ROOT + "/entity/" + entityType
	// etre.Entity type is expected to be in the payload, so passing in an empty
	// payload will trigger an error.
	var payload []byte
	var respErr etre.Error

	statusCode, err := test.MakeHTTPRequest("POST", url, payload, &respErr)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusInternalServerError {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusInternalServerError)
	}

	if respErr.Type != "internal-error" {
		t.Errorf("got Error.Type = %s, expected internal-error", respErr.Type)
	}
	if respErr.Message == "" {
		t.Errorf("Error.Message is empty, expected a value")
	}
}

func TestPostEntityHandlerInvalidValueTypeError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// arrays are not supported value types
	x := []int{0, 1, 2}
	entity := etre.Entity{"x": x}

	url := defaultServer.URL + etre.API_ROOT + "/entity/" + entityType
	payload, err := json.Marshal(entity)
	if err != nil {
		t.Fatal(err)
	}

	var respErr etre.Error
	statusCode, err := test.MakeHTTPRequest("POST", url, payload, &respErr)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusBadRequest {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusBadRequest)
	}

	if respErr.Type != "bad-request" {
		t.Errorf("got Error.Type = %s, expected internal-error", respErr.Type)
	}
	if respErr.Message == "" {
		t.Errorf("Error.Message is empty, expected a value")
	}
}

func TestGetEntityHandlerSuccessful(t *testing.T) {
	setup(t)
	defer teardown(t)

	url := defaultServer.URL + etre.API_ROOT + "/entity/" + entityType + "/" + seedId0
	var actual etre.Entity

	statusCode, err := test.MakeHTTPRequest("GET", url, nil, &actual)
	if err != nil {
		t.Fatal(err)
	}

	api.ConvertFloat64ToInt(actual)

	if diffs := deep.Equal(actual, seedEntities[0]); diffs != nil {
		t.Logf("got: %#v", actual)
		t.Error(diffs)
	}

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}
}

func TestGetEntityHandlerMissingIDError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Omit ID from URL
	url := defaultServer.URL + etre.API_ROOT + "/entity/" + entityType + "/"

	var respErr etre.Error
	statusCode, err := test.MakeHTTPRequest("GET", url, nil, &respErr)
	if err != nil {
		t.Fatal(err)
	}

	// This is technically an invalid route, so Echo returns a standard 404 message
	// and never calls out to our handler (that's why we don't check the contents
	// of the error message here).
	if statusCode != http.StatusNotFound {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusBadRequest)
	}
}

func TestGetEntityHandlerNotFoundError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// The db always returns a slice, even if empty. Empty slice and
	// no error means no matching entities were found. For the single
	// entity endpoint, that results in a 404.
	seedEntities = []etre.Entity{}

	// Needs to be a valid ObjectId
	id := "59ee2d725669fcc51f62aaaa"

	url := defaultServer.URL + etre.API_ROOT + "/entity/" + entityType + "/" + id
	var actual []byte

	statusCode, err := test.MakeHTTPRequest("GET", url, nil, &actual)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusNotFound {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusNotFound)
	}

	if actual != nil {
		t.Errorf("response error message = %s, expected nil", actual)
	}
}

func TestPutEntityHandlerSuccessful(t *testing.T) {
	setup(t)
	defer teardown(t)

	// On update, the entity store returns the prev values for changed labels,
	// and metalabels. This simulates that return. Also, the controller wraps
	// this in WriteResult.Diff. Lastly, since this is simulating data directly
	// from the db, _id and _rev are cast to the appropriate types; higher up,
	// these become string and uint, respectively.
	updateEntities = []etre.Entity{
		{"_id": bson.ObjectIdHex(seedId0), "_type": entityType, "_rev": float64(0), "foo": seedEntity0["foo"]},
	}

	// foo:bar -> foo:baz
	update := etre.Entity{"foo": "baz"}
	payload, err := json.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	var actual etre.WriteResult

	url := defaultServer.URL + etre.API_ROOT + "/entity/" + entityType + "/" + seedId0
	statusCode, err := test.MakeHTTPRequest("PUT", url, payload, &actual)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}

	if actual.Id != seedId0 {
		t.Errorf("got id %s, expected %s", actual.Id, seedId0)
	}
	expectURI := addr + etre.API_ROOT + "/entity/" + actual.Id
	if actual.URI != expectURI {
		t.Errorf("got URI %s, expected %s", actual.URI, expectURI)
	}
	if actual.Diff == nil {
		t.Fatal("got nil Diff, expected non-nil value")
	}
	updateEntities[0]["_id"] = seedId0 // convert back to string
	if diffs := deep.Equal(actual.Diff, updateEntities[0]); diffs != nil {
		t.Error(diffs)
	}
}

func TestPutEntityHandlerMissingIDError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Omit ID from URL
	url := defaultServer.URL + etre.API_ROOT + "/entity/" + entityType + "/"

	var respErr etre.Error
	statusCode, err := test.MakeHTTPRequest("PUT", url, nil, &respErr)
	if err != nil {
		t.Fatal(err)
	}

	// This is technically an invalid route, so Echo returns a standard 404 message
	// and never calls out to our handler (that's why we don't check the contents
	// of the error message here).
	if statusCode != http.StatusNotFound {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusBadRequest)
	}
}

func TestPutEntityHandlerPayloadError(t *testing.T) {
	setup(t)
	defer teardown(t)

	id := seedId0

	url := defaultServer.URL + etre.API_ROOT + "/entity/" + entityType + "/" + id
	// etre.Entity type is expected to be in the payload, so passing in an empty
	// payload will trigger an error.
	var payload []byte
	var respErr etre.Error

	statusCode, err := test.MakeHTTPRequest("PUT", url, payload, &respErr)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusInternalServerError {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusInternalServerError)
	}

	if respErr.Type != "internal-error" {
		t.Errorf("got Error.Type = %s, expected internal-error", respErr.Type)
	}
	if respErr.Message == "" {
		t.Errorf("Error.Message is empty, expected a value")
	}
}

func TestDeleteEntityHandlerSuccessful(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Same as TestPutEntityHandlerSuccessful above.
	deleteEntities = []etre.Entity{
		{"_id": bson.ObjectIdHex(seedId0), "_type": entityType, "_rev": float64(0), "foo": seedEntity0["foo"]},
	}

	var actual etre.WriteResult
	url := defaultServer.URL + etre.API_ROOT + "/entity/" + entityType + "/" + seedId0

	statusCode, err := test.MakeHTTPRequest("DELETE", url, nil, &actual)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}

	deleteEntities[0]["_id"] = seedId0 // convert back to string

	expect := etre.WriteResult{
		Id:   seedId0,
		URI:  addr + etre.API_ROOT + "/entity/" + seedId0,
		Diff: deleteEntities[0],
	}
	if diffs := deep.Equal(actual, expect); diffs != nil {
		t.Logf("got: %#v", actual)
		t.Logf("expect: %#v", expect)
		t.Error(diffs)
	}
}

func TestDeleteEntityHandlerMissingIDError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Omit ID from URL
	url := defaultServer.URL + etre.API_ROOT + "/entity/" + entityType + "/"

	var respErr etre.Error
	statusCode, err := test.MakeHTTPRequest("DELETE", url, nil, &respErr)
	if err != nil {
		t.Fatal(err)
	}

	// This is technically an invalid route, so Echo returns a standard 404 message
	// and never calls out to our handler (that's why we don't check the contents
	// of the error message here).
	if statusCode != http.StatusNotFound {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusBadRequest)
	}
}

// //////////////////////////////////////////////////////////////////////////
// Multiple Entity Management Handler Tests
// //////////////////////////////////////////////////////////////////////////

func TestPostEntitiesHandlerSuccessful(t *testing.T) {
	setup(t)
	defer teardown(t)

	createIds = make([]string, len(seedEntities))
	for i := range createIds {
		createIds[i] = fmt.Sprintf("id%d", i)
	}

	payload, err := json.Marshal(seedEntities)
	if err != nil {
		t.Fatal(err)
	}

	var actual []etre.WriteResult
	url := defaultServer.URL + etre.API_ROOT + "/entities/" + entityType

	statusCode, err := test.MakeHTTPRequest("POST", url, payload, &actual)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusCreated {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}

	if len(actual) != len(seedEntities) {
		t.Errorf("got %d ids, expected %d", len(actual), len(seedEntities))
	}
	for i, wr := range actual {
		if wr.Id != createIds[i] {
			t.Errorf("WriteResult.Id = %d, expected %d", wr.Id, createIds[i])
		}
		if wr.URI == "" {
			t.Errorf("WriteResult.URI not set: %#v", wr)
		}
		if wr.Diff != nil {
			t.Errorf("WriteResult.Diff is set: %#v", wr)
		}
		if wr.Error != "" {
			t.Errorf("WriteResult.Error is set: %#v", wr)
		}
	}
}

func TestPostEntitiesHandlerPayloadError(t *testing.T) {
	setup(t)
	defer teardown(t)

	url := defaultServer.URL + etre.API_ROOT + "/entities/" + entityType
	// etre.Entity type is expected to be in the payload, so passing in an empty
	// payload will trigger an error.
	var payload []byte
	var respErr etre.Error

	statusCode, err := test.MakeHTTPRequest("POST", url, payload, &respErr)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusInternalServerError {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusInternalServerError)
	}

	if respErr.Type != "internal-error" {
		t.Errorf("got Error.Type = %s, expected internal-error", respErr.Type)
	}
	if respErr.Message == "" {
		t.Errorf("Error.Message is empty, expected a value")
	}
}

func TestPostEntitiesHandlerInvalidValueTypeError(t *testing.T) {
	setup(t)
	defer teardown(t)

	yArr := []string{"foo", "bar", "baz"}

	entity2 := etre.Entity{"x": 2}
	// Arrays are not supported values types
	entity3 := etre.Entity{"y": yArr}
	entities := []etre.Entity{entity2, entity3}

	url := defaultServer.URL + etre.API_ROOT + "/entities/" + entityType
	payload, err := json.Marshal(entities)
	if err != nil {
		t.Fatal(err)
	}
	var respErr etre.Error

	statusCode, err := test.MakeHTTPRequest("POST", url, payload, &respErr)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusBadRequest {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusBadRequest)
	}

	if respErr.Type != "bad-request" {
		t.Errorf("got Error.Type = %s, expected internal-error", respErr.Type)
	}
	if respErr.Message == "" {
		t.Errorf("Error.Message is empty, expected a value")
	}
}

func TestGetEntitiesHandlerSuccessful(t *testing.T) {
	setup(t)
	defer teardown(t)

	expect := seedEntities

	query := url.QueryEscape("foo=bar")
	url := defaultServer.URL + etre.API_ROOT + "/entities/" + entityType + "?query=" + query
	var actual []etre.Entity

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

	if diffs := deep.Equal(actual, expect); diffs != nil {
		t.Error(diffs)
	}

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}
}

func TestGetEntitiesHandlerMissingQueryError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Omit query param from URL
	url := defaultServer.URL + etre.API_ROOT + "/entities/" + entityType + "?"
	expectErr := "query string is empty"
	testBadRequestError(t, "DELETE", url, expectErr)
}

func TestGetEntitiesHandlerEmptyQueryError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Omit query string from URL
	url := defaultServer.URL + etre.API_ROOT + "/entities/" + entityType + "?query"

	expectErr := "query string is empty"
	testBadRequestError(t, "GET", url, expectErr)
}

func TestGetEntitiesHandlerNotFoundError(t *testing.T) {
	setup(t)
	defer teardown(t)

	seedEntities = []etre.Entity{}

	labelSelector := "x=9999"
	query := url.QueryEscape(labelSelector)
	url := defaultServer.URL + etre.API_ROOT + "/entities/" + entityType + "?query=" + query

	var actual []etre.Entity
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

	// Same as TestPutEntityHandlerSuccessful.
	updateEntities = []etre.Entity{
		etre.Entity{"_id": bson.ObjectIdHex(seedId0), "_type": entityType, "_rev": float64(0), "foo": seedEntity0["foo"]},
		etre.Entity{"_id": bson.ObjectIdHex(seedId1), "_type": entityType, "_rev": float64(0), "foo": seedEntity0["foo"]},
	}

	update := etre.Entity{"foo": "baz"}
	payload, err := json.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}

	var actual []etre.WriteResult
	query := url.QueryEscape("x>0") // doesn't matter
	url := defaultServer.URL + etre.API_ROOT + "/entities/" + entityType + "?query=" + query

	statusCode, err := test.MakeHTTPRequest("PUT", url, payload, &actual)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}

	if len(actual) != 2 {
		t.Errorf("got %d WriteResult, expected 2", len(actual))
	}

	// Convert back to string
	updateEntities[0]["_id"] = seedId0
	updateEntities[1]["_id"] = seedId1

	expect := []etre.WriteResult{
		{
			Id:   seedId0,
			URI:  addr + etre.API_ROOT + "/entity/" + seedId0,
			Diff: updateEntities[0],
		},
		{
			Id:   seedId1,
			URI:  addr + etre.API_ROOT + "/entity/" + seedId1,
			Diff: updateEntities[1],
		},
	}
	if diffs := deep.Equal(actual, expect); diffs != nil {
		t.Error(diffs)
	}
}

func TestPutEntitiesHandlerMissingQueryError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Omit query param from URL
	url := defaultServer.URL + etre.API_ROOT + "/entities/" + entityType + "?"
	expectErr := "query string is empty"
	testBadRequestError(t, "DELETE", url, expectErr)
}

func TestPutEntitiesHandlerEmptyQueryError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Omit query string from URL
	url := defaultServer.URL + etre.API_ROOT + "/entities/" + entityType + "?query"
	expectErr := "query string is empty"
	testBadRequestError(t, "PUT", url, expectErr)
}

func TestPutEntitiesHandlerPayloadError(t *testing.T) {
	setup(t)
	defer teardown(t)

	query := url.QueryEscape("x>0")

	url := defaultServer.URL + etre.API_ROOT + "/entities/" + entityType + "?query=" + query
	// etre.Entity type is expected to be in the payload, so passing in an empty
	// payload will trigger an error.
	var payload []byte
	var respErr etre.Error

	statusCode, err := test.MakeHTTPRequest("PUT", url, payload, &respErr)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusInternalServerError {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusInternalServerError)
	}

	if respErr.Type != "internal-error" {
		t.Errorf("got Error.Type = %s, expected internal-error", respErr.Type)
	}
	if respErr.Message == "" {
		t.Errorf("Error.Message is empty, expected a value")
	}
}

func TestDeleteEntitiesHandlerSuccessful(t *testing.T) {
	setup(t)
	defer teardown(t)

	deleteEntities = []etre.Entity{
		etre.Entity{"_id": bson.ObjectIdHex(seedId0), "_type": entityType, "_rev": float64(0), "foo": "bar"},
		etre.Entity{"_id": bson.ObjectIdHex(seedId1), "_type": entityType, "_rev": float64(0), "foo": "bar"},
	}

	query := url.QueryEscape("foo=bar") // doesn't matter
	url := defaultServer.URL + etre.API_ROOT + "/entities/" + entityType + "?query=" + query
	var actual []etre.WriteResult

	statusCode, err := test.MakeHTTPRequest("DELETE", url, nil, &actual)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}

	if len(actual) != 2 {
		t.Errorf("got %d WriteResult, expected 2", len(actual))
	}

	// Convert back to string
	deleteEntities[0]["_id"] = seedId0
	deleteEntities[1]["_id"] = seedId1

	expect := []etre.WriteResult{
		{
			Id:   seedId0,
			URI:  addr + etre.API_ROOT + "/entity/" + seedId0,
			Diff: deleteEntities[0],
		},
		{
			Id:   seedId1,
			URI:  addr + etre.API_ROOT + "/entity/" + seedId1,
			Diff: deleteEntities[1],
		},
	}
	if diffs := deep.Equal(actual, expect); diffs != nil {
		t.Error(diffs)
	}
}

func TestDeleteEntitiesHandlerMissingQueryError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Omit query param from URL
	url := defaultServer.URL + etre.API_ROOT + "/entities/" + entityType + "?"
	expectErr := "query string is empty"
	testBadRequestError(t, "DELETE", url, expectErr)
}

func TestDeleteEntitiesHandlerEmptyQueryError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Omit query string from URL
	url := defaultServer.URL + etre.API_ROOT + "/entities/" + entityType
	expectErr := "query string is empty"
	testBadRequestError(t, "DELETE", url, expectErr)
}

////////////////////////////////////////////////////////////////////////////
// Helper Functions
////////////////////////////////////////////////////////////////////////////

func testBadRequestError(t *testing.T, httpVerb string, url string, expectErr string) {
	var respErr etre.Error

	statusCode, err := test.MakeHTTPRequest(httpVerb, url, nil, &respErr)
	if err != nil {
		t.Fatal(err)
	}

	if respErr.Message != expectErr {
		t.Errorf("response error message = %s, expected %s", respErr.Message, expectErr)
	}

	if statusCode != http.StatusBadRequest {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusBadRequest)
	}
}
