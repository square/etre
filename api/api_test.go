// Copyright 2017-2018, Square, Inc.

package api_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sync"
	"testing"

	"github.com/square/etre"
	"github.com/square/etre/api"
	"github.com/square/etre/app"
	"github.com/square/etre/auth"
	"github.com/square/etre/config"
	"github.com/square/etre/entity"
	"github.com/square/etre/metrics"
	"github.com/square/etre/query"
	"github.com/square/etre/test"
	"github.com/square/etre/test/mock"

	"github.com/globalsign/mgo/bson"
	"github.com/go-test/deep"
)

var (
	createIds         []string
	updateEntities    []etre.Entity
	deleteEntities    []etre.Entity
	deleteLabelEntity etre.Entity

	readErr   error
	createErr error
	updateErr error
	deleteErr error

	seedId0, seedId1                      string
	seedEntity0, seedEntity1, seedEntity2 etre.Entity
	seedEntities                          []etre.Entity

	gotWO    entity.WriteOp
	gotLabel string
)

var (
	addr          = "http://localhost"
	entityType    = "nodes"
	validate      = entity.NewValidator([]string{entityType})
	cfg           config.Config
	es            *mock.EntityStore
	defaultServer *httptest.Server
	mu            = &sync.Mutex{}
	metricsrec    = mock.NewMetricsRecorder()
)

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
			DeleteLabelFunc: func(wo entity.WriteOp, label string) (etre.Entity, error) {
				gotWO = wo
				gotLabel = label
				return deleteLabelEntity, nil
			},
		}
		cfg = config.Config{
			Server: config.ServerConfig{
				Addr: addr,
			},
		}
		appCtx := app.Context{
			Config:          cfg,
			EntityStore:     es,
			EntityValidator: validate,
			CDCStore:        nil,
			FeedFactory:     &mock.FeedFactory{},
			Auth:            auth.NewManager(nil, auth.NewAllowAll()),
			MetricsStore:    nil, // only needed for GET /metrics
			MetricsFactory:  mock.MetricsFactory{MetricRecorder: metricsrec},
		}
		defaultAPI := api.NewAPI(appCtx)
		defaultServer = httptest.NewServer(defaultAPI)
		t.Logf("started test HTTP server: %s\n", defaultServer.URL)
	}

	metricsrec.Reset()

	createIds = nil
	updateEntities = nil
	deleteEntities = nil
	deleteLabelEntity = etre.Entity{}

	gotWO = entity.WriteOp{}
	gotLabel = ""

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

func floatToInt(entities []etre.Entity) {
	for i, e := range entities {
		for label, val := range e {
			if reflect.TypeOf(val).Kind() == reflect.Float64 {
				entities[i][label] = int(val.(float64))
			}
		}
	}
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
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusCreated)
	}

	expect := etre.WriteResult{
		Writes: []etre.Write{
			{
				Id:  "id1",
				URI: addr + etre.API_ROOT + "/entity/id1",
			},
		},
	}
	if diffs := deep.Equal(actual, expect); diffs != nil {
		t.Logf("%+v", actual.Error)
		t.Error(diffs)
	}

	// Verify proper metrics are incremented: first, API always sets entity type.
	// Then it increments Query for every query. Since this is a write, +1 to Write.
	// It's an insert type write, so +1 to Create. And finally, every query's
	// reponse time (latency) is recorded.
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.Create, IntVal: 1},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(metricsrec.Called, expectMetrics); diffs != nil {
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
	// Writes always return an etre.WriteResult, even on error
	var respErr etre.WriteResult

	statusCode, err := test.MakeHTTPRequest("POST", url, payload, &respErr)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != api.ErrInternal.HTTPStatus {
		t.Errorf("response status = %d, expected %d", statusCode, api.ErrInternal.HTTPStatus)
	}

	if respErr.Error == nil {
		t.Fatalf("WriteResult.Error is nil, expected it to be set")
	}
	if respErr.Error.Type != api.ErrInternal.Type {
		t.Errorf("got Error.Type = %s, expected %s", respErr.Error.Type, api.ErrInternal.Type)
	}
	if respErr.Error.Message == "" {
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

	var respErr etre.WriteResult
	statusCode, err := test.MakeHTTPRequest("POST", url, payload, &respErr)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusBadRequest {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusBadRequest)
	}

	if respErr.Error == nil {
		t.Fatalf("WriteResult.Error is nil, expected it to be set")
	}
	if respErr.Error.Type != "invalid-value-type" {
		t.Errorf("got Error.Type = %s, expected invalid-value-type", respErr.Error.Type)
	}
	if respErr.Error.Message == "" {
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

	if len(actual) == 0 {
		t.Fatal("did not return an entity")
	}
	floatToInt([]etre.Entity{actual})
	if diffs := deep.Equal(actual, seedEntity0); diffs != nil {
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

	updateEntities[0]["_id"] = seedId0 // convert back to string

	expect := etre.WriteResult{
		Writes: []etre.Write{
			{
				Id:   seedId0,
				URI:  addr + etre.API_ROOT + "/entity/" + seedId0,
				Diff: updateEntities[0],
			},
		},
	}
	if diffs := deep.Equal(actual, expect); diffs != nil {
		t.Error(diffs)
	}
}

func TestPutEntityHandlerMissingIDError(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Omit ID from URL
	url := defaultServer.URL + etre.API_ROOT + "/entity/" + entityType + "/"

	var respErr etre.WriteResult
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
	var respErr etre.WriteResult

	statusCode, err := test.MakeHTTPRequest("PUT", url, payload, &respErr)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusInternalServerError {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusInternalServerError)
	}

	if respErr.Error == nil {
		t.Fatalf("WriteResult.Error is nil, expected it to be set")
	}
	if respErr.Error.Type != "internal-error" {
		t.Errorf("got Error.Type = %s, expected internal-error", respErr.Error.Type)
	}
	if respErr.Error.Message == "" {
		t.Errorf("Error.Message is empty, expected a value")
	}
}

func TestPutEntityHandlerDuplicateEntity(t *testing.T) {
	setup(t)
	defer teardown(t)

	updateErr = entity.DbError{
		Type:     "duplicate-entity", // the key to making this happen
		EntityId: seedId0,
		Err:      fmt.Errorf("some error msg from mongo"),
	}
	update := etre.Entity{"foo": "baz"} // doesn't matter, returning that ^
	payload, err := json.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}

	var respErr etre.WriteResult
	url := defaultServer.URL + etre.API_ROOT + "/entity/" + entityType + "/" + seedId0
	statusCode, err := test.MakeHTTPRequest("PUT", url, payload, &respErr)
	if err != nil {
		t.Fatal(err)
	}
	if statusCode != http.StatusConflict {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusBadRequest)
	}
	if respErr.Error == nil {
		t.Fatalf("WriteResult.Error is nil, expected it to be set")
	}
	if respErr.Error.Type != "duplicate-entity" {
		t.Errorf("got Error.Type = %s, expected duplicate-entity", respErr.Error.Type)
	}
	if respErr.Error.Message == "" {
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
		Writes: []etre.Write{
			{
				Id:   seedId0,
				URI:  addr + etre.API_ROOT + "/entity/" + seedId0,
				Diff: deleteEntities[0],
			},
		},
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

	var respErr etre.WriteResult
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

	var actual etre.WriteResult
	url := defaultServer.URL + etre.API_ROOT + "/entities/" + entityType

	statusCode, err := test.MakeHTTPRequest("POST", url, payload, &actual)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusCreated {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}

	if len(actual.Writes) != len(seedEntities) {
		t.Errorf("got %d ids, expected %d", len(actual.Writes), len(seedEntities))
	}
	for i, wr := range actual.Writes {
		if wr.Id != createIds[i] {
			t.Errorf("WriteResult.Id = %s, expected %s", wr.Id, createIds[i])
		}
		if wr.URI == "" {
			t.Errorf("WriteResult.URI not set: %#v", wr)
		}
		if wr.Diff != nil {
			t.Errorf("WriteResult.Diff is set: %#v", wr)
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
	var respErr etre.WriteResult

	statusCode, err := test.MakeHTTPRequest("POST", url, payload, &respErr)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusInternalServerError {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusInternalServerError)
	}

	if respErr.Error == nil {
		t.Fatalf("WriteResult.Error is nil, expected it to be set")
	}
	if respErr.Error.Type != "internal-error" {
		t.Errorf("got Error.Type = %s, expected internal-error", respErr.Error.Type)
	}
	if respErr.Error.Message == "" {
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
	var respErr etre.WriteResult

	statusCode, err := test.MakeHTTPRequest("POST", url, payload, &respErr)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusBadRequest {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusBadRequest)
	}

	if respErr.Error == nil {
		t.Fatalf("WriteResult.Error is nil, expected it to be set")
	}
	if respErr.Error.Type != "invalid-value-type" {
		t.Errorf("got Error.Type = %s, expected invalid-value-type", respErr.Error.Type)
	}
	if respErr.Error.Message == "" {
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

	floatToInt(actual)
	for _, e := range actual {
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

	var respErr etre.Error
	statusCode, err := test.MakeHTTPRequest("GET", url, nil, &respErr)
	if err != nil {
		t.Fatal(err)
	}
	if statusCode != http.StatusBadRequest {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusBadRequest)
	}
	if respErr.Type != api.ErrInvalidQuery.Type {
		t.Errorf("got error type %s, expected %s", respErr.Type, api.ErrInvalidQuery.Type)
	}

	// Empty query
	url = defaultServer.URL + etre.API_ROOT + "/entities/" + entityType + "?query"
	statusCode, err = test.MakeHTTPRequest("GET", url, nil, &respErr)
	if err != nil {
		t.Fatal(err)
	}
	if statusCode != http.StatusBadRequest {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusBadRequest)
	}
	if respErr.Type != api.ErrInvalidQuery.Type {
		t.Errorf("got error type %s, expected %s", respErr.Type, api.ErrInvalidQuery.Type)
	}
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

	var actual etre.WriteResult
	query := url.QueryEscape("x>0") // doesn't matter
	url := defaultServer.URL + etre.API_ROOT + "/entities/" + entityType + "?query=" + query

	statusCode, err := test.MakeHTTPRequest("PUT", url, payload, &actual)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}

	if len(actual.Writes) != 2 {
		t.Errorf("got %d WriteResult, expected 2", len(actual.Writes))
	}

	// Convert back to string
	updateEntities[0]["_id"] = seedId0
	updateEntities[1]["_id"] = seedId1

	expect := etre.WriteResult{
		Writes: []etre.Write{
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
		},
	}
	if diffs := deep.Equal(actual, expect); diffs != nil {
		t.Error(diffs)
	}
}

func TestPutEntitiesHandlerMissingQueryError(t *testing.T) {
	setup(t)
	defer teardown(t)

	var respErr etre.WriteResult

	// Omit query param from URL
	url := defaultServer.URL + etre.API_ROOT + "/entities/" + entityType + "?"
	statusCode, err := test.MakeHTTPRequest("PUT", url, nil, &respErr)
	if err != nil {
		t.Fatal(err)
	}
	if statusCode != http.StatusBadRequest {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusBadRequest)
	}
	if respErr.Error == nil {
		t.Fatal("WriteResult.Error is nil, expected it to be set")
	}
	if respErr.Error.Type != api.ErrInvalidQuery.Type {
		t.Errorf("got error type %s, expected %s", respErr.Error.Type, api.ErrInvalidQuery.Type)
	}

	// Empty query
	url = defaultServer.URL + etre.API_ROOT + "/entities/" + entityType + "?query"
	statusCode, err = test.MakeHTTPRequest("PUT", url, nil, &respErr)
	if err != nil {
		t.Fatal(err)
	}
	if statusCode != api.ErrInvalidQuery.HTTPStatus {
		t.Errorf("response status = %d, expected %d", statusCode, api.ErrInvalidQuery.HTTPStatus)
	}
	if respErr.Error == nil {
		t.Fatal("WriteResult.Error is nil, expected it to be set")
	}
	if respErr.Error.Type != api.ErrInvalidQuery.Type {
		t.Errorf("got error type %s, expected %s", respErr.Error.Type, api.ErrInvalidQuery.Type)
	}
}

func TestPutEntitiesHandlerPayloadError(t *testing.T) {
	setup(t)
	defer teardown(t)

	query := url.QueryEscape("x>0")

	url := defaultServer.URL + etre.API_ROOT + "/entities/" + entityType + "?query=" + query
	// etre.Entity type is expected to be in the payload, so passing in an empty
	// payload will trigger an error.
	var payload []byte
	var respErr etre.WriteResult

	statusCode, err := test.MakeHTTPRequest("PUT", url, payload, &respErr)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusInternalServerError {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusInternalServerError)
	}

	if respErr.Error == nil {
		t.Fatalf("WriteResult.Error is nil, expected it to be set")
	}
	if respErr.Error.Type != "internal-error" {
		t.Errorf("got Error.Type = %s, expected internal-error", respErr.Error.Type)
	}
	if respErr.Error.Message == "" {
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
	var actual etre.WriteResult

	statusCode, err := test.MakeHTTPRequest("DELETE", url, nil, &actual)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}

	// Convert back to string
	deleteEntities[0]["_id"] = seedId0
	deleteEntities[1]["_id"] = seedId1

	expect := etre.WriteResult{
		Writes: []etre.Write{
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
		},
	}
	if diffs := deep.Equal(actual, expect); diffs != nil {
		t.Error(diffs)
	}
}

func TestDeleteEntitiesHandlerMissingQueryError(t *testing.T) {
	setup(t)
	defer teardown(t)

	var respErr etre.WriteResult

	// Omit query param from URL
	url := defaultServer.URL + etre.API_ROOT + "/entities/" + entityType + "?"
	statusCode, err := test.MakeHTTPRequest("DELETE", url, nil, &respErr)
	if err != nil {
		t.Fatal(err)
	}
	if statusCode != api.ErrInvalidQuery.HTTPStatus {
		t.Errorf("response status = %d, expected %d", statusCode, api.ErrInvalidQuery.HTTPStatus)
	}
	if respErr.Error == nil {
		t.Fatal("WriteResult.Error is nil, expected it to be set")
	}
	if respErr.Error.Type != api.ErrInvalidQuery.Type {
		t.Errorf("got error type %s, expected %s", respErr.Error.Type, api.ErrInvalidQuery.Type)
	}

	// Empty query
	url = defaultServer.URL + etre.API_ROOT + "/entities/" + entityType
	statusCode, err = test.MakeHTTPRequest("DELETE", url, nil, &respErr)
	if statusCode != api.ErrInvalidQuery.HTTPStatus {
		t.Errorf("response status = %d, expected %d", statusCode, api.ErrInvalidQuery.HTTPStatus)
	}
	if respErr.Error == nil {
		t.Fatal("WriteResult.Error is nil, expected it to be set")
	}
	if respErr.Error.Type != api.ErrInvalidQuery.Type {
		t.Errorf("got error type %s, expected %s", respErr.Error.Type, api.ErrInvalidQuery.Type)
	}
}

func TestDeleteLabelHandler(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Need to return the old entity with _id so the controller can turn it
	// into a WriteResult
	deleteLabelEntity = etre.Entity{
		"_id":   bson.ObjectIdHex(seedId0),
		"_type": entityType,
		"_rev":  uint(0),
		"foo":   "bar",
	}

	// Success
	url := defaultServer.URL + etre.API_ROOT + "/entity/" + entityType + "/" + seedId0 + "/labels/baz"
	statusCode, err := test.MakeHTTPRequest("DELETE", url, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}
	if gotLabel != "baz" {
		t.Errorf("got label %s, expected baz", gotLabel)
	}
	expectWO := entity.WriteOp{
		User:       "etre",
		EntityType: entityType,
		EntityId:   seedId0,
	}
	if diff := deep.Equal(gotWO, expectWO); diff != nil {
		t.Error(diff)
	}

	// Cannot delete metalabel
	url = defaultServer.URL + etre.API_ROOT + "/entity/" + entityType + "/" + seedId0 + "/labels/_id"
	statusCode, err = test.MakeHTTPRequest("DELETE", url, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if statusCode != http.StatusBadRequest {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusBadRequest)
	}
}

// --------------------------------------------------------------------------
// v0.8 compatibility
// --------------------------------------------------------------------------

func TestV08PostEntityHandlerSuccessful(t *testing.T) {
	setup(t)
	defer teardown(t)

	// v0.9: etre.WriteResult{Writes: []etre.Write, ...}
	// v0.8: []etre.Write
	test.Headers["X-Etre-Version"] = "0.8.0-alpha"
	defer delete(test.Headers, "X-Etre-Version")

	entity := etre.Entity{"x": 8}
	payload, err := json.Marshal(entity)
	if err != nil {
		t.Fatal(err)
	}

	createIds = []string{"id1"} // global var

	url := defaultServer.URL + etre.API_ROOT + "/entity/" + entityType
	var actual []etre.Write
	statusCode, err := test.MakeHTTPRequest("POST", url, payload, &actual)
	if err != nil {
		t.Fatal(err)
	}
	if statusCode != http.StatusCreated {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusCreated)
	}
	expect := []etre.Write{
		{
			Id:  "id1",
			URI: addr + etre.API_ROOT + "/entity/id1",
		},
	}
	if diffs := deep.Equal(actual, expect); diffs != nil {
		t.Logf("%+v", actual)
		t.Error(diffs)
	}
}
