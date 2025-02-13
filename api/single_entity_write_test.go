// Copyright 2017-2020, Square, Inc.

package api_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/go-test/deep"

	"github.com/square/etre"
	"github.com/square/etre/api"
	"github.com/square/etre/entity"
	"github.com/square/etre/metrics"
	"github.com/square/etre/query"
	"github.com/square/etre/test"
	"github.com/square/etre/test/mock"
)

// //////////////////////////////////////////////////////////////////////////
// Insert
// //////////////////////////////////////////////////////////////////////////

func TestPostEntityOK(t *testing.T) {
	// Test that POST /entities/:type works correctly. Under the hood, this
	// calls entity.Store.CreateEntities() with the single entity from
	// the HTTP caller. So we expect a list of entities with just the one
	// sent (payload). The returned entity id becomes parts of the WriteResult.
	var gotWO entity.WriteOp
	var gotEntities []etre.Entity
	store := mock.EntityStore{
		CreateEntitiesFunc: func(wo entity.WriteOp, entities []etre.Entity) ([]string, error) {
			gotWO = wo
			gotEntities = entities
			return []string{"id1"}, nil
		},
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()

	newEntity := etre.Entity{"host": "local"}
	payload, err := json.Marshal(newEntity)
	if err != nil {
		t.Fatal(err)
	}

	etreurl := server.url + etre.API_ROOT + "/entity/" + entityType

	var gotWR etre.WriteResult
	statusCode, err := test.MakeHTTPRequest("POST", etreurl, payload, &gotWR)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusCreated {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusCreated)
	}

	expectWR := etre.WriteResult{
		Writes: []etre.Write{
			{
				EntityId: "id1",
				URI:      addr + etre.API_ROOT + "/entity/id1",
			},
		},
	}
	if diffs := deep.Equal(gotWR, expectWR); diffs != nil {
		t.Error(diffs)
	}

	expectWO := entity.WriteOp{
		Caller:     "test", // from mock.AuthPlugin
		EntityType: entityType,
	}
	if diff := deep.Equal(gotWO, expectWO); diff != nil {
		t.Error(diff)
	}

	expectEntities := []etre.Entity{newEntity}
	if diff := deep.Equal(gotEntities, expectEntities); diff != nil {
		t.Error(diff)
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.CreateOne, IntVal: 1},
		{Method: "Inc", Metric: metrics.Created, IntVal: 1},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}
}

func TestPostEntityDuplicate(t *testing.T) {
	// Test that POST /entities/:type returns HTTP 403 Conflict on duplicate
	// which we simulate by returning what entity.Store would:
	store := mock.EntityStore{
		CreateEntitiesFunc: func(wo entity.WriteOp, entities []etre.Entity) ([]string, error) {
			// Real CreateEntities() always returns []string, not nil, because
			// it supports partial writes
			return []string{}, entity.DbError{Err: fmt.Errorf("dupe"), Type: "duplicate-entity"}
		},
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()

	newEntity := etre.Entity{"host": "local"}
	payload, err := json.Marshal(newEntity)
	if err != nil {
		t.Fatal(err)
	}

	etreurl := server.url + etre.API_ROOT + "/entity/" + entityType

	var gotWR etre.WriteResult
	statusCode, err := test.MakeHTTPRequest("POST", etreurl, payload, &gotWR)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusConflict {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusConflict)
	}

	if gotWR.Error == nil || gotWR.Error.Message == "" {
		t.Errorf("WriteResult.Error is empty, expected duplicate key error")
	}
	if len(gotWR.Writes) != 0 {
		t.Errorf("%d writes, exected 0: %+v", len(gotWR.Writes), gotWR.Writes)
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.CreateOne, IntVal: 1},
		//{Method: "Inc", Metric: metrics.Created, IntVal: 1},
		{Method: "Inc", Metric: metrics.DbError, IntVal: 1},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}
}

func TestPostEntityErrors(t *testing.T) {
	// Test that POST /entities/:type returns an error for any issue
	created := false
	store := mock.EntityStore{
		CreateEntitiesFunc: func(wo entity.WriteOp, entities []etre.Entity) ([]string, error) {
			created = true
			return []string{"id1"}, nil
		},
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()

	// ----------------------------------------------------------------------
	// Can't create with _id
	// ----------------------------------------------------------------------
	newEntity := etre.Entity{"host": "local", "_id": testEntityIds[0]}
	payload, err := json.Marshal(newEntity)
	if err != nil {
		t.Fatal(err)
	}

	etreurl := server.url + etre.API_ROOT + "/entity/" + entityType

	var gotWR etre.WriteResult
	statusCode, err := test.MakeHTTPRequest("POST", etreurl, payload, &gotWR)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusBadRequest {
		t.Errorf("got HTTP status = %d, expected %d: %+v", statusCode, http.StatusBadRequest, gotWR)
	}

	if gotWR.Error == nil {
		t.Errorf("WriteResult.Error is nil, expected error message")
	} else if gotWR.Error.Type != "cannot-set-metalabel" {
		t.Errorf("WriteResult.Error.Type = %s, expected cannot-set-metalabel", gotWR.Error.Type)
	}

	if created == true {
		t.Error("CreateEntities called, expected no call due to error")
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.CreateOne, IntVal: 1},
		//{Method: "Inc", Metric: metrics.Created, IntVal: 1}, // NOT created due to error:
		{Method: "Inc", Metric: metrics.ClientError, IntVal: 1},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}

	// ----------------------------------------------------------------------
	// No value/zero entities (can't create nothing)
	// ----------------------------------------------------------------------
	server.metricsrec.Reset()

	newEntity = etre.Entity{}
	payload, err = json.Marshal(newEntity)
	if err != nil {
		t.Fatal(err)
	}

	gotWR = etre.WriteResult{}
	statusCode, err = test.MakeHTTPRequest("POST", etreurl, payload, &gotWR)
	t.Logf("got WriteResult: %+v", gotWR)
	if err != nil {
		t.Fatal(err)
	}
	if statusCode != http.StatusBadRequest {
		t.Errorf("got HTTP status = %d, expected %d: %+v", statusCode, http.StatusBadRequest, gotWR)
	}
	if gotWR.Error == nil {
		t.Errorf("WriteResult.Error is nil, expected error message")
	} else if gotWR.Error.Type != "empty-entity" {
		t.Errorf("WriteResult.Error.Type = %s, expected empty-entity", gotWR.Error.Type)
	}
	if created == true {
		t.Error("CreateEntities called, expected no call due to error")
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics = []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.CreateOne, IntVal: 1},
		//{Method: "Inc", Metric: metrics.Created, IntVal: 1}, // NOT created due to error:
		{Method: "Inc", Metric: metrics.ClientError, IntVal: 1},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}

	// ----------------------------------------------------------------------
	// Invalid JSON
	// ----------------------------------------------------------------------
	server.metricsrec.Reset()

	payload = []byte("bad")

	gotWR = etre.WriteResult{}
	statusCode, err = test.MakeHTTPRequest("POST", etreurl, payload, &gotWR)
	t.Logf("got WriteResult: %+v", gotWR)
	if err != nil {
		t.Fatal(err)
	}
	if statusCode != http.StatusBadRequest {
		t.Errorf("got HTTP status = %d, expected %d: %+v", statusCode, http.StatusBadRequest, gotWR)
	}
	if gotWR.Error == nil {
		t.Errorf("WriteResult.Error is nil, expected error message")
	} else if gotWR.Error.Type != "invalid-content" {
		t.Errorf("WriteResult.Error.Type = %s, expected invalid-content", gotWR.Error.Type)
	}
	if created == true {
		t.Error("CreateEntities called, expected no call due to error")
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics = []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.CreateOne, IntVal: 1},
		//{Method: "Inc", Metric: metrics.Created, IntVal: 1}, // NOT created due to error:
		{Method: "Inc", Metric: metrics.ClientError, IntVal: 1},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}
}

// //////////////////////////////////////////////////////////////////////////
// Update
// //////////////////////////////////////////////////////////////////////////

func TestPutEntityOK(t *testing.T) {
	// Test that PUT /entities/:type/:id works correctly. Under the hood, this
	// calls entity.Store.UpdateEntities() with the single entity from
	// the HTTP caller. So we expect a list of entities with just the one
	// sent (payload). The return is the old values of the changed labels.
	var gotWO entity.WriteOp
	var gotQuery query.Query
	store := mock.EntityStore{
		UpdateEntitiesFunc: func(wo entity.WriteOp, q query.Query, patch etre.Entity) ([]etre.Entity, error) {
			gotWO = wo
			gotQuery = q
			diff := []etre.Entity{
				{"_id": testEntityId0, "_type": entityType, "_rev": int64(0), "foo": "oldVal"},
			}
			return diff, nil
		},
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()

	newEntity := etre.Entity{"foo": "bar"}
	payload, err := json.Marshal(newEntity)
	if err != nil {
		t.Fatal(err)
	}

	etreurl := server.url + etre.API_ROOT + "/entity/" + entityType + "/" + testEntityIds[0]

	var gotWR etre.WriteResult
	statusCode, err := test.MakeHTTPRequest("PUT", etreurl, payload, &gotWR)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}

	expectWR := etre.WriteResult{
		Writes: []etre.Write{
			{
				EntityId: testEntityIds[0],
				URI:      addr + etre.API_ROOT + "/entity/" + testEntityIds[0],
				Diff: etre.Entity{
					"_id":   testEntityIds[0],
					"_type": entityType,
					"_rev":  float64(0),
					"foo":   "oldVal",
				},
			},
		},
	}
	if diffs := deep.Equal(gotWR, expectWR); diffs != nil {
		t.Error(diffs)
	}

	expectWO := entity.WriteOp{
		Caller:     "test", // from mock.AuthPlugin
		EntityType: entityType,
		EntityId:   testEntityIds[0],
	}
	if diff := deep.Equal(gotWO, expectWO); diff != nil {
		t.Error(diff)
	}

	expectQuery, _ := query.Translate("_id=" + testEntityIds[0])
	if diff := deep.Equal(gotQuery, expectQuery); diff != nil {
		t.Error(diff)
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.UpdateId, IntVal: 1},
		{Method: "IncLabel", Metric: metrics.LabelUpdate, StringVal: "foo"}, // label in patch
		{Method: "Inc", Metric: metrics.Updated, IntVal: 1},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}
}

func TestPutEntityDuplicate(t *testing.T) {
	// Test that PUT /entities/:type/:id returns HTTP 403 Conflict on duplicate
	// which we simulate by returning what entity.Store would:
	store := mock.EntityStore{
		UpdateEntitiesFunc: func(wo entity.WriteOp, q query.Query, patch etre.Entity) ([]etre.Entity, error) {
			return nil, entity.DbError{
				Type:     "duplicate-entity", // the key to making this happen
				EntityId: testEntityIds[0],
				Err:      fmt.Errorf("some error msg from mongo"),
			}
		},
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()

	newEntity := etre.Entity{"foo": "bar"}
	payload, err := json.Marshal(newEntity)
	if err != nil {
		t.Fatal(err)
	}

	etreurl := server.url + etre.API_ROOT + "/entity/" + entityType + "/" + testEntityIds[0]

	var gotWR etre.WriteResult
	statusCode, err := test.MakeHTTPRequest("PUT", etreurl, payload, &gotWR)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusConflict {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusConflict)
	}

	if gotWR.Error == nil || gotWR.Error.Message == "" {
		t.Errorf("WriteResult.Error is empty, expected duplicate key error")
	}
	if len(gotWR.Writes) != 0 {
		t.Errorf("%d writes, exected 0: %+v", len(gotWR.Writes), gotWR.Writes)
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.UpdateId, IntVal: 1},
		{Method: "IncLabel", Metric: metrics.LabelUpdate, StringVal: "foo"}, // label in patch
		//{Method: "Inc", Metric: metrics.Updated, IntVal: 1},
		{Method: "Inc", Metric: metrics.DbError, IntVal: 1},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}
}

func TestPutEntityNotFound(t *testing.T) {
	// Test that PUT /entities/:type/:id returns HTTP 404 when there's no entity
	// with the given id. In this case, the entity.Store returns an empty diff:
	store := mock.EntityStore{
		UpdateEntitiesFunc: func(wo entity.WriteOp, q query.Query, patch etre.Entity) ([]etre.Entity, error) {
			return []etre.Entity{}, nil // no entities matched
		},
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()

	newEntity := etre.Entity{"foo": "bar"}
	payload, err := json.Marshal(newEntity)
	if err != nil {
		t.Fatal(err)
	}

	etreurl := server.url + etre.API_ROOT + "/entity/" + entityType + "/" + testEntityIds[0]

	var gotWR etre.WriteResult
	statusCode, err := test.MakeHTTPRequest("PUT", etreurl, payload, &gotWR)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusNotFound {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusNotFound)
	}

	if gotWR.Error == nil || gotWR.Error.Message == "" {
		t.Errorf("WriteResult.Error is empty, expected not found error")
	}
	if len(gotWR.Writes) != 0 {
		t.Errorf("%d writes, exected 0: %+v", len(gotWR.Writes), gotWR.Writes)
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.UpdateId, IntVal: 1},
		{Method: "IncLabel", Metric: metrics.LabelUpdate, StringVal: "foo"}, // label in patch
		//{Method: "Inc", Metric: metrics.Updated, IntVal: 1},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}
}

func TestPutEntityErrors(t *testing.T) {
	// Test that PUT /entities/:type/:id returns errors unless all inputs are correct
	updated := false
	store := mock.EntityStore{
		UpdateEntitiesFunc: func(wo entity.WriteOp, q query.Query, patch etre.Entity) ([]etre.Entity, error) {
			updated = true
			return []etre.Entity{{"_id": testEntityId0, "_type": entityType, "_rev": int64(0)}}, nil
		},
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()

	newEntity := etre.Entity{"foo": "bar"}
	payload, err := json.Marshal(newEntity)
	if err != nil {
		t.Fatal(err)
	}

	// ----------------------------------------------------------------------
	// Missing entity id (:id)
	// ----------------------------------------------------------------------

	// This causea a 404 from the API framework because there's no route for
	// this path
	etreurl := server.url + etre.API_ROOT + "/entity/" + entityType + "/"

	var gotWR etre.WriteResult
	statusCode, err := test.MakeHTTPRequest("PUT", etreurl, payload, &gotWR)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusNotFound {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusNotFound)
	}

	if diffs := deep.Equal(*gotWR.Error, api.ErrEndpointNotFound); diffs != nil {
		t.Errorf("got Error %+v, expected api.ErrEndpointNotFound %+v; diffs: %+v", gotWR.Error, api.ErrEndpointNotFound, diffs)
	}
	if len(gotWR.Writes) != 0 {
		t.Errorf("%d writes, exected 0: %+v", len(gotWR.Writes), gotWR.Writes)
	}

	if updated == true {
		t.Error("UpdateEntities called, expected no call due to error")
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}

	// ----------------------------------------------------------------------
	// Invalid entity id (:id)
	// ----------------------------------------------------------------------
	server.metricsrec.Reset()

	etreurl = server.url + etre.API_ROOT + "/entity/" + entityType + "/foo" // "foo" isn't valid

	gotWR = etre.WriteResult{}
	statusCode, err = test.MakeHTTPRequest("PUT", etreurl, payload, &gotWR)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusBadRequest {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusBadRequest)
	}

	if gotWR.Error == nil {
		t.Errorf("WriteResult.Error is nil, expected error message")
	} else if gotWR.Error.Type != "invalid-param" {
		t.Errorf("WriteResult.Error.Type = %s, expected invalid-param", gotWR.Error.Type)
	}
	if len(gotWR.Writes) != 0 {
		t.Errorf("%d writes, exected 0: %+v", len(gotWR.Writes), gotWR.Writes)
	}

	if updated == true {
		t.Error("UpdateEntities called, expected no call due to error")
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics = []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		// {Method: "Inc", Metric: metrics.UpdateId, IntVal: 1}, // not incremented because handler not called
		//{Method: "IncLabel", Metric: metrics.LabelUpdate, StringVal: "foo"}, // label in patch
		{Method: "Inc", Metric: metrics.ClientError, IntVal: 1},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}

	// ----------------------------------------------------------------------
	// Invalid patch
	// ----------------------------------------------------------------------
	server.metricsrec.Reset()

	// Patch can't have _id
	newEntity = etre.Entity{"_id": testEntityIds[0], "foo": "bar"}
	payload, err = json.Marshal(newEntity)
	if err != nil {
		t.Fatal(err)
	}

	etreurl = server.url + etre.API_ROOT + "/entity/" + entityType + "/" + testEntityIds[0]

	gotWR = etre.WriteResult{}
	statusCode, err = test.MakeHTTPRequest("PUT", etreurl, payload, &gotWR)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusBadRequest {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusBadRequest)
	}

	if gotWR.Error == nil {
		t.Errorf("WriteResult.Error is nil, expected error message")
	} else if gotWR.Error.Type != "cannot-change-metalabel" {
		t.Errorf("WriteResult.Error.Type = %s, expected cannot-change-metalabel", gotWR.Error.Type)
	}
	if len(gotWR.Writes) != 0 {
		t.Errorf("%d writes, exected 0: %+v", len(gotWR.Writes), gotWR.Writes)
	}

	if updated == true {
		t.Error("UpdateEntities called, expected no call due to error")
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics = []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.UpdateId, IntVal: 1},
		//{Method: "IncLabel", Metric: metrics.LabelUpdate, StringVal: "foo"}, // label in patch
		{Method: "Inc", Metric: metrics.ClientError, IntVal: 1},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}

	// ----------------------------------------------------------------------
	// Empty patch entity
	// ----------------------------------------------------------------------
	server.metricsrec.Reset()

	newEntity = etre.Entity{}
	payload, err = json.Marshal(newEntity)
	if err != nil {
		t.Fatal(err)
	}

	etreurl = server.url + etre.API_ROOT + "/entity/" + entityType + "/" + testEntityIds[0]

	gotWR = etre.WriteResult{}
	statusCode, err = test.MakeHTTPRequest("PUT", etreurl, payload, &gotWR)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusBadRequest {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusBadRequest)
	}

	if gotWR.Error == nil {
		t.Errorf("WriteResult.Error is nil, expected error message")
	} else if gotWR.Error.Type != "empty-entity" {
		t.Errorf("WriteResult.Error.Type = %s, expected empty-entity", gotWR.Error.Type)
	}
	if len(gotWR.Writes) != 0 {
		t.Errorf("%d writes, exected 0: %+v", len(gotWR.Writes), gotWR.Writes)
	}

	if updated == true {
		t.Error("UpdateEntities called, expected no call due to error")
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics = []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.UpdateId, IntVal: 1},
		//{Method: "IncLabel", Metric: metrics.LabelUpdate, StringVal: "foo"}, // label in patch
		{Method: "Inc", Metric: metrics.ClientError, IntVal: 1},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}

	// ----------------------------------------------------------------------
	// Bad JSON
	// ----------------------------------------------------------------------
	server.metricsrec.Reset()

	payload = []byte("bad")
	etreurl = server.url + etre.API_ROOT + "/entity/" + entityType + "/" + testEntityIds[0]

	gotWR = etre.WriteResult{}
	statusCode, err = test.MakeHTTPRequest("PUT", etreurl, payload, &gotWR)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusBadRequest {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusBadRequest)
	}

	if gotWR.Error == nil {
		t.Errorf("WriteResult.Error is nil, expected error message")
	} else if gotWR.Error.Type != "invalid-content" {
		t.Errorf("WriteResult.Error.Type = %s, expected invalid-content", gotWR.Error.Type)
	}
	if len(gotWR.Writes) != 0 {
		t.Errorf("%d writes, exected 0: %+v", len(gotWR.Writes), gotWR.Writes)
	}

	if updated == true {
		t.Error("UpdateEntities called, expected no call due to error")
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics = []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.UpdateId, IntVal: 1},
		//{Method: "IncLabel", Metric: metrics.LabelUpdate, StringVal: "foo"}, // label in patch
		{Method: "Inc", Metric: metrics.ClientError, IntVal: 1},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}
}

// //////////////////////////////////////////////////////////////////////////
// Delete
// //////////////////////////////////////////////////////////////////////////

func TestDeleteEntityOK(t *testing.T) {
	// Test that DELETE /entities/:type/:id works correctly. Under the hood, this
	// calls entity.Store.DeleteEntities() with the single entity from
	// the HTTP caller. So we expect a list of entities with just the one
	// sent (payload). The return are the old entities.
	var gotWO entity.WriteOp
	var gotQuery query.Query
	store := mock.EntityStore{
		DeleteEntitiesFunc: func(wo entity.WriteOp, q query.Query) ([]etre.Entity, error) {
			gotWO = wo
			gotQuery = q
			diff := []etre.Entity{
				{"_id": testEntityId0, "_type": entityType, "_rev": int64(0), "foo": "oldVal"},
			}
			return diff, nil
		},
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()

	newEntity := etre.Entity{"foo": "bar"}
	payload, err := json.Marshal(newEntity)
	if err != nil {
		t.Fatal(err)
	}

	etreurl := server.url + etre.API_ROOT + "/entity/" + entityType + "/" + testEntityIds[0]

	var gotWR etre.WriteResult
	statusCode, err := test.MakeHTTPRequest("DELETE", etreurl, payload, &gotWR)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}

	expectWR := etre.WriteResult{
		Writes: []etre.Write{
			{
				EntityId: testEntityIds[0],
				URI:      addr + etre.API_ROOT + "/entity/" + testEntityIds[0],
				Diff: etre.Entity{
					"_id":   testEntityIds[0],
					"_type": entityType,
					"_rev":  float64(0),
					"foo":   "oldVal",
				},
			},
		},
	}
	if diffs := deep.Equal(gotWR, expectWR); diffs != nil {
		t.Error(diffs)
	}

	expectWO := entity.WriteOp{
		Caller:     "test", // from mock.AuthPlugin
		EntityType: entityType,
		EntityId:   testEntityIds[0],
	}
	if diff := deep.Equal(gotWO, expectWO); diff != nil {
		t.Error(diff)
	}

	expectQuery, _ := query.Translate("_id=" + testEntityIds[0])
	if diff := deep.Equal(gotQuery, expectQuery); diff != nil {
		t.Error(diff)
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.DeleteId, IntVal: 1},
		{Method: "Inc", Metric: metrics.Deleted, IntVal: 1},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}
}

// //////////////////////////////////////////////////////////////////////////
// Delete label
// //////////////////////////////////////////////////////////////////////////

func TestDeleteLabel(t *testing.T) {
	// Test that DELETE /entities/:type/:id/label/:label works correctly
	var gotWO entity.WriteOp
	var gotLabel string
	store := mock.EntityStore{
		DeleteLabelFunc: func(wo entity.WriteOp, label string) (etre.Entity, error) {
			gotWO = wo
			gotLabel = label
			return etre.Entity{"_id": testEntityId0, "_type": entityType, "_rev": int64(0), "foo": "oldVal"}, nil
		},
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()

	etreurl := server.url + etre.API_ROOT + "/entity/" + entityType + "/" + testEntityIds[0] +
		"/labels/foo"

	var gotWR etre.WriteResult
	statusCode, err := test.MakeHTTPRequest("DELETE", etreurl, nil, &gotWR)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}

	expectWR := etre.WriteResult{
		Writes: []etre.Write{
			{
				EntityId: testEntityIds[0],
				URI:      addr + etre.API_ROOT + "/entity/" + testEntityIds[0],
				Diff: etre.Entity{
					"_id":   testEntityIds[0],
					"_type": entityType,
					"_rev":  float64(0),
					"foo":   "oldVal", // deleted label
				},
			},
		},
	}
	if diffs := deep.Equal(gotWR, expectWR); diffs != nil {
		t.Error(diffs)
	}

	expectWO := entity.WriteOp{
		Caller:     "test", // from mock.AuthPlugin
		EntityType: entityType,
		EntityId:   testEntityIds[0],
	}
	if diff := deep.Equal(gotWO, expectWO); diff != nil {
		t.Error(diff)
	}

	// Pretty important that the label from the URL is passed to
	// entity.Store.DeleteLabel(), i.e. we delete the correct label
	if gotLabel != "foo" {
		t.Errorf("got label %s, expected foo (from URL)", gotLabel)
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.DeleteLabel, IntVal: 1},
		{Method: "IncLabel", Metric: metrics.LabelDelete, StringVal: "foo"},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}
}
