// Copyright 2017-2019, Square, Inc.

package api_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/go-test/deep"
	"github.com/stretchr/testify/require"

	"github.com/square/etre"
	"github.com/square/etre/entity"
	"github.com/square/etre/metrics"
	"github.com/square/etre/query"
	"github.com/square/etre/test"
	"github.com/square/etre/test/mock"
)

// --------------------------------------------------------------------------
// Insert
// --------------------------------------------------------------------------

func TestPostEntitiesOK(t *testing.T) {
	// Test that POST /entities handler calls store.CreateEntities with the
	// posted entities (client's HTTP payload), and passes it a correct WriteOp,
	// increments metrics correctly, and returns a correct WriteResult. Apart
	// from validation (covered in other tests), that's all the handler does.
	var gotWO entity.WriteOp
	var gotEntities []etre.Entity
	store := mock.EntityStore{
		CreateEntitiesFunc: func(wo entity.WriteOp, entities []etre.Entity) ([]string, error) {
			gotWO = wo
			gotEntities = entities
			return []string{"id1", "id2"}, nil
		},
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()

	// On create, entities can't have metalabels, so we can't use testEntities
	entities := []etre.Entity{{"a": "1"}, {"b": "2"}}
	payload, err := json.Marshal(entities)
	require.NoError(t, err)

	var gotWR etre.WriteResult
	url := server.url + etre.API_ROOT + "/entities/" + entityType
	statusCode, err := test.MakeHTTPRequest("POST", url, payload, &gotWR)
	t.Logf("got WriteResult: %+v", gotWR)
	require.NoError(t, err)

	if statusCode != http.StatusCreated {
		t.Errorf("got HTTP status = %d, expected %d: %+v", statusCode, http.StatusCreated, gotWR)
	}

	expectWR := etre.WriteResult{
		Writes: []etre.Write{
			{EntityId: "id1", URI: uri("id1")},
			{EntityId: "id2", URI: uri("id2")},
		},
	}
	if diff := deep.Equal(gotWR, expectWR); diff != nil {
		t.Error(diff)
	}

	expectWO := entity.WriteOp{
		Caller:     "test", // from mock.AuthPlugin
		EntityType: entityType,
	}
	if diff := deep.Equal(gotWO, expectWO); diff != nil {
		t.Error(diff)
	}

	if diff := deep.Equal(gotEntities, entities); diff != nil {
		t.Error(diff)
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.CreateMany, IntVal: 1},
		{Method: "Val", Metric: metrics.CreateBulk, IntVal: 2},
		{Method: "Inc", Metric: metrics.Created, IntVal: 2},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}
}

func TestPostEntitiesErrors(t *testing.T) {
	// Test that POST /entities handler validate the clients HTTP payload.
	// If invalid, it should return an etre.WriteResult with an error.
	// Most importantly: CreateEntities() should _not_ be called.
	created := false
	store := mock.EntityStore{
		CreateEntitiesFunc: func(wo entity.WriteOp, entities []etre.Entity) ([]string, error) {
			created = true
			return []string{"id1", "id2"}, nil
		},
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()
	url := server.url + etre.API_ROOT + "/entities/" + entityType

	// On create, entities can't have metalabels, sou use testEntities
	// which have all the metalabels. This should cause an error.
	payload, err := json.Marshal(testEntities)
	require.NoError(t, err)

	var gotWR etre.WriteResult
	statusCode, err := test.MakeHTTPRequest("POST", url, payload, &gotWR)
	t.Logf("got WriteResult: %+v", gotWR)
	require.NoError(t, err)

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
		{Method: "Inc", Metric: metrics.CreateMany, IntVal: 1},
		{Method: "Val", Metric: metrics.CreateBulk, IntVal: 3}, // len(testEntities)
		//{Method: "Inc", Metric: metrics.Created, IntVal: 2}, // NOT created due to error:
		{Method: "Inc", Metric: metrics.ClientError, IntVal: 1},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}

	// ----------------------------------------------------------------------
	// Invalid value type
	// ----------------------------------------------------------------------
	server.metricsrec.Reset()

	// Arrays are not supported values types, so this should cause a similar error
	yArr := []string{"foo", "bar", "baz"}
	entity1 := etre.Entity{"x": 2}    // ok
	entity2 := etre.Entity{"y": yArr} // invalid
	entities := []etre.Entity{entity1, entity2}

	payload, err = json.Marshal(entities)
	require.NoError(t, err)

	gotWR = etre.WriteResult{}
	statusCode, err = test.MakeHTTPRequest("POST", url, payload, &gotWR)
	t.Logf("got WriteResult: %+v", gotWR)
	require.NoError(t, err)

	if statusCode != http.StatusBadRequest {
		t.Errorf("got HTTP status = %d, expected %d: %+v", statusCode, http.StatusBadRequest, gotWR)
	}
	if gotWR.Error == nil {
		t.Errorf("WriteResult.Error is nil, expected error message")
	} else if gotWR.Error.Type != "invalid-value-type" {
		t.Errorf("WriteResult.Error.Type = %s, expected invalid-value-type", gotWR.Error.Type)
	}
	if created == true {
		t.Error("CreateEntities called, expected no call due to error")
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics = []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.CreateMany, IntVal: 1},
		{Method: "Val", Metric: metrics.CreateBulk, IntVal: 2}, // len(entities)
		//{Method: "Inc", Metric: metrics.Created, IntVal: 2}, // NOT created due to error:
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

	// Arrays are not supported values types, so this should cause a similar error
	entities = []etre.Entity{}
	payload, err = json.Marshal(entities)
	require.NoError(t, err)

	gotWR = etre.WriteResult{}
	statusCode, err = test.MakeHTTPRequest("POST", url, payload, &gotWR)
	t.Logf("got WriteResult: %+v", gotWR)
	require.NoError(t, err)

	if statusCode != http.StatusBadRequest {
		t.Errorf("got HTTP status = %d, expected %d: %+v", statusCode, http.StatusBadRequest, gotWR)
	}
	if gotWR.Error == nil {
		t.Errorf("WriteResult.Error is nil, expected error message")
	} else if gotWR.Error.Type != "no-content" {
		t.Errorf("WriteResult.Error.Type = %s, expected no-content", gotWR.Error.Type)
	}
	if created == true {
		t.Error("CreateEntities called, expected no call due to error")
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics = []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.CreateMany, IntVal: 1},
		// {Method: "Val", Metric: metrics.CreateBulk, IntVal: 2}, // NOT created due to error:
		//{Method: "Inc", Metric: metrics.Created, IntVal: 2}, // NOT created due to error:
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
	statusCode, err = test.MakeHTTPRequest("POST", url, payload, &gotWR)
	t.Logf("got WriteResult: %+v", gotWR)
	require.NoError(t, err)

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
		{Method: "Inc", Metric: metrics.CreateMany, IntVal: 1},
		// {Method: "Val", Metric: metrics.CreateBulk, IntVal: 2}, // NOT created due to error:
		//{Method: "Inc", Metric: metrics.Created, IntVal: 2}, // NOT created due to error:
		{Method: "Inc", Metric: metrics.ClientError, IntVal: 1},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}
}

// --------------------------------------------------------------------------
// Update
// --------------------------------------------------------------------------

func TestPutEntitiesOK(t *testing.T) {
	// Test that PUT /entities handler passes all the correct values to
	// UpdateEntities() which would update the matching entities. For this
	// test, we just ensure that the store func (UpdateEntities) gets the
	// right values. The store func itself is tested in entity/store_test.go.
	// Also verifying that all the right metrics are incremented.
	var gotWO entity.WriteOp
	var gotQuery query.Query
	var gotPatch etre.Entity
	store := mock.EntityStore{
		UpdateEntitiesFunc: func(wo entity.WriteOp, q query.Query, patch etre.Entity) ([]etre.Entity, error) {
			gotWO = wo
			gotQuery = q
			gotPatch = patch
			diff := []etre.Entity{
				{"_id": testEntityId0, "_type": entityType, "_rev": int64(0), "foo": "oldVal"},
			}
			return diff, nil
		},
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()

	// Set foo=bar on all matching entities (metrics +1 update on "foo")
	patch := etre.Entity{"foo": "bar"}
	payload, err := json.Marshal(patch)
	require.NoError(t, err)

	// Match entities with a=b (metrics +1 read on "a")
	etreurl := server.url + etre.API_ROOT + "/entities/" + entityType +
		"?query=" + url.QueryEscape("a=b")

	var gotWR etre.WriteResult
	statusCode, err := test.MakeHTTPRequest("PUT", etreurl, payload, &gotWR)
	t.Logf("got WriteResult: %+v", gotWR)
	require.NoError(t, err)

	if statusCode != http.StatusOK {
		t.Errorf("got HTTP status = %d, expected %d: %+v", statusCode, http.StatusOK, gotWR)
	}

	// Returns the old values for the patched labels
	expectWR := etre.WriteResult{
		Writes: []etre.Write{
			{
				EntityId: testEntityIds[0],
				URI:      uri(testEntityIds[0]),
				Diff: etre.Entity{
					"_id":   testEntityIds[0],
					"_type": entityType,
					"_rev":  float64(0), // float64 because all JSON numbers are float
					"foo":   "oldVal",
				},
			},
		},
	}
	if diff := deep.Equal(gotWR, expectWR); diff != nil {
		t.Error(diff)
	}

	expectWO := entity.WriteOp{
		Caller:     "test", // from mock.AuthPlugin
		EntityType: entityType,
	}
	if diff := deep.Equal(gotWO, expectWO); diff != nil {
		t.Error(diff)
	}

	expectQuery, _ := query.Translate("a=b")
	if diff := deep.Equal(gotQuery, expectQuery); diff != nil {
		t.Error(diff)
	}

	if diff := deep.Equal(gotPatch, patch); diff != nil {
		t.Error(diff)
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.UpdateQuery, IntVal: 1},
		{Method: "Val", Metric: metrics.Labels, IntVal: 1},
		{Method: "IncLabel", Metric: metrics.LabelRead, StringVal: "a"},     // label in query
		{Method: "IncLabel", Metric: metrics.LabelUpdate, StringVal: "foo"}, // label in patch
		{Method: "Val", Metric: metrics.UpdateBulk, IntVal: 1},
		{Method: "Inc", Metric: metrics.Updated, IntVal: 1},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}
}

func TestPutEntitiesErrors(t *testing.T) {
	// Test that PUT /entities returns the proper errors and increments the proper
	// metrics when any input is invalid. The UpdateEntities() should not be called.
	updated := false
	store := mock.EntityStore{
		UpdateEntitiesFunc: func(wo entity.WriteOp, q query.Query, patch etre.Entity) ([]etre.Entity, error) {
			updated = true
			return []etre.Entity{}, nil
		},
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()

	// ----------------------------------------------------------------------
	// No query (cannot update all entities)
	// ----------------------------------------------------------------------
	etreurl := server.url + etre.API_ROOT + "/entities/" + entityType // missing ?query=...
	patch := etre.Entity{"foo": "bar"}
	payload, err := json.Marshal(patch)
	require.NoError(t, err)

	var gotWR etre.WriteResult
	statusCode, err := test.MakeHTTPRequest("PUT", etreurl, payload, &gotWR)
	t.Logf("got WriteResult: %+v", gotWR)
	require.NoError(t, err)

	if statusCode != http.StatusBadRequest {
		t.Errorf("got HTTP status = %d, expected %d: %+v", statusCode, http.StatusBadRequest, gotWR)
	}

	if gotWR.Error == nil {
		t.Errorf("WriteResult.Error is nil, expected error message")
	} else if gotWR.Error.Type != "invalid-query" {
		t.Errorf("WriteResult.Error.Type = %s, expected invalid-query", gotWR.Error.Type)
	}

	if updated == true {
		t.Error("UpdateEntities called, expected no call due to error")
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.UpdateQuery, IntVal: 1},
		{Method: "Inc", Metric: metrics.ClientError, IntVal: 1}, // error
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}

	// ----------------------------------------------------------------------
	// Empty query
	// ----------------------------------------------------------------------
	server.metricsrec.Reset()
	etreurl = server.url + etre.API_ROOT + "/entities/" + entityType + "?query="
	gotWR = etre.WriteResult{}
	statusCode, err = test.MakeHTTPRequest("PUT", etreurl, payload, &gotWR)
	t.Logf("got WriteResult: %+v", gotWR)
	require.NoError(t, err)

	if statusCode != http.StatusBadRequest {
		t.Errorf("got HTTP status = %d, expected %d: %+v", statusCode, http.StatusBadRequest, gotWR)
	}

	if gotWR.Error == nil {
		t.Errorf("WriteResult.Error is nil, expected error message")
	} else if gotWR.Error.Type != "invalid-query" {
		t.Errorf("WriteResult.Error.Type = %s, expected invalid-query", gotWR.Error.Type)
	}

	if updated == true {
		t.Error("UpdateEntities called, expected no call due to error")
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics = []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.UpdateQuery, IntVal: 1},
		{Method: "Inc", Metric: metrics.ClientError, IntVal: 1}, // error
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}

	// ----------------------------------------------------------------------
	// Invalid query
	// ----------------------------------------------------------------------
	server.metricsrec.Reset()

	etreurl = server.url + etre.API_ROOT + "/entities/" + entityType +
		"?query=" + url.QueryEscape("*foo=bar") // "*foo" is not a valid label

	patch = etre.Entity{"foo": "bar"}
	payload, err = json.Marshal(patch)
	require.NoError(t, err)

	gotWR = etre.WriteResult{}
	statusCode, err = test.MakeHTTPRequest("PUT", etreurl, payload, &gotWR)
	t.Logf("got WriteResult: %+v", gotWR)
	require.NoError(t, err)

	if statusCode != http.StatusBadRequest {
		t.Errorf("got HTTP status = %d, expected %d: %+v", statusCode, http.StatusBadRequest, gotWR)
	}

	if gotWR.Error == nil {
		t.Errorf("WriteResult.Error is nil, expected error message")
	} else if gotWR.Error.Type != "invalid-query" {
		t.Errorf("WriteResult.Error.Type = %s, expected invalid-query", gotWR.Error.Type)
	}

	if updated == true {
		t.Error("UpdateEntities called, expected no call due to error")
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics = []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.UpdateQuery, IntVal: 1},
		{Method: "Inc", Metric: metrics.ClientError, IntVal: 1}, // error
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}

	// ----------------------------------------------------------------------
	// No patch (empty payload)
	// ----------------------------------------------------------------------
	server.metricsrec.Reset()

	patch = etre.Entity{}
	payload, err = json.Marshal(patch)
	require.NoError(t, err)
	etreurl = server.url + etre.API_ROOT + "/entities/" + entityType +
		"?query=" + url.QueryEscape("a=b")

	gotWR = etre.WriteResult{}
	statusCode, err = test.MakeHTTPRequest("PUT", etreurl, payload, &gotWR)
	t.Logf("got WriteResult: %+v", gotWR)
	require.NoError(t, err)

	if statusCode != http.StatusBadRequest {
		t.Errorf("got HTTP status = %d, expected %d: %+v", statusCode, http.StatusBadRequest, gotWR)
	}

	if gotWR.Error == nil {
		t.Errorf("WriteResult.Error is nil, expected error message")
	} else if gotWR.Error.Type != "no-content" {
		t.Errorf("WriteResult.Error.Type = %s, expected no-content", gotWR.Error.Type)
	}

	if updated == true {
		t.Error("UpdateEntities called, expected no call due to error")
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics = []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.UpdateQuery, IntVal: 1},
		{Method: "Inc", Metric: metrics.ClientError, IntVal: 1}, // error
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}

	// ----------------------------------------------------------------------
	// Invalid payload (but valid JSON): cannot patch metalabels
	// ----------------------------------------------------------------------
	server.metricsrec.Reset()

	patch = etre.Entity{"_type": "newType"} // can't change metalabels
	payload, err = json.Marshal(patch)
	require.NoError(t, err)
	etreurl = server.url + etre.API_ROOT + "/entities/" + entityType +
		"?query=" + url.QueryEscape("a=b")
	gotWR = etre.WriteResult{}
	statusCode, err = test.MakeHTTPRequest("PUT", etreurl, payload, &gotWR)
	t.Logf("got WriteResult: %+v", gotWR)
	require.NoError(t, err)

	if statusCode != http.StatusBadRequest {
		t.Errorf("got HTTP status = %d, expected %d: %+v", statusCode, http.StatusBadRequest, gotWR)
	}

	if gotWR.Error == nil {
		t.Errorf("WriteResult.Error is nil, expected error message")
	} else if gotWR.Error.Type != "cannot-change-metalabel" {
		t.Errorf("WriteResult.Error.Type = %s, expected cannot-change-metalabel", gotWR.Error.Type)
	}

	if updated == true {
		t.Error("UpdateEntities called, expected no call due to error")
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics = []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.UpdateQuery, IntVal: 1},
		{Method: "Inc", Metric: metrics.ClientError, IntVal: 1}, // error
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}
}

// --------------------------------------------------------------------------
// Delete
// --------------------------------------------------------------------------

func TestDeleteEntitiesOK(t *testing.T) {
	// Test that DELETE /entities handler passes all the correct values to
	// DeleteEntities() which would delete the matching entities. This test
	// is almost identialy to PUT /entities.
	var gotWO entity.WriteOp
	var gotQuery query.Query
	store := mock.EntityStore{
		DeleteEntitiesFunc: func(wo entity.WriteOp, q query.Query) ([]etre.Entity, error) {
			gotWO = wo
			gotQuery = q
			return []etre.Entity{
				{"_id": testEntityId0, "_type": entityType, "_rev": int64(0), "foo": "oldVal"},
			}, nil
		},
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()

	// Set foo=bar on all matching entities (metrics +1 update on "foo")
	patch := etre.Entity{"foo": "bar"}
	payload, err := json.Marshal(patch)
	require.NoError(t, err)

	// Match entities with a=b (metrics +1 read on "a")
	etreurl := server.url + etre.API_ROOT + "/entities/" + entityType +
		"?query=" + url.QueryEscape("a=b")

	var gotWR etre.WriteResult
	statusCode, err := test.MakeHTTPRequest("DELETE", etreurl, payload, &gotWR)
	t.Logf("got WriteResult: %+v", gotWR)
	require.NoError(t, err)

	if statusCode != http.StatusOK {
		t.Errorf("got HTTP status = %d, expected %d: %+v", statusCode, http.StatusOK, gotWR)
	}

	// Returns the old values for the patched labels
	expectWR := etre.WriteResult{
		Writes: []etre.Write{
			{
				EntityId: testEntityIds[0],
				URI:      uri(testEntityIds[0]),
				Diff: etre.Entity{
					"_id":   testEntityIds[0],
					"_type": entityType,
					"_rev":  float64(0), // float64 because all JSON numbers are float
					"foo":   "oldVal",
				},
			},
		},
	}
	if diff := deep.Equal(gotWR, expectWR); diff != nil {
		t.Error(diff)
	}

	expectWO := entity.WriteOp{
		Caller:     "test", // from mock.AuthPlugin
		EntityType: entityType,
	}
	if diff := deep.Equal(gotWO, expectWO); diff != nil {
		t.Error(diff)
	}

	expectQuery, _ := query.Translate("a=b")
	if diff := deep.Equal(gotQuery, expectQuery); diff != nil {
		t.Error(diff)
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.DeleteQuery, IntVal: 1},
		{Method: "Val", Metric: metrics.Labels, IntVal: 1},
		{Method: "IncLabel", Metric: metrics.LabelRead, StringVal: "a"}, // label in query
		{Method: "Val", Metric: metrics.DeleteBulk, IntVal: 1},
		{Method: "Inc", Metric: metrics.Deleted, IntVal: 1},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}
}

func TestDeleteEntitiesErrors(t *testing.T) {
	// Test that DELETE /entities returns the proper errors and increments the proper
	// metrics when any input is invalid. The DeleteEntities() should not be called.
	deleted := false
	store := mock.EntityStore{
		DeleteEntitiesFunc: func(wo entity.WriteOp, q query.Query) ([]etre.Entity, error) {
			deleted = true
			return []etre.Entity{}, nil
		},
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()

	// ----------------------------------------------------------------------
	// No query (cannot update all entities)
	// ----------------------------------------------------------------------
	etreurl := server.url + etre.API_ROOT + "/entities/" + entityType // missing ?query=...
	patch := etre.Entity{"foo": "bar"}
	payload, err := json.Marshal(patch)
	require.NoError(t, err)

	var gotWR etre.WriteResult
	statusCode, err := test.MakeHTTPRequest("DELETE", etreurl, payload, &gotWR)
	t.Logf("got WriteResult: %+v", gotWR)
	require.NoError(t, err)

	if statusCode != http.StatusBadRequest {
		t.Errorf("got HTTP status = %d, expected %d: %+v", statusCode, http.StatusBadRequest, gotWR)
	}

	if gotWR.Error == nil {
		t.Errorf("WriteResult.Error is nil, expected error message")
	} else if gotWR.Error.Type != "invalid-query" {
		t.Errorf("WriteResult.Error.Type = %s, expected invalid-query", gotWR.Error.Type)
	}

	if deleted == true {
		t.Error("DeleteEntities called, expected no call due to error")
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.DeleteQuery, IntVal: 1},
		{Method: "Inc", Metric: metrics.ClientError, IntVal: 1}, // error
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}

	// ----------------------------------------------------------------------
	// Empty query
	// ----------------------------------------------------------------------
	server.metricsrec.Reset()
	etreurl = server.url + etre.API_ROOT + "/entities/" + entityType + "?query="
	gotWR = etre.WriteResult{}
	statusCode, err = test.MakeHTTPRequest("DELETE", etreurl, payload, &gotWR)
	t.Logf("got WriteResult: %+v", gotWR)
	require.NoError(t, err)

	if statusCode != http.StatusBadRequest {
		t.Errorf("got HTTP status = %d, expected %d: %+v", statusCode, http.StatusBadRequest, gotWR)
	}

	if gotWR.Error == nil {
		t.Errorf("WriteResult.Error is nil, expected error message")
	} else if gotWR.Error.Type != "invalid-query" {
		t.Errorf("WriteResult.Error.Type = %s, expected invalid-query", gotWR.Error.Type)
	}

	if deleted == true {
		t.Error("DeleteEntities called, expected no call due to error")
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics = []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.DeleteQuery, IntVal: 1},
		{Method: "Inc", Metric: metrics.ClientError, IntVal: 1}, // error
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}

	// ----------------------------------------------------------------------
	// Invalid query
	// ----------------------------------------------------------------------
	server.metricsrec.Reset()

	etreurl = server.url + etre.API_ROOT + "/entities/" + entityType +
		"?query=" + url.QueryEscape("*foo=bar") // "*foo" is not a valid label

	patch = etre.Entity{"foo": "bar"}
	payload, err = json.Marshal(patch)
	require.NoError(t, err)

	gotWR = etre.WriteResult{}
	statusCode, err = test.MakeHTTPRequest("DELETE", etreurl, payload, &gotWR)
	t.Logf("got WriteResult: %+v", gotWR)
	require.NoError(t, err)

	if statusCode != http.StatusBadRequest {
		t.Errorf("got HTTP status = %d, expected %d: %+v", statusCode, http.StatusBadRequest, gotWR)
	}

	if gotWR.Error == nil {
		t.Errorf("WriteResult.Error is nil, expected error message")
	} else if gotWR.Error.Type != "invalid-query" {
		t.Errorf("WriteResult.Error.Type = %s, expected invalid-query", gotWR.Error.Type)
	}

	if deleted == true {
		t.Error("DeleteEntities called, expected no call due to error")
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics = []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.DeleteQuery, IntVal: 1},
		{Method: "Inc", Metric: metrics.ClientError, IntVal: 1}, // error
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}
}
