// Copyright 2017-2020, Square, Inc.

package api_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	require.NoError(t, err)

	etreurl := server.url + etre.API_ROOT + "/entity/" + entityType

	var gotWR etre.WriteResult
	statusCode, err := test.MakeHTTPRequest("POST", etreurl, payload, &gotWR)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, statusCode)

	expectWR := etre.WriteResult{
		Writes: []etre.Write{
			{
				EntityId: "id1",
				URI:      addr + etre.API_ROOT + "/entity/id1",
			},
		},
	}
	assert.Equal(t, expectWR, gotWR)

	expectWO := entity.WriteOp{
		Caller:     "test", // from mock.AuthPlugin
		EntityType: entityType,
	}
	assert.Equal(t, expectWO, gotWO)

	expectEntities := []etre.Entity{newEntity}
	assert.Equal(t, expectEntities, gotEntities)

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.CreateOne, IntVal: 1},
		{Method: "Inc", Metric: metrics.Created, IntVal: 1},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	assert.Equal(t, expectMetrics, server.metricsrec.Called)
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
	require.NoError(t, err)

	etreurl := server.url + etre.API_ROOT + "/entity/" + entityType

	var gotWR etre.WriteResult
	statusCode, err := test.MakeHTTPRequest("POST", etreurl, payload, &gotWR)
	require.NoError(t, err)
	assert.Equal(t, http.StatusConflict, statusCode)
	require.Error(t, gotWR.Error)
	assert.NotEmpty(t, gotWR.Error.Type)
	assert.Len(t, gotWR.Writes, 0)

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
	assert.Equal(t, expectMetrics, server.metricsrec.Called)
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
	require.NoError(t, err)

	etreurl := server.url + etre.API_ROOT + "/entity/" + entityType

	var gotWR etre.WriteResult
	statusCode, err := test.MakeHTTPRequest("POST", etreurl, payload, &gotWR)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, statusCode)
	require.Error(t, gotWR.Error)
	assert.Equal(t, "cannot-set-metalabel", gotWR.Error.Type)
	assert.False(t, created, "CreateEntities called, expected no call due to error")

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
	assert.Equal(t, expectMetrics, server.metricsrec.Called)

	// ----------------------------------------------------------------------
	// No value/zero entities (can't create nothing)
	// ----------------------------------------------------------------------
	server.metricsrec.Reset()

	newEntity = etre.Entity{}
	payload, err = json.Marshal(newEntity)
	require.NoError(t, err)

	gotWR = etre.WriteResult{}
	statusCode, err = test.MakeHTTPRequest("POST", etreurl, payload, &gotWR)
	t.Logf("got WriteResult: %+v", gotWR)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, statusCode)
	require.Error(t, gotWR.Error)
	assert.Equal(t, "empty-entity", gotWR.Error.Type)
	assert.False(t, created, "CreateEntities called, expected no call due to error")

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
	assert.Equal(t, expectMetrics, server.metricsrec.Called)

	// ----------------------------------------------------------------------
	// Invalid JSON
	// ----------------------------------------------------------------------
	server.metricsrec.Reset()

	payload = []byte("bad")

	gotWR = etre.WriteResult{}
	statusCode, err = test.MakeHTTPRequest("POST", etreurl, payload, &gotWR)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, statusCode)
	require.Error(t, gotWR.Error)
	assert.Equal(t, "invalid-content", gotWR.Error.Type)
	assert.False(t, created, "CreateEntities called, expected no call due to error")

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
	assert.Equal(t, expectMetrics, server.metricsrec.Called)
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
	require.NoError(t, err)

	etreurl := server.url + etre.API_ROOT + "/entity/" + entityType + "/" + testEntityIds[0]

	var gotWR etre.WriteResult
	statusCode, err := test.MakeHTTPRequest("PUT", etreurl, payload, &gotWR)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, statusCode)

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
	assert.Equal(t, expectWR, gotWR)

	expectWO := entity.WriteOp{
		Caller:     "test", // from mock.AuthPlugin
		EntityType: entityType,
		EntityId:   testEntityIds[0],
	}
	assert.Equal(t, expectWO, gotWO)

	expectQuery, _ := query.Translate("_id=" + testEntityIds[0])
	assert.Equal(t, expectQuery, gotQuery)

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
	assert.Equal(t, expectMetrics, server.metricsrec.Called)
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
	require.NoError(t, err)

	etreurl := server.url + etre.API_ROOT + "/entity/" + entityType + "/" + testEntityIds[0]

	var gotWR etre.WriteResult
	statusCode, err := test.MakeHTTPRequest("PUT", etreurl, payload, &gotWR)
	require.NoError(t, err)
	assert.Equal(t, http.StatusConflict, statusCode)
	require.Error(t, gotWR.Error)
	assert.Equal(t, "duplicate-entity", gotWR.Error.Type)
	assert.Len(t, gotWR.Writes, 0)

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
	assert.Equal(t, expectMetrics, server.metricsrec.Called)
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
	require.NoError(t, err)

	etreurl := server.url + etre.API_ROOT + "/entity/" + entityType + "/" + testEntityIds[0]

	var gotWR etre.WriteResult
	statusCode, err := test.MakeHTTPRequest("PUT", etreurl, payload, &gotWR)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, statusCode)
	require.Error(t, gotWR.Error)
	assert.Equal(t, "entity-not-found", gotWR.Error.Type)
	assert.Empty(t, gotWR.Writes)

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
	assert.Equal(t, expectMetrics, server.metricsrec.Called)
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
	require.NoError(t, err)

	// ----------------------------------------------------------------------
	// Missing entity id (:id)
	// ----------------------------------------------------------------------

	// This causea a 404 from the API framework because there's no route for
	// this path
	etreurl := server.url + etre.API_ROOT + "/entity/" + entityType + "/"

	var gotWR etre.WriteResult
	statusCode, err := test.MakeHTTPRequest("PUT", etreurl, payload, &gotWR)
	require.NoError(t, err)

	assert.Equal(t, http.StatusNotFound, statusCode)
	assert.Equal(t, *gotWR.Error, api.ErrEndpointNotFound)
	assert.Len(t, gotWR.Writes, 0)
	assert.False(t, updated, "UpdateEntities called, expected no call due to error")

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{}
	assert.Equal(t, expectMetrics, server.metricsrec.Called)

	// ----------------------------------------------------------------------
	// Invalid entity id (:id)
	// ----------------------------------------------------------------------
	server.metricsrec.Reset()

	etreurl = server.url + etre.API_ROOT + "/entity/" + entityType + "/foo" // "foo" isn't valid

	gotWR = etre.WriteResult{}
	statusCode, err = test.MakeHTTPRequest("PUT", etreurl, payload, &gotWR)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, statusCode)
	require.Error(t, gotWR.Error)
	assert.Equal(t, "invalid-param", gotWR.Error.Type)
	assert.Empty(t, gotWR.Writes)
	assert.False(t, updated, "UpdateEntities called, expected no call due to error")

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
	assert.Equal(t, expectMetrics, server.metricsrec.Called)

	// ----------------------------------------------------------------------
	// Invalid patch
	// ----------------------------------------------------------------------
	server.metricsrec.Reset()

	// Patch can't have _id
	newEntity = etre.Entity{"_id": testEntityIds[0], "foo": "bar"}
	payload, err = json.Marshal(newEntity)
	require.NoError(t, err)

	etreurl = server.url + etre.API_ROOT + "/entity/" + entityType + "/" + testEntityIds[0]

	gotWR = etre.WriteResult{}
	statusCode, err = test.MakeHTTPRequest("PUT", etreurl, payload, &gotWR)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, statusCode)
	require.Error(t, gotWR.Error)
	assert.Equal(t, "cannot-change-metalabel", gotWR.Error.Type)
	assert.Len(t, gotWR.Writes, 0)
	assert.False(t, updated, "UpdateEntities called, expected no call due to error")

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
	assert.Equal(t, expectMetrics, server.metricsrec.Called)

	// ----------------------------------------------------------------------
	// Empty patch entity
	// ----------------------------------------------------------------------
	server.metricsrec.Reset()

	newEntity = etre.Entity{}
	payload, err = json.Marshal(newEntity)
	require.NoError(t, err)

	etreurl = server.url + etre.API_ROOT + "/entity/" + entityType + "/" + testEntityIds[0]

	gotWR = etre.WriteResult{}
	statusCode, err = test.MakeHTTPRequest("PUT", etreurl, payload, &gotWR)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, statusCode)
	require.Error(t, gotWR.Error)
	assert.Equal(t, "empty-entity", gotWR.Error.Type)
	assert.Len(t, gotWR.Writes, 0)
	assert.False(t, updated, "UpdateEntities called, expected no call due to error")

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
	assert.Equal(t, expectMetrics, server.metricsrec.Called)

	// ----------------------------------------------------------------------
	// Bad JSON
	// ----------------------------------------------------------------------
	server.metricsrec.Reset()

	payload = []byte("bad")
	etreurl = server.url + etre.API_ROOT + "/entity/" + entityType + "/" + testEntityIds[0]

	gotWR = etre.WriteResult{}
	statusCode, err = test.MakeHTTPRequest("PUT", etreurl, payload, &gotWR)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, statusCode)
	require.Error(t, gotWR.Error)
	assert.Equal(t, "invalid-content", gotWR.Error.Type)
	assert.Len(t, gotWR.Writes, 0)
	assert.False(t, updated, "UpdateEntities called, expected no call due to error")

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
	assert.Equal(t, expectMetrics, server.metricsrec.Called)
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
	require.NoError(t, err)

	etreurl := server.url + etre.API_ROOT + "/entity/" + entityType + "/" + testEntityIds[0]

	var gotWR etre.WriteResult
	statusCode, err := test.MakeHTTPRequest("DELETE", etreurl, payload, &gotWR)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, statusCode)

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
	assert.Equal(t, expectWR, gotWR)

	expectWO := entity.WriteOp{
		Caller:     "test", // from mock.AuthPlugin
		EntityType: entityType,
		EntityId:   testEntityIds[0],
	}
	assert.Equal(t, expectWO, gotWO)

	expectQuery, _ := query.Translate("_id=" + testEntityIds[0])
	assert.Equal(t, expectQuery, gotQuery)

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.DeleteId, IntVal: 1},
		{Method: "Inc", Metric: metrics.Deleted, IntVal: 1},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	assert.Equal(t, expectMetrics, server.metricsrec.Called)
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
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, statusCode)

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
	assert.Equal(t, expectWR, gotWR)

	expectWO := entity.WriteOp{
		Caller:     "test", // from mock.AuthPlugin
		EntityType: entityType,
		EntityId:   testEntityIds[0],
	}
	assert.Equal(t, expectWO, gotWO)

	// Pretty important that the label from the URL is passed to
	// entity.Store.DeleteLabel(), i.e. we delete the correct label
	assert.Equal(t, "foo", gotLabel)

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Write, IntVal: 1},
		{Method: "Inc", Metric: metrics.DeleteLabel, IntVal: 1},
		{Method: "IncLabel", Metric: metrics.LabelDelete, StringVal: "foo"},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	assert.Equal(t, expectMetrics, server.metricsrec.Called)
}
