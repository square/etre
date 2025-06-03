// Copyright 2017-2020, Square, Inc.

package api_test

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/square/etre"
	"github.com/square/etre/api"
	"github.com/square/etre/auth"
	"github.com/square/etre/entity"
	"github.com/square/etre/metrics"
	"github.com/square/etre/query"
	"github.com/square/etre/test"
	"github.com/square/etre/test/mock"
)

// //////////////////////////////////////////////////////////////////////////
// Read Entity
// //////////////////////////////////////////////////////////////////////////

func TestGetEntityBasic(t *testing.T) {
	// Test the most basic GET /entity/:type/:id gets the entity. This is
	// really a wrapper to call ReadEntitiesFunc() with _id=:id.
	var gotQuery query.Query
	var gotFilter etre.QueryFilter
	store := mock.EntityStore{
		ReadEntitiesFunc: func(entityType string, q query.Query, f etre.QueryFilter) ([]etre.Entity, error) {
			gotQuery = q
			gotFilter = f
			return testEntitiesWithObjectIDs[0:1], nil
		},
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()

	etreurl := server.url + etre.API_ROOT + "/entity/" + entityType + "/" + testEntityIds[0]

	var gotEntity etre.Entity
	statusCode, err := test.MakeHTTPRequest("GET", etreurl, nil, &gotEntity)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, statusCode, "response status = %d, expected %d", statusCode, http.StatusOK)

	// GET /entity/:type/:id = "_id=:id"
	expectQuery, _ := query.Translate("_id=" + testEntityIds[0])
	assert.Equal(t, expectQuery, gotQuery)

	// No filter options provided in URL
	expectFilter := etre.QueryFilter{}
	assert.Equal(t, expectFilter, gotFilter)

	fixInt64([]etre.Entity{gotEntity}) // JSON float64(_rev) ->, int64(_rev)
	assert.Equal(t, testEntities[0], gotEntity)

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Read, IntVal: 1},
		{Method: "Inc", Metric: metrics.ReadId, IntVal: 1},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	fixLatencyMetric(t, 150, expectMetrics, server.metricsrec.Called)
	assert.Equal(t, expectMetrics, server.metricsrec.Called)

	// -- Auth -----------------------------------------------------------
	require.Len(t, server.auth.AuthenticateArgs, 1)
	assert.Equal(t, []mock.AuthorizeArgs{{
		Action: auth.Action{Op: auth.OP_READ, EntityType: entityType},
		Caller: auth.Caller{Name: "test", MetricGroups: []string{"test"}},
	}}, server.auth.AuthorizeArgs)
}

func TestGetEntityReturnLabels(t *testing.T) {
	// Test that GET /entity/:type/:id works with etre.QueryFilter.ReturnLabels.
	// The real entity.Store does this and is tested in that pkg, so here we're testing
	// that the URL param "labels=" is processed and passed along to the entity.Store.
	var gotFilter etre.QueryFilter
	store := mock.EntityStore{
		ReadEntitiesFunc: func(entityType string, q query.Query, f etre.QueryFilter) ([]etre.Entity, error) {
			gotFilter = f
			return testEntitiesWithObjectIDs[0:1], nil
		},
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()

	etreurl := server.url + etre.API_ROOT + "/entity/" + entityType + "/" + testEntityIds[0] +
		"?labels=" + url.QueryEscape("a,b")

	var gotEntity etre.Entity
	statusCode, err := test.MakeHTTPRequest("GET", etreurl, nil, &gotEntity)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, statusCode)

	// Here's what we're looking for. The handler will parse "labels=a,b" from the URL
	// into a etre.QueryFilter{} and pass it to the entity.Store.
	expectFilter := etre.QueryFilter{
		ReturnLabels: []string{"a", "b"},
	}
	assert.Equal(t, expectFilter, gotFilter)
	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Read, IntVal: 1},
		{Method: "Inc", Metric: metrics.ReadId, IntVal: 1},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	fixLatencyMetric(t, 150, expectMetrics, server.metricsrec.Called)
	assert.Equal(t, expectMetrics, server.metricsrec.Called)

	// -- Auth -----------------------------------------------------------
	require.Len(t, server.auth.AuthenticateArgs, 1)
	assert.Equal(t, []mock.AuthorizeArgs{{
		Action: auth.Action{Op: auth.OP_READ, EntityType: entityType},
		Caller: auth.Caller{Name: "test", MetricGroups: []string{"test"}},
	}}, server.auth.AuthorizeArgs)
}

func TestGetEntityNotFound(t *testing.T) {
	// Test that GET /entity/:type/:id returns 404 when the entity doesn't exist.
	// We simulate this by making ReadEntities() below return an empty list which
	// the real entity.Store() does when no entity exists with the given _id.
	store := mock.EntityStore{
		ReadEntitiesFunc: func(entityType string, q query.Query, f etre.QueryFilter) ([]etre.Entity, error) {
			return []etre.Entity{}, nil
		},
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()

	etreurl := server.url + etre.API_ROOT + "/entity/" + entityType + "/" + testEntityIds[0]

	var gotEntity etre.Entity
	statusCode, err := test.MakeHTTPRequest("GET", etreurl, nil, &gotEntity)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, statusCode)

	// No HTTP response body
	assert.Nil(t, gotEntity)

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Read, IntVal: 1},
		{Method: "Inc", Metric: metrics.ReadId, IntVal: 1},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	fixLatencyMetric(t, 150, expectMetrics, server.metricsrec.Called)
	assert.Equal(t, expectMetrics, server.metricsrec.Called)
}

func TestGetEntityErrors(t *testing.T) {
	// Test that GET /entity/:type/:id returns correct errors
	read := false
	var dbError error
	store := mock.EntityStore{
		ReadEntitiesFunc: func(entityType string, q query.Query, f etre.QueryFilter) ([]etre.Entity, error) {
			read = true
			return nil, dbError
		},
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()

	var gotError, expectError etre.Error

	// ----------------------------------------------------------------------
	// Invalid entity id
	// ----------------------------------------------------------------------

	// "foo" is not a valid MongoDB object ID
	etreurl := server.url + etre.API_ROOT + "/entity/" + entityType + "/foo"

	gotError = etre.Error{}
	statusCode, err := test.MakeHTTPRequest("GET", etreurl, nil, &gotError)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, statusCode)
	assert.Equal(t, "invalid-param", gotError.Type)
	assert.False(t, read, "ReadEntities called, expected no call due to error")

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Read, IntVal: 1},
		// {Method: "Inc", Metric: metrics.ReadId, IntVal: 1}, // metric not incremented because handler not called
		{Method: "Inc", Metric: metrics.ClientError, IntVal: 1}, // error
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	fixLatencyMetric(t, 150, expectMetrics, server.metricsrec.Called)
	assert.Equal(t, expectMetrics, server.metricsrec.Called)

	// ----------------------------------------------------------------------
	// Missing entity id (/:id)
	// ----------------------------------------------------------------------
	server.metricsrec.Reset()
	etreurl = server.url + etre.API_ROOT + "/entity/" + entityType + "/" // + testEntityIds[0]

	gotError = etre.Error{}
	statusCode, err = test.MakeHTTPRequest("GET", etreurl, nil, &gotError)
	require.NoError(t, err)

	// This is 404 not 400 (bad request) because there's no endpoint for /entity/:type
	assert.Equal(t, http.StatusNotFound, statusCode)
	expectError = api.ErrEndpointNotFound
	assert.Equal(t, expectError, gotError)
	assert.False(t, read, "ReadEntities called, expected no call due to error")

	// -- Metrics -----------------------------------------------------------
	expectMetrics = []mock.MetricMethodArgs{}
	fixLatencyMetric(t, 150, expectMetrics, server.metricsrec.Called)
	assert.Equal(t, expectMetrics, server.metricsrec.Called)

	// ----------------------------------------------------------------------
	// Db error
	// ----------------------------------------------------------------------
	server.metricsrec.Reset()

	dbError = entity.DbError{Err: fmt.Errorf("fake error"), Type: "db-read"}

	etreurl = server.url + etre.API_ROOT + "/entity/" + entityType + "/" + testEntityIds[0]
	gotError = etre.Error{}
	statusCode, err = test.MakeHTTPRequest("GET", etreurl, nil, &gotError)
	require.NoError(t, err)

	assert.Equal(t, http.StatusServiceUnavailable, statusCode)
	assert.NotEmpty(t, gotError.Message)

	// -- Metrics -----------------------------------------------------------
	expectMetrics = []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Read, IntVal: 1},
		{Method: "Inc", Metric: metrics.ReadId, IntVal: 1},
		{Method: "Inc", Metric: metrics.DbError, IntVal: 1}, // error
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	fixLatencyMetric(t, 150, expectMetrics, server.metricsrec.Called)
	assert.Equal(t, expectMetrics, server.metricsrec.Called)
}

// //////////////////////////////////////////////////////////////////////////
// Read Labels
// //////////////////////////////////////////////////////////////////////////

func TestGetEntityLabels(t *testing.T) {
	// Test that GET /entity/:type/:id/labels works
	var gotQuery query.Query
	var gotFilter etre.QueryFilter
	store := mock.EntityStore{
		ReadEntitiesFunc: func(entityType string, q query.Query, f etre.QueryFilter) ([]etre.Entity, error) {
			gotQuery = q
			gotFilter = f
			return testEntitiesWithObjectIDs[0:1], nil
		},
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()

	etreurl := server.url + etre.API_ROOT + "/entity/" + entityType + "/" + testEntityIds[0] + "/labels"

	var gotLabels []string
	statusCode, err := test.MakeHTTPRequest("GET", etreurl, nil, &gotLabels)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, statusCode)

	// GET /entity/:type/:id = "_id=:id"
	expectQuery, _ := query.Translate("_id=" + testEntityIds[0])
	assert.Equal(t, expectQuery, gotQuery)

	// No filter options provided in URL
	expectFilter := etre.QueryFilter{}
	assert.Equal(t, expectFilter, gotFilter)

	expectLabels := testEntities[0].Labels()
	assert.Equal(t, expectLabels, gotLabels)

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Read, IntVal: 1},
		{Method: "Inc", Metric: metrics.ReadLabels, IntVal: 1},
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	fixLatencyMetric(t, 150, expectMetrics, server.metricsrec.Called)
	assert.Equal(t, expectMetrics, server.metricsrec.Called)

	// -- Auth -----------------------------------------------------------
	require.Len(t, server.auth.AuthenticateArgs, 1)
	assert.Equal(t, []mock.AuthorizeArgs{{
		Action: auth.Action{Op: auth.OP_READ, EntityType: entityType},
		Caller: auth.Caller{Name: "test", MetricGroups: []string{"test"}},
	}}, server.auth.AuthorizeArgs)
}
