// Copyright 2017-2020, Square, Inc.

package api_test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/stretchr/testify/require"

	"github.com/square/etre"
	"github.com/square/etre/api"
	"github.com/square/etre/entity"
	"github.com/square/etre/metrics"
	"github.com/square/etre/query"
	"github.com/square/etre/test"
	"github.com/square/etre/test/mock"
)

func TestQueryBasic(t *testing.T) {
	// Test the most basic GET /entities?query=Q, no filter options
	var gotQuery query.Query
	var gotFilter etre.QueryFilter
	store := mock.EntityStore{
		ReadEntitiesFunc: func(entityType string, q query.Query, f etre.QueryFilter) ([]etre.Entity, error) {
			gotQuery = q
			gotFilter = f
			return testEntitiesWithObjectIDs, nil
		},
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()

	q := "host=local"
	etreurl := server.url + etre.API_ROOT + "/entities/" + entityType +
		"?query=" + url.QueryEscape(q)

	var gotEntities []etre.Entity
	statusCode, err := test.MakeHTTPRequest("GET", etreurl, nil, &gotEntities)
	require.NoError(t, err)

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}

	expectQuery, _ := query.Translate(q)
	if diff := deep.Equal(gotQuery, expectQuery); diff != nil {
		t.Error(diff)
	}

	expectFilter := etre.QueryFilter{}
	if diff := deep.Equal(gotFilter, expectFilter); diff != nil {
		t.Error(diff)
	}

	fixRev(gotEntities) // JSON float64(_rev) ->, int64(_rev)
	if diffs := deep.Equal(gotEntities, testEntities); diffs != nil {
		t.Error(diffs)
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Read, IntVal: 1},
		{Method: "Inc", Metric: metrics.ReadQuery, IntVal: 1},
		{Method: "Val", Metric: metrics.Labels, IntVal: 1},                 // label in query
		{Method: "IncLabel", Metric: metrics.LabelRead, StringVal: "host"}, // label in query
		{Method: "Val", Metric: metrics.ReadMatch, IntVal: 3},              // len(testEntities)
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}

	// ----------------------------------------------------------------------
	// Multi-label query
	// ----------------------------------------------------------------------
	server.metricsrec.Reset()

	q = "host=local,env=production"
	etreurl = server.url + etre.API_ROOT + "/entities/" + entityType +
		"?query=" + url.QueryEscape(q)

	statusCode, err = test.MakeHTTPRequest("GET", etreurl, nil, &gotEntities)
	require.NoError(t, err)

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}

	expectQuery, _ = query.Translate(q)
	if diff := deep.Equal(gotQuery, expectQuery); diff != nil {
		t.Error(diff)
	}

	expectFilter = etre.QueryFilter{}
	if diff := deep.Equal(gotFilter, expectFilter); diff != nil {
		t.Error(diff)
	}

	// Don't care about the return from the mock store, that was tested above

	// -- Metrics -----------------------------------------------------------
	expectMetrics = []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Read, IntVal: 1},
		{Method: "Inc", Metric: metrics.ReadQuery, IntVal: 1},
		{Method: "Val", Metric: metrics.Labels, IntVal: 2},                 // label in query
		{Method: "IncLabel", Metric: metrics.LabelRead, StringVal: "host"}, // label in query
		{Method: "IncLabel", Metric: metrics.LabelRead, StringVal: "env"},  // label in query
		{Method: "Val", Metric: metrics.ReadMatch, IntVal: 3},              // len(testEntities)
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}

	// ----------------------------------------------------------------------
	// Single-label exists query
	// ----------------------------------------------------------------------
	server.metricsrec.Reset()

	q = "active"
	etreurl = server.url + etre.API_ROOT + "/entities/" + entityType +
		"?query=" + url.QueryEscape(q)

	statusCode, err = test.MakeHTTPRequest("GET", etreurl, nil, &gotEntities)
	require.NoError(t, err)

	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}

	expectQuery, _ = query.Translate(q)
	if diff := deep.Equal(gotQuery, expectQuery); diff != nil {
		t.Error(diff)
	}

	expectFilter = etre.QueryFilter{}
	if diff := deep.Equal(gotFilter, expectFilter); diff != nil {
		t.Error(diff)
	}

	// Don't care about the return from the mock store, that was tested above

	// -- Metrics -----------------------------------------------------------
	expectMetrics = []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Read, IntVal: 1},
		{Method: "Inc", Metric: metrics.ReadQuery, IntVal: 1},
		{Method: "Val", Metric: metrics.Labels, IntVal: 1},                   // label in query
		{Method: "IncLabel", Metric: metrics.LabelRead, StringVal: "active"}, // label in query
		{Method: "Val", Metric: metrics.ReadMatch, IntVal: 3},                // len(testEntities)
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}
}

func TestQueryNoMatches(t *testing.T) {
	// Test when GET /entities?query=Q matches no queries. This is simulated by
	// returning an empty list of entities in the mock func below. The HTTP response
	// is still 200 OK in this case because there's no error.
	var gotQuery query.Query
	store := mock.EntityStore{
		ReadEntitiesFunc: func(entityType string, q query.Query, f etre.QueryFilter) ([]etre.Entity, error) {
			gotQuery = q
			return []etre.Entity{}, nil // no matching queries
		},
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()

	q := "host=local"
	etreurl := server.url + etre.API_ROOT + "/entities/" + entityType +
		"?query=" + url.QueryEscape(q)

	var gotEntities []etre.Entity
	statusCode, err := test.MakeHTTPRequest("GET", etreurl, nil, &gotEntities)
	require.NoError(t, err)

	// HTTP response is still 200 OK because query was ok, there just weren't
	// any matching queries
	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}

	expectQuery, _ := query.Translate(q)
	if diff := deep.Equal(gotQuery, expectQuery); diff != nil {
		t.Error(diff)
	}

	// Empty result, no matching queries
	if diffs := deep.Equal(gotEntities, []etre.Entity{}); diffs != nil {
		t.Error(diffs)
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Read, IntVal: 1},
		{Method: "Inc", Metric: metrics.ReadQuery, IntVal: 1},
		{Method: "Val", Metric: metrics.Labels, IntVal: 1},
		{Method: "IncLabel", Metric: metrics.LabelRead, StringVal: "host"},
		{Method: "Val", Metric: metrics.ReadMatch, IntVal: 0}, // no matching queries
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}
}

// --------------------------------------------------------------------------
// Errors
// --------------------------------------------------------------------------

func TestQueryErrorsDatabaseError(t *testing.T) {
	// Test that GET /entities/:type?query=Q handles a database error correctly.
	// Db errors (and only db errors return HTTP 503 "Service Unavailable".
	store := mock.EntityStore{
		ReadEntitiesFunc: func(entityType string, q query.Query, f etre.QueryFilter) ([]etre.Entity, error) {
			return nil, entity.DbError{Err: fmt.Errorf("fake error"), Type: "db-read"}
		},
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()

	etreurl := server.url + etre.API_ROOT + "/entities/" + entityType +
		"?query=" + url.QueryEscape("a=b")

	var gotError etre.Error
	statusCode, err := test.MakeHTTPRequest("GET", etreurl, nil, &gotError)
	require.NoError(t, err)

	if statusCode != http.StatusServiceUnavailable { // 503
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusServiceUnavailable)
	}

	expectError := etre.Error{
		Message:    "fake error",
		Type:       "db-read",
		HTTPStatus: http.StatusServiceUnavailable, // 503
	}
	if diffs := deep.Equal(gotError, expectError); diffs != nil {
		t.Error(diffs)
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Read, IntVal: 1},
		{Method: "Inc", Metric: metrics.ReadQuery, IntVal: 1},
		{Method: "Val", Metric: metrics.Labels, IntVal: 1},
		{Method: "IncLabel", Metric: metrics.LabelRead, StringVal: "a"},
		{Method: "Inc", Metric: metrics.DbError, IntVal: 1}, // db error
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}
}

func TestQueryErrorsNoEntityType(t *testing.T) {
	// Test that GET /entities?query=Q returns an error because /:type is missing.
	// Caller knows this is "URL not found" not "no entities found" because the
	// endpoint always returns 200 evenif no entities are found. But the API itself
	// must return 404 when an endpoint/route is called that isn't defined, and
	// this one isn't because the "/:type" part of the URL path is required,
	// i.e. no handler for GET /entities is defined.
	//
	// You can run "../test/coverage -test.run TestQueryErrorsNoEntityType" and
	// see that the handler is never called.
	store := mock.EntityStore{
		ReadEntitiesFunc: func(entityType string, q query.Query, f etre.QueryFilter) ([]etre.Entity, error) {
			return nil, entity.DbError{Err: fmt.Errorf("fake error"), Type: "db-read"}
		},
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()

	etreurl := server.url + etre.API_ROOT + "/entities?query=" + url.QueryEscape("a=b")

	var gotError etre.Error
	statusCode, err := test.MakeHTTPRequest("GET", etreurl, nil, &gotError)
	require.NoError(t, err)

	if statusCode != http.StatusNotFound {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusNotFound)
	}

	expectError := api.ErrEndpointNotFound
	if diffs := deep.Equal(gotError, expectError); diffs != nil {
		t.Error(diffs)
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}

	// ----------------------------------------------------------------------
	// With trailing slash to ensure API doesn't pass "" for :type
	// ----------------------------------------------------------------------
	etreurl = server.url + etre.API_ROOT + "/entities/?query=" + url.QueryEscape("a=b")
	gotError = etre.Error{}
	statusCode, err = test.MakeHTTPRequest("GET", etreurl, nil, &gotError)
	require.NoError(t, err)

	if statusCode != http.StatusNotFound {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusNotFound)
	}
	if diffs := deep.Equal(gotError, expectError); diffs != nil {
		t.Error(diffs)
	}

	// -- Metrics -----------------------------------------------------------
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}
}

func TestQueryErrorsTimeout(t *testing.T) {
	// Test that GET /entities/:type?query=Q handles a database timeout correctly.
	// Db errors (and only db errors return HTTP 503 "Service Unavailable".
	store := mock.EntityStore{
		ReadEntitiesFunc: func(entityType string, q query.Query, f etre.QueryFilter) ([]etre.Entity, error) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			<-ctx.Done()
			return nil, entity.DbError{Err: ctx.Err(), Type: "db-read"}
		},
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()

	etreurl := server.url + etre.API_ROOT + "/entities/" + entityType +
		"?query=" + url.QueryEscape("a=b")

	var gotError etre.Error
	statusCode, err := test.MakeHTTPRequest("GET", etreurl, nil, &gotError)
	require.NoError(t, err)

	if statusCode != http.StatusServiceUnavailable { // 503
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusServiceUnavailable)
	}

	if gotError.Type != "db-read" {
		t.Errorf("got etre.Error.Type = %s, expected \"db-read\": %+v", gotError.Type, gotError)
	}

	if len(server.metricsrec.Called) == 8 {
		if server.metricsrec.Called[7].Metric != metrics.LatencyMs || server.metricsrec.Called[7].IntVal < 90 || server.metricsrec.Called[7].IntVal > 150 {
			t.Errorf("metrics.LatencyMs = %d, expected 90-150ms", server.metricsrec.Called[7].IntVal)
		}
		server.metricsrec.Called[7].IntVal = 0
	} else {
		t.Errorf("got %d metrics, expected 8 (can't check metrics.LatencyMs, see below)", len(server.metricsrec.Called))
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "EntityType", StringVal: entityType},
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
		{Method: "Inc", Metric: metrics.Read, IntVal: 1},
		{Method: "Inc", Metric: metrics.ReadQuery, IntVal: 1},
		{Method: "Val", Metric: metrics.Labels, IntVal: 1},
		{Method: "IncLabel", Metric: metrics.LabelRead, StringVal: "a"},
		{Method: "Inc", Metric: metrics.QueryTimeout, IntVal: 1}, // query timeout
		{Method: "Val", Metric: metrics.LatencyMs, IntVal: 0},
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}
}

func TestResponseCompression(t *testing.T) {
	// Stand up the server
	store := mock.EntityStore{
		ReadEntitiesFunc: func(entityType string, q query.Query, f etre.QueryFilter) ([]etre.Entity, error) {
			return testEntitiesWithObjectIDs, nil
		},
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()

	// Make the http request.
	etreurl := server.url + etre.API_ROOT + "/entities/" + entityType +
		"?query=" + url.QueryEscape("foo=bar")
	req, err := http.NewRequest("GET", etreurl, nil)
	require.NoError(t, err)

	// Make the request
	// Note that the http client automatically enables gzip, so we don't have to set the "Accept-Encoding" header.
	res, err := http.DefaultClient.Do(req)
	defer res.Body.Close()
	require.NoError(t, err)

	// The http client strips the "Content-Encoding" header so we can't check it directly.
	// Instead, we have to check the "Uncompressed" flag, which will be *true* if the content came back compressed and was decompressed by the http client.
	if !res.Uncompressed {
		t.Errorf("response was not compressed, expected it to be")
	}

	// Make sure content type is correct
	if res.Header.Get("Content-Type") != "application/json" {
		t.Errorf("response Content-Type = %s, expected application/json", res.Header.Get("Content-Type"))
	}
}
