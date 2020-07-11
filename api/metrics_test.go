// Copyright 2017-2020, Square, Inc.

package api_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/go-test/deep"

	"github.com/square/etre"
	"github.com/square/etre/api"
	"github.com/square/etre/app"
	"github.com/square/etre/auth"
	"github.com/square/etre/config"
	"github.com/square/etre/metrics"
	"github.com/square/etre/test"
	"github.com/square/etre/test/mock"
)

func setupWithMetrics(t *testing.T, cfg config.Config, store mock.EntityStore) *server {
	// The same real metrics used in server.Init()
	ms := metrics.NewMemoryStore()
	mf := metrics.GroupFactory{Store: ms}
	sm := metrics.NewSystemMetrics()
	server := &server{
		store: store,
		cfg:   cfg,
	}
	appCtx := app.Context{
		Config:          server.cfg,
		EntityStore:     server.store,
		EntityValidator: validate,
		Auth:            auth.NewManager(nil, auth.NewAllowAll()),
		MetricsStore:    ms,
		MetricsFactory:  mf,
		SystemMetrics:   sm,
	}
	server.api = api.NewAPI(appCtx)
	server.ts = httptest.NewServer(server.api)

	u, err := url.Parse(server.ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	server.url = fmt.Sprintf("http://%s", u.Host)

	return server
}

// //////////////////////////////////////////////////////////////////////////
// Metrics
// //////////////////////////////////////////////////////////////////////////

func TestMetricsGet(t *testing.T) {
	// GET /metrics should return 200, i.e. not panic or any other number of
	// failures because metrics reporting requires several moving parts
	// the list of entity types in the metrics
	server := setupWithMetrics(t, defaultConfig, mock.EntityStore{})
	defer server.ts.Close()

	etreurl := server.url + etre.API_ROOT + "/entities/" + entityType + "?query=x"
	statusCode, err := test.MakeHTTPRequest("GET", etreurl, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if statusCode != http.StatusOK {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusOK)
	}

	// GET /metrics
	etreurl = server.url + etre.API_ROOT + "/metrics"
	var gotMetrics etre.Metrics
	statusCode, err = test.MakeHTTPRequest("GET", etreurl, nil, &gotMetrics)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusOK {
		t.Errorf("got HTTP status = %d, expected %d", statusCode, http.StatusOK)
	}

	if gotMetrics.System == nil {
		t.Errorf("Metrics.System is nil, expected value")
	}

	if len(gotMetrics.Groups) != 1 {
		t.Errorf("got %d Metrics.Groups, expected 1: %+v", len(gotMetrics.Groups), gotMetrics.Groups)
	}
}

/*
func TestMetricsInvalidEntityType(t *testing.T) {
	// Invalid entity types should not generate metrics, i.e. don't pollute
	// the list of entity types in the metrics
	url := defaultServer.URL + etre.API_ROOT + "/entity/bad-type/" + seedId0
	var actual etre.Entity
	statusCode, err := test.MakeHTTPRequest("GET", url, nil, &actual)
	if err != nil {
		t.Fatal(err)
	}
	if statusCode != http.StatusBadRequest {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusBadRequest)
	}

	// No group/entity metrics because the entity type was invalid
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "Inc", Metric: metrics.InvalidEntityType, IntVal: 1},
		{Method: "Inc", Metric: metrics.ClientError, IntVal: 1},
	}
	if diffs := deep.Equal(metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got (em): %+v", metricsrec.Called)
		t.Logf("expect (em): %+v", expectMetrics)
		t.Error(diffs)
	}

	// System metrics
	expectMetrics = []mock.MetricMethodArgs{
		{Method: "Inc", Metric: metrics.Query, IntVal: 1},
	}
	if diffs := deep.Equal(sysmetrics.Called, expectMetrics); diffs != nil {
		t.Logf("   got (sys): %+v", sysmetrics.Called)
		t.Logf("expect (sys): %+v", expectMetrics)
		t.Error(diffs)
	}
}
*/

func TestMetricsBadRoute(t *testing.T) {
	// Test that metrics, especially Load, are correct when route is 404 and not
	// under /api/v1/ route group. This addresses a bug where Load could be negative
	// because the post-route hook is called that did Load -= 1 but the pre-route
	// hook that did Load += 1 was never called because the route wasn't under that
	// route group.
	server := setup(t, defaultConfig, mock.EntityStore{})
	defer server.ts.Close()

	etreurl := server.url + "/api" // bad route, not under route group
	statusCode, err := test.MakeHTTPRequest("GET", etreurl, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if statusCode != http.StatusNotFound {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusNotFound)
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}
	if diffs := deep.Equal(server.sysmetrics.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.sysmetrics.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}

}
