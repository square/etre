// Copyright 2017-2020, Square, Inc.

package api_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	require.NoError(t, err)

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
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, statusCode)

	// GET /metrics
	etreurl = server.url + etre.API_ROOT + "/metrics"
	var gotMetrics etre.Metrics
	statusCode, err = test.MakeHTTPRequest("GET", etreurl, nil, &gotMetrics)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, statusCode)
	assert.NotNil(t, gotMetrics.System)
	assert.Len(t, gotMetrics.Groups, 1)
}

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
	require.NoError(t, err)

	assert.Equal(t, http.StatusNotFound, statusCode)

	// -- Metrics -----------------------------------------------------------
	assert.Empty(t, server.metricsrec.Called)
	assert.Empty(t, server.sysmetrics.Called)
}
