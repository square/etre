// Copyright 2017-2020, Square, Inc.

package api_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/square/etre"
	"github.com/square/etre/api"
	"github.com/square/etre/app"
	"github.com/square/etre/auth"
	"github.com/square/etre/config"
	"github.com/square/etre/entity"
	"github.com/square/etre/metrics"
	"github.com/square/etre/query"
	srv "github.com/square/etre/server"
	"github.com/square/etre/test"
	"github.com/square/etre/test/mock"
)

var (
	addr       = "http://localhost"
	entityType = "nodes"
	validate   = entity.NewValidator([]string{entityType})
	cfg        config.Config
)

type server struct {
	cfg             config.Config
	store           mock.EntityStore
	api             *api.API
	ts              *httptest.Server
	url             string
	auth            *mock.AuthPlugin
	cdcStore        *mock.CDCStore
	streamerFactory *mock.StreamerFactory
	metricsrec      *mock.MetricRecorder
	sysmetrics      *mock.MetricRecorder
}

var testEntities = []etre.Entity{
	{"_id": "59f10d2a5669fc79103a0000", "_type": "node", "_rev": int64(0), "x": "1", "foo": "bar"},
	{"_id": "59f10d2a5669fc79103a1111", "_type": "node", "_rev": int64(0), "x": "2", "foo": "bar"},
	{"_id": "59f10d2a5669fc79103a2222", "_type": "node", "_rev": int64(0), "x": "3", "foo": "bar"},
}

var testEntityIds = []string{"59f10d2a5669fc79103a0000", "59f10d2a5669fc79103a1111", "59f10d2a5669fc79103a2222"}

var (
	testEntityId0, _ = primitive.ObjectIDFromHex(testEntityIds[0])
	testEntityId1, _ = primitive.ObjectIDFromHex(testEntityIds[1])
	testEntityId2, _ = primitive.ObjectIDFromHex(testEntityIds[2])
)

var testEntitiesWithObjectIDs = []etre.Entity{
	{"_id": testEntityId0, "_type": "node", "_rev": int64(0), "x": "1", "foo": "bar"},
	{"_id": testEntityId1, "_type": "node", "_rev": int64(0), "x": "2", "foo": "bar"},
	{"_id": testEntityId2, "_type": "node", "_rev": int64(0), "x": "3", "foo": "bar"},
}

var defaultConfig = config.Config{
	Server: config.ServerConfig{
		Addr: addr,
	},
	Datasource: config.DatasourceConfig{
		QueryTimeout: config.DEFAULT_DB_QUERY_TIMEOUT,
	},
}

func setup(t *testing.T, cfg config.Config, store mock.EntityStore) *server {
	etre.DebugEnabled = true

	server := &server{
		store:           store,
		cfg:             cfg,
		auth:            &mock.AuthPlugin{},
		cdcStore:        &mock.CDCStore{},
		streamerFactory: &mock.StreamerFactory{},
		metricsrec:      mock.NewMetricsRecorder(),
		sysmetrics:      mock.NewMetricsRecorder(),
	}

	acls, err := srv.MapConfigACLRoles(cfg.Security.ACL)
	require.NoError(t, err, "invalid Config.ACL: %s", err)

	appCtx := app.Context{
		Config:          server.cfg,
		EntityStore:     server.store,
		EntityValidator: validate,
		Auth:            auth.NewManager(acls, server.auth),
		MetricsStore:    mock.MetricsStore{},
		MetricsFactory:  mock.MetricsFactory{MetricRecorder: server.metricsrec},
		StreamerFactory: server.streamerFactory,
		SystemMetrics:   server.sysmetrics,
	}
	server.api = api.NewAPI(appCtx)
	server.ts = httptest.NewServer(server.api)

	u, err := url.Parse(server.ts.URL)
	require.NoError(t, err)

	server.url = fmt.Sprintf("http://%s", u.Host)

	return server
}

func uri(id string) string {
	return addr + etre.API_ROOT + "/entity/" + id
}

func fixRev(e []etre.Entity) {
	for i := range e {
		f := e[i]["_rev"].(float64)
		delete(e[i], "_rev")
		e[i]["_rev"] = int64(f)
	}
}

// --------------------------------------------------------------------------

func TestStatus(t *testing.T) {
	// Test that GET /status works, returns HTTP 200
	server := setup(t, defaultConfig, mock.EntityStore{})
	defer server.ts.Close()

	var gotStatus map[string]string
	url := server.url + etre.API_ROOT + "/status"
	statusCode, err := test.MakeHTTPRequest("GET", url, nil, &gotStatus)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, statusCode)
	expectStatus := map[string]string{
		"ok":      "yes",
		"version": etre.VERSION,
	}
	assert.Equal(t, expectStatus, gotStatus)
}

func TestValidateEntityType(t *testing.T) {
	server := setup(t, defaultConfig, mock.EntityStore{})
	defer server.ts.Close()

	// ----------------------------------------------------------------------
	// Read
	etreurl := server.url + etre.API_ROOT + "/entity/invalid/" + testEntityIds[0]

	var gotError etre.Error
	statusCode, err := test.MakeHTTPRequest("GET", etreurl, nil, &gotError)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, statusCode)
	assert.NotEmpty(t, gotError.Message)

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "Inc", Metric: metrics.InvalidEntityType, IntVal: 1},
		{Method: "Inc", Metric: metrics.ClientError, IntVal: 1}, // error
	}
	assert.Equal(t, expectMetrics, server.metricsrec.Called)

	// ----------------------------------------------------------------------
	// Write
	server.metricsrec.Reset()

	etreurl = server.url + etre.API_ROOT + "/entity/invalid/" + testEntityIds[0]

	var gotWR etre.WriteResult
	statusCode, err = test.MakeHTTPRequest("PUT", etreurl, nil, &gotWR)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, statusCode)
	require.NotNil(t, gotWR.Error)
	assert.Equal(t, "invalid-entity-type", gotWR.Error.Type)

	// -- Metrics -----------------------------------------------------------
	assert.Equal(t, expectMetrics, server.metricsrec.Called)
}

func TestClientQueryTimeout(t *testing.T) {
	// Test client header X-Etre-Query-Timeout (etre.QUERY_TIMEOUT_HEADER) is
	// used in lieu of the server default (config.datasource.query_timeout)
	// and plumbed all the way down to the entity.Store context
	var gotCtx context.Context
	store := mock.EntityStore{}
	store.WithContextFunc = func(ctx context.Context) entity.Store {
		gotCtx = ctx
		return store
	}
	store.ReadEntitiesFunc = func(entityType string, q query.Query, f etre.QueryFilter) ([]etre.Entity, error) {
		return testEntitiesWithObjectIDs[0:1], nil
	}
	server := setup(t, defaultConfig, store)
	defer server.ts.Close()

	// ----------------------------------------------------------------------
	// Default server value: config.DEFAULT_DB_QUERY_TIMEOUT = 2s

	etreurl := server.url + etre.API_ROOT + "/entity/" + entityType + "/" + testEntityIds[0]
	var gotEntity etre.Entity
	statusCode, err := test.MakeHTTPRequest("GET", etreurl, nil, &gotEntity)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, statusCode)

	// If set properly, the deadline of the context will be very close to 2s.
	// This is a number number because the deadline is in the future.
	gotDeadline, set := gotCtx.Deadline()
	d := time.Now().Sub(gotDeadline).Seconds()
	assert.True(t, set, "query timeout deadline not set, expected it to be set")
	assert.True(t, -d >= 1.8 && -d <= 2.2, "deadline %f, expected between 1.8-2.2s (2s default)", d)

	// ----------------------------------------------------------------------
	// Client passes X-Etre-Query-Timeout
	test.Headers = map[string]string{
		etre.QUERY_TIMEOUT_HEADER: "5s",
	}
	defer func() { test.Headers = map[string]string{} }()

	statusCode, err = test.MakeHTTPRequest("GET", etreurl, nil, &gotEntity)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, statusCode)
	gotDeadline, set = gotCtx.Deadline()
	d = time.Now().Sub(gotDeadline).Seconds()
	assert.True(t, set, "query timeout deadline not set, expected it to be set")
	assert.True(t, -d >= 4.8 && -d <= 5.2, "deadline %f, expected between 4.8-5.2s (5s client)", d)
}
