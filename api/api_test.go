// Copyright 2017-2020, Square, Inc.

package api_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/go-test/deep"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/square/etre"
	"github.com/square/etre/api"
	"github.com/square/etre/app"
	"github.com/square/etre/auth"
	"github.com/square/etre/config"
	"github.com/square/etre/entity"
	"github.com/square/etre/metrics"
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
	if err != nil {
		t.Fatalf("invalid Config.ACL: %s", err)
	}

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
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	if statusCode != http.StatusOK {
		t.Errorf("got HTTP status = %d, expected %d", statusCode, http.StatusOK)
	}
	expectStatus := map[string]string{
		"ok":      "yes",
		"version": etre.VERSION,
	}
	if diff := deep.Equal(gotStatus, expectStatus); diff != nil {
		t.Error(diff)
	}
}

func TestValidateEntityType(t *testing.T) {
	server := setup(t, defaultConfig, mock.EntityStore{})
	defer server.ts.Close()

	// ----------------------------------------------------------------------
	// Read
	etreurl := server.url + etre.API_ROOT + "/entity/invalid/" + testEntityIds[0]

	var gotError etre.Error
	statusCode, err := test.MakeHTTPRequest("GET", etreurl, nil, &gotError)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusBadRequest {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusBadRequest)
	}
	if gotError.Message == "" {
		t.Errorf("no error message in etre.Error, expected one: %+v", gotError)
	}

	// -- Metrics -----------------------------------------------------------
	expectMetrics := []mock.MetricMethodArgs{
		{Method: "Inc", Metric: metrics.InvalidEntityType, IntVal: 1},
		{Method: "Inc", Metric: metrics.ClientError, IntVal: 1}, // error
	}
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}

	// ----------------------------------------------------------------------
	// Write
	server.metricsrec.Reset()

	etreurl = server.url + etre.API_ROOT + "/entity/invalid/" + testEntityIds[0]

	var gotWR etre.WriteResult
	statusCode, err = test.MakeHTTPRequest("PUT", etreurl, nil, &gotWR)
	if err != nil {
		t.Fatal(err)
	}

	if statusCode != http.StatusBadRequest {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusBadRequest)
	}

	if gotWR.Error == nil {
		t.Errorf("WriteResult.Error is nil, expected error message")
	} else if gotWR.Error.Type != "invalid-entity-type" {
		t.Errorf("WriteResult.Error.Type = %s, expected invalid-entity-type", gotWR.Error.Type)
	}

	// -- Metrics -----------------------------------------------------------
	if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
		t.Logf("   got: %+v", server.metricsrec.Called)
		t.Logf("expect: %+v", expectMetrics)
		t.Error(diffs)
	}
}
