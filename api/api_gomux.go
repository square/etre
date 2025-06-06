// Copyright 2017-2020, Square, Inc.

// Package api provides API endpoints and controllers.
package api

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	httpSwagger "github.com/swaggo/http-swagger"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/square/etre"
	"github.com/square/etre/app"
	"github.com/square/etre/auth"
	"github.com/square/etre/cdc/changestream"
	"github.com/square/etre/docs"
	"github.com/square/etre/entity"
	"github.com/square/etre/metrics"
	"github.com/square/etre/query"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const reqKey = "rc"

type req struct {
	ctx        context.Context
	caller     auth.Caller
	wo         entity.WriteOp
	gm         metrics.Metrics
	inst       app.Instrument
	entityType string
	entityId   string
	write      bool
}

// API provides controllers for endpoints it registers with a router.
type API struct {
	addr                     string
	crt                      string
	key                      string
	es                       entity.Store
	validate                 entity.Validator
	auth                     auth.Plugin
	metricsStore             metrics.Store
	cdcDisabled              bool
	streamFactory            changestream.StreamerFactory
	metricsFactory           metrics.Factory
	systemMetrics            metrics.Metrics
	queryTimeout             time.Duration
	queryLatencySLA          time.Duration
	queryProfSampleRate      int
	queryProfReportThreshold time.Duration
	srv                      *http.Server
}

// NewAPI godoc
// @title Etre API
// @version 1
// @description Etre is an entity API for managing primitive entities with labels.
// @contact.url https://github.com/square/etre
// @license.name Apache 2.0
// @license.url http://www.apache.org/licenses/LICENSE-2.0
func NewAPI(appCtx app.Context) *API {
	queryLatencySLA, _ := time.ParseDuration(appCtx.Config.Metrics.QueryLatencySLA)
	queryProfReportThreshold, _ := time.ParseDuration(appCtx.Config.Metrics.QueryProfileReportThreshold)
	queryTimeout, _ := time.ParseDuration(appCtx.Config.Datasource.QueryTimeout)
	api := &API{
		addr:                     appCtx.Config.Server.Addr,
		crt:                      appCtx.Config.Server.TLSCert,
		key:                      appCtx.Config.Server.TLSKey,
		es:                       appCtx.EntityStore,
		validate:                 appCtx.EntityValidator,
		auth:                     appCtx.Auth,
		cdcDisabled:              appCtx.Config.CDC.Disabled,
		streamFactory:            appCtx.StreamerFactory,
		metricsFactory:           appCtx.MetricsFactory,
		metricsStore:             appCtx.MetricsStore,
		systemMetrics:            appCtx.SystemMetrics,
		queryTimeout:             queryTimeout,
		queryLatencySLA:          queryLatencySLA,
		queryProfSampleRate:      int(appCtx.Config.Metrics.QueryProfileSampleRate * 100),
		queryProfReportThreshold: queryProfReportThreshold,
	}

	mux := http.NewServeMux()

	// /////////////////////////////////////////////////////////////////////
	// Query
	// /////////////////////////////////////////////////////////////////////
	mux.Handle("GET "+etre.API_ROOT+"/entities/{type}", api.requestWrapper(http.HandlerFunc(api.getEntitiesHandler)))

	// /////////////////////////////////////////////////////////////////////
	// Bulk Write
	// /////////////////////////////////////////////////////////////////////
	mux.Handle("POST "+etre.API_ROOT+"/entities/{type}", api.requestWrapper(http.HandlerFunc(api.postEntitiesHandler)))
	mux.Handle("PUT "+etre.API_ROOT+"/entities/{type}", api.requestWrapper(http.HandlerFunc(api.putEntitiesHandler)))
	mux.Handle("DELETE "+etre.API_ROOT+"/entities/{type}", api.requestWrapper(http.HandlerFunc(api.deleteEntitiesHandler)))

	// /////////////////////////////////////////////////////////////////////
	// Single Entity
	// /////////////////////////////////////////////////////////////////////
	mux.Handle("POST "+etre.API_ROOT+"/entity/{type}", api.requestWrapper(http.HandlerFunc(api.postEntityHandler)))
	mux.Handle("GET "+etre.API_ROOT+"/entity/{type}/{id}", api.requestWrapper(api.id(http.HandlerFunc(api.getEntityHandler))))
	mux.Handle("PUT "+etre.API_ROOT+"/entity/{type}/{id}", api.requestWrapper(api.id(http.HandlerFunc(api.putEntityHandler))))
	mux.Handle("DELETE "+etre.API_ROOT+"/entity/{type}/{id}", api.requestWrapper(api.id(http.HandlerFunc(api.deleteEntityHandler))))
	mux.Handle("GET "+etre.API_ROOT+"/entity/{type}/{id}/labels", api.requestWrapper(api.id(http.HandlerFunc(api.getLabelsHandler))))
	mux.Handle("DELETE "+etre.API_ROOT+"/entity/{type}/{id}/labels/{label}", api.requestWrapper(api.id(http.HandlerFunc(api.deleteLabelHandler))))

	// /////////////////////////////////////////////////////////////////////
	// Metrics and status
	// /////////////////////////////////////////////////////////////////////
	mux.HandleFunc("GET "+etre.API_ROOT+"/metrics", api.metricsHandler)
	mux.HandleFunc("GET "+etre.API_ROOT+"/status", api.statusHandler)

	// /////////////////////////////////////////////////////////////////////
	// Changes
	// /////////////////////////////////////////////////////////////////////
	mux.Handle("GET "+etre.API_ROOT+"/changes", api.cdcWrapper(http.HandlerFunc(api.changesHandler)))

	// /////////////////////////////////////////////////////////////////////
	// OpenAPI docs
	// /////////////////////////////////////////////////////////////////////
	docs.SwaggerInfo.BasePath = etre.API_ROOT
	mux.Handle("GET /apidocs/", httpSwagger.Handler())

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		err := ErrEndpointNotFound
		write := isWriteRequest(r.Method)
		if write {
			wr := etre.WriteResult{Error: &err}
			json.NewEncoder(w).Encode(wr)
		} else {
			json.NewEncoder(w).Encode(err)
		}
		return
	})

	api.srv = &http.Server{
		Addr:    appCtx.Config.Server.Addr,
		Handler: mux,
	}

	return api
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	api.srv.Handler.ServeHTTP(w, r)
}

func (api *API) Run() error {
	if api.crt != "" && api.key != "" {
		log.Printf("Listening on %s with TLS", api.addr)
		return api.srv.ListenAndServeTLS(api.crt, api.key)
	}
	log.Printf("Listening on %s", api.addr)
	return api.srv.ListenAndServe()
}

func (api *API) Stop() error {
	return api.srv.Shutdown(context.TODO())
}

// requestWrapper adds auth and metrics middleware to the endpoint handler for
// entity read/write endpoints. CDC should use cdcWrapper instead.
func (api *API) requestWrapper(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		write := isWriteRequest(r.Method)

		// Etre request context passed to endpoint handler
		rc := &req{
			entityType: r.PathValue("type"),
			write:      write,
		}

		// requests passed to requestWrapper should always have an entity type
		if rc.entityType == "" {
			etreErr := etre.Error{
				Message:    "missing entity type in request path: " + r.URL.String(),
				Type:       "bad-request",
				HTTPStatus: http.StatusBadRequest,
			}
			if write {
				api.WriteResult(rc, w, nil, etreErr)
			} else {
				api.readError(rc, w, etreErr)
			}
			return
		}

		defer func() {
			if r := recover(); r != nil {
				err, ok := r.(error)
				if !ok {
					err = fmt.Errorf("%v", r)
				}
				b := make([]byte, 4096)
				n := runtime.Stack(b, false)
				etreErr := etre.Error{
					Message:    fmt.Sprintf("PANIC: %s\n%s", err, string(b[0:n])),
					Type:       "panic",
					HTTPStatus: http.StatusInternalServerError,
				}
				log.Printf("PANIC: %s\n%s\n\n", err, string(b[0:n]))
				if write {
					api.WriteResult(rc, w, nil, etreErr)
				} else {
					api.readError(rc, w, etreErr)
				}
			}
		}()

		api.systemMetrics.Inc(metrics.Load, 1)
		defer api.systemMetrics.Inc(metrics.Load, -1)

		api.systemMetrics.Inc(metrics.Query, 1)

		// Instrument query_profile_sample_rate% of queries
		if rand.Intn(100) < api.queryProfSampleRate {
			rc.inst = app.NewTimerInstrument()
		} else {
			rc.inst = app.NopInstrument
		}

		// --------------------------------------------------------------
		// Query timeout
		// --------------------------------------------------------------
		var ctx context.Context
		var cancel context.CancelFunc

		queryTimeout := api.queryTimeout
		if qth := r.Header.Get(etre.QUERY_TIMEOUT_HEADER); qth != "" {
			d, err := time.ParseDuration(qth)
			if err != nil {
				err := etre.Error{
					Message:    fmt.Sprintf("invalid %s header: %s: %s", etre.QUERY_TIMEOUT_HEADER, qth, err),
					Type:       "invalid-query-timeout",
					HTTPStatus: http.StatusBadRequest,
				}
				if write {
					api.WriteResult(rc, w, nil, err)
				} else {
					api.readError(rc, w, err)
				}
				return
			}
			queryTimeout = d
		}
		ctx, cancel = context.WithTimeout(r.Context(), queryTimeout)

		defer cancel() // don't leak
		t0 := time.Now()

		// --------------------------------------------------------------
		// Authenticate
		// --------------------------------------------------------------
		rc.inst.Start("authenticate")
		caller, err := api.auth.Authenticate(r)
		rc.inst.Stop("authenticate")
		if err != nil {
			log.Printf("AUTH: failed to authenticate: %s (caller: %+v request: %+v)", err, caller, r)
			api.systemMetrics.Inc(metrics.AuthenticationFailed, 1)
			authErr := auth.Error{
				Err:        err,
				Type:       "access-denied",
				HTTPStatus: http.StatusUnauthorized,
			}
			if write {
				api.WriteResult(rc, w, nil, authErr)
			} else {
				api.readError(rc, w, authErr)
			}
			return
		}
		rc.caller = caller

		// --------------------------------------------------------------
		// Metrics
		// --------------------------------------------------------------

		// This makes a metrics.Group which is a 1-to-many proxy for every
		// metric group in caller.MetricGroups. E.g. if the groups are "ods"
		// and "finch", then every metric is recorded in both groups.
		gm := api.metricsFactory.Make(caller.MetricGroups)
		rc.gm = gm

		if err := api.validate.EntityType(rc.entityType); err != nil {
			log.Printf("Invalid entity type: '%s': caller=%+v request=%+v", rc.entityType, caller, r)
			gm.Inc(metrics.InvalidEntityType, 1)
			if write {
				api.WriteResult(rc, w, nil, err)
			} else {
				api.readError(rc, w, err)
			}
			return
		}

		// Bind group metrics to entity type
		gm.EntityType(rc.entityType)
		gm.Inc(metrics.Query, 1) // all queries (QPS)

		// auth.Manager extracts trace values from X-Etre-Trace header
		if caller.Trace != nil {
			gm.Trace(caller.Trace)
		}

		// --------------------------------------------------------------
		// Authorize
		// --------------------------------------------------------------

		rc.inst.Start("authorize")
		if write {
			gm.Inc(metrics.Write, 1) // all writes (write QPS)

			// Don't allow empty PUT or POST, client must provide entities for these
			if r.Method != "DELETE" && r.ContentLength == 0 {
				api.WriteResult(rc, w, nil, ErrNoContent)
				return
			}

			// All writes require a write op
			rc.wo = writeOp(r, caller)
			if err := api.validate.WriteOp(rc.wo); err != nil {
				api.WriteResult(rc, w, nil, err)
				return
			}
			if rc.wo.SetOp != "" {
				gm.Inc(metrics.SetOp, 1)
			}

			if err := api.auth.Authorize(caller, auth.Action{EntityType: rc.entityType, Op: auth.OP_WRITE}); err != nil {
				log.Printf("AUTH: not authorized: %s (caller: %+v request: %+v)", err, caller, r)
				gm.Inc(metrics.AuthorizationFailed, 1)
				authErr := auth.Error{
					Err:        err,
					Type:       "not-authorized",
					HTTPStatus: http.StatusForbidden,
				}
				api.WriteResult(rc, w, nil, authErr)
				return
			}
		} else {
			gm.Inc(metrics.Read, 1) // all reads (read QPS)

			if err := api.auth.Authorize(caller, auth.Action{EntityType: rc.entityType, Op: auth.OP_READ}); err != nil {
				log.Printf("AUTH: not authorized: %s (caller: %+v request: %+v)", err, caller, r)
				gm.Inc(metrics.AuthorizationFailed, 1)
				authErr := auth.Error{
					Err:        err,
					Type:       "not-authorized",
					HTTPStatus: http.StatusForbidden,
				}
				api.readError(rc, w, authErr)
				return
			}
		}
		rc.inst.Stop("authorize")

		// //////////////////////////////////////////////////////////////////////
		// Endpoint
		// //////////////////////////////////////////////////////////////////////

		next.ServeHTTP(w, r.WithContext(context.WithValue(ctx, reqKey, rc)))

		// //////////////////////////////////////////////////////////////////////
		// After
		// //////////////////////////////////////////////////////////////////////

		// Record query latency (response time) in milliseconds
		queryLatency := time.Now().Sub(t0)
		gm.Val(metrics.LatencyMs, int64(queryLatency/time.Millisecond))

		// Did the query take too long (miss SLA)?
		if api.queryLatencySLA > 0 && queryLatency > api.queryLatencySLA {
			gm.Inc(metrics.MissSLA, 1)
			profile := rc.inst != app.NopInstrument
			log.Printf("Missed SLA: %s %s %s (profile: %t caller=%+v request=%+v)", r.Method, r.URL.String(), queryLatency,
				profile, caller, r)
		}

		// Print query profile if it was timed and exceeds the report threshold
		if rc.inst != app.NopInstrument && queryLatency > api.queryProfReportThreshold {
			log.Printf("Query profile: %s %s %s %+v (caller=%+v)", r.Method, r.URL.String(), queryLatency, rc.inst.Report(), caller)
		}
	})
}

// cdcWrapper auth and metrics middleware for the changes endpoint. This endpoint is
// unique because it does not have an entity type and is a long-lived connection that
// should not use query timeout and long running query instrumentation.
func (api *API) cdcWrapper(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Etre request context passed to endpoint handler
		rc := &req{}

		defer func() {
			if r := recover(); r != nil {
				err, ok := r.(error)
				if !ok {
					err = fmt.Errorf("%v", r)
				}
				b := make([]byte, 4096)
				n := runtime.Stack(b, false)
				etreErr := etre.Error{
					Message:    fmt.Sprintf("PANIC: %s\n%s", err, string(b[0:n])),
					Type:       "panic",
					HTTPStatus: http.StatusInternalServerError,
				}
				log.Printf("PANIC: %s\n%s\n\n", err, string(b[0:n]))
				api.readError(rc, w, etreErr)
			}
		}()

		api.systemMetrics.Inc(metrics.Load, 1)
		defer api.systemMetrics.Inc(metrics.Load, -1)

		api.systemMetrics.Inc(metrics.Query, 1)

		// --------------------------------------------------------------
		// Authenticate
		// --------------------------------------------------------------
		caller, err := api.auth.Authenticate(r)
		if err != nil {
			log.Printf("AUTH: failed to authenticate: %s (caller: %+v request: %+v)", err, caller, r)
			api.systemMetrics.Inc(metrics.AuthenticationFailed, 1)
			authErr := auth.Error{
				Err:        err,
				Type:       "access-denied",
				HTTPStatus: http.StatusUnauthorized,
			}
			api.readError(rc, w, authErr)
			return
		}
		rc.caller = caller

		// --------------------------------------------------------------
		// Metrics
		// --------------------------------------------------------------

		// This makes a metrics.Group which is a 1-to-many proxy for every
		// metric group in caller.MetricGroups. E.g. if the groups are "ods"
		// and "finch", then every metric is recorded in both groups.
		//
		// Note that CDC does not have an entity type, so entity type specific
		// metrics should not be recorded.
		gm := api.metricsFactory.Make(caller.MetricGroups)
		rc.gm = gm

		// --------------------------------------------------------------
		// Authorize
		// --------------------------------------------------------------

		if err := api.auth.Authorize(caller, auth.Action{Op: auth.OP_CDC}); err != nil {
			log.Printf("AUTH: not authorized: %s (caller: %+v request: %+v)", err, caller, r)
			gm.Inc(metrics.AuthorizationFailed, 1)
			authErr := auth.Error{
				Err:        err,
				Type:       "not-authorized",
				HTTPStatus: http.StatusForbidden,
			}
			api.readError(rc, w, authErr)
			return
		}

		// //////////////////////////////////////////////////////////////////////
		// Endpoint
		// //////////////////////////////////////////////////////////////////////

		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), reqKey, rc)))
	})
}

func (api *API) id(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		rc := ctx.Value(reqKey).(*req)

		var err error
		var entityId primitive.ObjectID

		id := r.PathValue("id") // 1. from URL
		if id == "" {
			err = ErrMissingParam.New("missing id param")
		} else {
			entityId, err = primitive.ObjectIDFromHex(id) // 2. convert to/validate as ObjectID
			if err != nil {
				err = ErrInvalidParam.New("id '%s' is not a valid ObjectID: %v", id, err)
			}
		}
		if err != nil {
			if rc.write {
				api.WriteResult(rc, w, nil, err)
			} else {
				api.readError(rc, w, err)
			}
			return
		}

		rc.entityId = entityId.Hex() // 3. ObjectID to hex value (string)
		next.ServeHTTP(w, r)
	})
}

// //////////////////////////////////////////////////////////////////////////
// Query
// //////////////////////////////////////////////////////////////////////////

// getEntitiesHandler godoc
// @Summary Query a set of entities
// @Description Query entities of a type specified by the :type endpoint.
// @Description Returns a set of entities matching the labels in the `query` query parameter.
// @Description All labels of each entity are returned, unless specific labels are specified in the `labels` query parameter.
// @Description The result set is reduced to distinct values if the request includes the `distinct` query parameter (requires `lables` name a single label).
// @Description If the query is longer than 2000 characters, use the POST /query endpoint (to be implemented).
// @ID getEntitiesHandler
// @Produce json
// @Param type path string true "Entity type"
// @Param query query string true "Selector"
// @Param labels query string false "Comma-separated list of labels to return"
// @Param distinct query boolean false "Reduce results to one per distinct value"
// @Success 200 {array} etre.Entity "OK"
// @Failure 400,404 {object} etre.Error
// @Router /entities/:type [get]
func (api *API) getEntitiesHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()             // query timeout
	rc := ctx.Value(reqKey).(*req) // Etre request context
	rc.inst.Start("handler")
	defer rc.inst.Stop("handler")

	rc.gm.Inc(metrics.ReadQuery, 1) // specific read type

	// Parse query (label selector) from URL
	q, err := parseQuery(r)
	if err != nil {
		api.readError(rc, w, err)
		return
	}

	// Label metrics
	rc.gm.Val(metrics.Labels, int64(len(q.Predicates)))
	for _, p := range q.Predicates {
		rc.gm.IncLabel(metrics.LabelRead, p.Label)
	}

	// Query Filter
	f := etre.QueryFilter{}
	qv := r.URL.Query() // ?x=1&y=2&z -> https://godoc.org/net/url#Values
	if csv, ok := qv["labels"]; ok {
		f.ReturnLabels = strings.Split(csv[0], ",")
	}
	if _, ok := qv["distinct"]; ok {
		f.Distinct = true
	}
	if f.Distinct && len(f.ReturnLabels) > 1 {
		api.readError(rc, w, ErrInvalidQuery.New("distinct requires only 1 return label but %d specified: %v", len(f.ReturnLabels), f.ReturnLabels))
		return
	}

	// Query data store (instrumented)
	rc.inst.Start("db")
	entities, err := api.es.ReadEntities(ctx, rc.entityType, q, f)
	rc.inst.Stop("db")
	if err != nil {
		api.readError(rc, w, err)
		return
	}
	rc.gm.Val(metrics.ReadMatch, int64(len(entities)))

	// Success: return matching entities (possibly empty list)
	rc.inst.Start("encode-response")
	if len(entities) > 0 && strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		// use gzip compression if we have data to send back and the client accepts gzip
		w.Header().Set("Content-Encoding", "gzip")
		gzw := gzip.NewWriter(w)
		defer gzw.Close()
		json.NewEncoder(gzw).Encode(entities)
	} else {
		// no compression
		json.NewEncoder(w).Encode(entities)
	}
	rc.inst.Stop("encode-response")
}

// //////////////////////////////////////////////////////////////////////////
// Bulk Write
// //////////////////////////////////////////////////////////////////////////

// postEntitiesHandler godoc
// @Summary Create entities in bulk
// @Description Given JSON payload, create new entities of the given :type.
// @Description Some meta-labels are filled in by Etre, e.g. `_id`.
// @Description Optionally specify `setOp`, `setId`, and `setSize` together to define a SetOp.
// @ID postEntitiesHandler
// @Accept json
// @Produce json
// @Param type path string true "Entity type"
// @Param setOp query string false "SetOp"
// @Param setId query string false "SetId"
// @Param setSize query int false "SetSize"
// @Success 201 {array} string "List of new entity id's"
// @Failure 400 {object} etre.Error
// @Router /entities/:type [post]
func (api *API) postEntitiesHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()             // query timeout
	rc := ctx.Value(reqKey).(*req) // Etre request context
	rc.inst.Start("handler")
	defer rc.inst.Stop("handler")

	rc.gm.Inc(metrics.CreateMany, 1) // specific write type

	// Return values at reply (not mutually exclusive)
	var ids []string
	var err error

	// Read new entities from client. Should be an array of entities like:
	//   [{a:1,b:"foo"},{a:2,b:"bar"}]
	// Must have >= 1 entity; we're strict to guard against client bugs.
	var entities []etre.Entity
	if err = json.NewDecoder(r.Body).Decode(&entities); err != nil {
		err = ErrInvalidContent
		goto reply
	}
	if len(entities) == 0 {
		err = ErrNoContent
		goto reply
	}

	// Validate new entities before attempting to write
	rc.gm.Val(metrics.CreateBulk, int64(len(entities))) // inc before validating
	if err = api.validate.Entities(entities, entity.VALIDATE_ON_CREATE); err != nil {
		goto reply
	}

	// Write new entities to data store
	ids, err = api.es.CreateEntities(ctx, rc.wo, entities)
	rc.gm.Inc(metrics.Created, int64(len(ids)))

reply:
	api.WriteResult(rc, w, ids, err)
}

// putEntitiesHandler godoc
// @Summary Update matching entities in bulk
// @Description Given JSON payload, update labels in matching entities of the given :type.
// @Description Applies update to the set of entities matching the labels in the `query` query parameter.
// @Description Optionally specify `setOp`, `setId`, and `setSize` together to define a SetOp.
// @ID putEntitiesHandler
// @Accept json
// @Produce json
// @Param type path string true "Entity type"
// @Param query query string true "Selector"
// @Param setOp query string false "SetOp"
// @Param setId query string false "SetId"
// @Param setSize query int false "SetSize"
// @Success 200 {array} etre.Entity "Set of matching entities after update applied."
// @Failure 400 {object} etre.Error
// @Router /entities/:type [put]
func (api *API) putEntitiesHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()             // query timeout
	rc := ctx.Value(reqKey).(*req) // Etre request context
	rc.inst.Start("handler")
	defer rc.inst.Stop("handler")

	rc.gm.Inc(metrics.UpdateQuery, 1) // specific write type

	// Return values at reply (not mutually exclusive)
	var entities []etre.Entity
	var err error

	var patch etre.Entity

	// Parse query (label selector) from URL
	var q query.Query
	q, err = parseQuery(r)
	if err != nil {
		goto reply
	}

	// Read and validate patch entity
	if err = json.NewDecoder(r.Body).Decode(&patch); err != nil {
		err = ErrInvalidContent
		goto reply
	}
	if len(patch) == 0 {
		err = ErrNoContent
		goto reply
	}
	if err = api.validate.Entities([]etre.Entity{patch}, entity.VALIDATE_ON_UPDATE); err != nil {
		goto reply
	}

	// Label metrics (read and update)
	rc.gm.Val(metrics.Labels, int64(len(q.Predicates)))
	for _, p := range q.Predicates {
		rc.gm.IncLabel(metrics.LabelRead, p.Label)
	}
	for label := range patch {
		rc.gm.IncLabel(metrics.LabelUpdate, label)
	}

	// Patch all entities matching query
	entities, err = api.es.UpdateEntities(ctx, rc.wo, q, patch)
	rc.gm.Val(metrics.UpdateBulk, int64(len(entities)))
	rc.gm.Inc(metrics.Updated, int64(len(entities)))

reply:
	api.WriteResult(rc, w, entities, err)
}

// deleteEntitiesHandler godoc
// @Summary Remove matching entities in bulk
// @Description Deletes the set of entities of the given :type, matching the labels in the `query` query parameter.
// @ID deleteEntitiesHandler
// @Produce json
// @Param type path string true "Entity type"
// @Param query query string true "Selector"
// @Param setOp query string false "SetOp"
// @Param setId query string false "SetId"
// @Param setSize query int false "SetSize"
// @Success 200 {array} etre.Entity "OK"
// @Failure 400 {object} etre.Error
// @Router /entities/:type [delete]
func (api *API) deleteEntitiesHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()             // query timeout
	rc := ctx.Value(reqKey).(*req) // Etre request context
	rc.inst.Start("handler")
	defer rc.inst.Stop("handler")

	rc.gm.Inc(metrics.DeleteQuery, 1) // specific write type

	// Return values at reply (not mutually exclusive)
	var entities []etre.Entity
	var err error

	// Parse query (label selector) from URL
	var q query.Query
	q, err = parseQuery(r)
	if err != nil {
		goto reply
	}

	// Label metrics
	rc.gm.Val(metrics.Labels, int64(len(q.Predicates)))
	for _, p := range q.Predicates {
		rc.gm.IncLabel(metrics.LabelRead, p.Label)
	}

	// Delete entities, returns the deleted entities
	entities, err = api.es.DeleteEntities(ctx, rc.wo, q)
	rc.gm.Val(metrics.DeleteBulk, int64(len(entities)))
	rc.gm.Inc(metrics.Deleted, int64(len(entities)))

reply:
	api.WriteResult(rc, w, entities, err)
}

// //////////////////////////////////////////////////////////////////////////
// Single Entity
// //////////////////////////////////////////////////////////////////////////

// getEntityHandler godoc
// @Summary Get one entity by id
// @Description Return one entity of the given :type, identified by the path parameter :id.
// @Description All labels of each entity are returned, unless specific labels are specified in the `labels` query parameter.
// @ID getEntityHandler
// @Produce json
// @Param type path string true "Entity type"
// @Param id path string true "Entity ID"
// @Param labels query string false "Comma-separated list of labels to return"
// @Success 200 {object} etre.Entity "OK"
// @Failure 400,404 {object} etre.Error
// @Router /entity/:type/:id [get]
func (api *API) getEntityHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()             // query timeout
	rc := ctx.Value(reqKey).(*req) // Etre request context
	rc.inst.Start("handler")
	defer rc.inst.Stop("handler")

	rc.gm.Inc(metrics.ReadId, 1) // specific read type

	// Query Filter
	f := etre.QueryFilter{}
	qv := r.URL.Query() // ?x=1&y=2&z -> https://godoc.org/net/url#Values
	if csv, ok := qv["labels"]; ok {
		f.ReturnLabels = strings.Split(csv[0], ",")
	}

	// Read the entity by ID
	q, _ := query.Translate("_id=" + rc.entityId)
	entities, err := api.es.ReadEntities(ctx, rc.entityType, q, f)
	if err != nil {
		api.readError(rc, w, err)
		return
	}
	if len(entities) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	json.NewEncoder(w).Encode(entities[0])
}

// getLabelsHandler godoc
// @Summary Return the labels for a single entity.
// @Description Return an array of label names used by a single entity of the given :type, identified by the path parameter :id.
// @Description The values of these labels are not returned.
// @ID getLabelsHandler
// @Produce json
// @Param type path string true "Entity type"
// @Param id path string true "Entity ID"
// @Success 200 {array} string "OK"
// @Failure 400,404 {object} etre.Error
// @Router /entity/:type/:id/labels [get]
func (api *API) getLabelsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()             // query timeout
	rc := ctx.Value(reqKey).(*req) // Etre request context
	rc.inst.Start("handler")
	defer rc.inst.Stop("handler")

	rc.gm.Inc(metrics.ReadLabels, 1) // specific read type

	q, _ := query.Translate("_id=" + rc.entityId)
	entities, err := api.es.ReadEntities(ctx, rc.entityType, q, etre.QueryFilter{})
	if err != nil {
		api.readError(rc, w, err)
		return
	}
	if len(entities) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	json.NewEncoder(w).Encode(entities[0].Labels())
}

// --------------------------------------------------------------------------
// Single entity writes
// --------------------------------------------------------------------------

// postEntityHandler godoc
// @Summary Create one entity
// @Description Given JSON payload, create one new entity of the given :type.
// @Description Some meta-labels are filled in by Etre, e.g. `_id`.
// @Description Optionally specify `setOp`, `setId`, and `setSize` together to define a SetOp.
// @ID postEntityHandler
// @Accept json
// @Produce json
// @Param type path string true "Entity type"
// @Param setOp query string false "SetOp"
// @Param setId query string false "SetId"
// @Param setSize query int false "SetSize"
// @Success 201 {array} string "List of new entity id's"
// @Failure 400,404 {object} etre.Error
// @Router /entity/:type [post]
func (api *API) postEntityHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()             // query timeout
	rc := ctx.Value(reqKey).(*req) // Etre request context
	rc.inst.Start("handler")
	defer rc.inst.Stop("handler")

	rc.gm.Inc(metrics.CreateOne, 1)

	var newEntity etre.Entity
	var ids []string
	var err error

	// Read and validate new entity
	if err = json.NewDecoder(r.Body).Decode(&newEntity); err != nil {
		err = ErrInvalidContent
		goto reply
	}

	if err = api.validate.Entities([]etre.Entity{newEntity}, entity.VALIDATE_ON_CREATE); err != nil {
		goto reply
	}

	// Create new entity
	ids, err = api.es.CreateEntities(ctx, rc.wo, []etre.Entity{newEntity})
	if err == nil {
		rc.gm.Inc(metrics.Created, 1)
	}

reply:
	api.WriteResult(rc, w, ids, err)
}

// putEntityHandler godoc
// @Summary Patch one entity by _id
// @Description Given JSON payload, update labels in the entity of the given :type and :id.
// @Description Optionally specify `setOp`, `setId`, and `setSize` together to define a SetOp.
// @ID putEntityHandler
// @Accept json
// @Produce json
// @Param type path string true "Entity type"
// @Param id path string true "Entity ID"
// @Param setOp query string false "SetOp"
// @Param setId query string false "SetId"
// @Param setSize query int false "SetSize"
// @Success 200 {array} etre.Entity "Entity after update applied."
// @Failure 400,404 {object} etre.Error
// @Router /entity/:type/:id [put]
func (api *API) putEntityHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()             // query timeout
	rc := ctx.Value(reqKey).(*req) // Etre request context
	rc.inst.Start("handler")
	defer rc.inst.Stop("handler")

	rc.gm.Inc(metrics.UpdateId, 1)

	// Return values at reply (not mutually exclusive)
	var patch etre.Entity
	var entities []etre.Entity
	var err error

	q, _ := query.Translate("_id=" + rc.entityId)

	// Read and validate patch entity
	if err = json.NewDecoder(r.Body).Decode(&patch); err != nil {
		err = ErrInvalidContent
		goto reply
	}
	if err = api.validate.Entities([]etre.Entity{patch}, entity.VALIDATE_ON_UPDATE); err != nil {
		goto reply
	}

	// Label metrics (update)
	for label := range patch {
		rc.gm.IncLabel(metrics.LabelUpdate, label)
	}

	// Patch one entity by ID
	entities, err = api.es.UpdateEntities(ctx, rc.wo, q, patch)
	if err != nil {
		goto reply
	} else if len(entities) == 0 {
		err = ErrNotFound
		goto reply
	} else {
		rc.gm.Inc(metrics.Updated, 1)
	}

reply:
	api.WriteResult(rc, w, entities, err)
}

// deleteEntityHandler godoc
// @Summary Delete one entity
// @Summary Remove entity of the given :type and matching the :id parameter.
// @Description Deletes the set of entities matching the labels in the `query` query parameter.
// @ID deleteEntityHandler
// @Produce json
// @Param type path string true "Entity type"
// @Param id path string true "Entity ID"
// @Param setOp query string false "SetOp"
// @Param setId query string false "SetId"
// @Param setSize query int false "SetSize"
// @Success 200 {array} etre.Entity "Set of deleted entities."
// @Failure 400,404 {object} etre.Error
// @Router /entity/:type/:id [delete]
func (api *API) deleteEntityHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()             // query timeout
	rc := ctx.Value(reqKey).(*req) // Etre request context
	rc.inst.Start("handler")
	defer rc.inst.Stop("handler")

	rc.gm.Inc(metrics.DeleteId, 1)

	var entities []etre.Entity
	var err error

	// Get and validate entity id from URL (:id).
	// wo has the same (string) value but didn't validate it.
	q, _ := query.Translate("_id=" + rc.entityId)

	// Delete one entity by ID
	entities, err = api.es.DeleteEntities(ctx, rc.wo, q)
	if err != nil {
		goto reply
	} else if len(entities) == 0 {
		err = ErrNotFound
	} else {
		rc.gm.Inc(metrics.Deleted, 1)
	}

reply:
	api.WriteResult(rc, w, entities, err)
}

// deleteLabelHandler godoc
// @Summary Delete a label from one entity
// @Description Remove one label from one entity of the given :type and matching the :id parameter.
// @ID deleteLabelHandler
// @Produce json
// @Param type path string true "Entity type"
// @Param id path string true "Entity ID"
// @Param label path string true "Label name"
// @Param setOp query string false "SetOp"
// @Param setId query string false "SetId"
// @Param setSize query int false "SetSize"
// @Success 200 {object} etre.Entity "Entity after the label is deleted."
// @Failure 400,404 {object} etre.Error
// @Router /entity/:type/:id/labels/:label [delete]
func (api *API) deleteLabelHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()             // query timeout
	rc := ctx.Value(reqKey).(*req) // Etre request context
	rc.inst.Start("handler")
	defer rc.inst.Stop("handler")

	rc.gm.Inc(metrics.DeleteLabel, 1)

	var err error
	var label string
	var diff etre.Entity

	label = r.PathValue("label")
	if label == "" {
		err = ErrMissingParam.New("missing label param")
		goto reply
	}
	rc.gm.IncLabel(metrics.LabelDelete, label)
	if err = api.validate.DeleteLabel(label); err != nil {
		goto reply
	}

	// Delete label from entity
	diff, err = api.es.DeleteLabel(ctx, rc.wo, label)
	if err != nil {
		if err == etre.ErrEntityNotFound {
			err = nil // delete is idempotent
		}
	}

reply:
	api.WriteResult(rc, w, diff, err)
}

// --------------------------------------------------------------------------
// Metrics and status
// --------------------------------------------------------------------------

// metricsHandler godoc
// @Summary Report calling metrics for Etre
// @Description Reports a summary of how Etre was called by different groups.
// @Description System Report includes a counter of queries, a `load` which is the number of currently executing queries, and counters of errors and failed authentications.
// @Description Group Reports are made for each user-defined group, which correspond to authentication types.
// @Description Group Reports have sub-reports for request failures, query traffic per entity type, and CDC activity.
// @ID metricsHandler
// @Produce json
// @Param reset query string no "If 'yes' or 'true' then reset metrics to zero."
// @Success 200 {object} etre.Metrics "OK"
// @Router /metrics [get]
func (api *API) metricsHandler(w http.ResponseWriter, r *http.Request) {
	// If &reset={true|false}, reset histogram samples
	reset := false
	qv := r.URL.Query()
	v := qv["reset"]
	if len(v) > 0 && (strings.ToLower(v[0]) == "yes" || strings.ToLower(v[0]) == "true") {
		reset = true
	}

	// Get system metrics which returns an etre.Metrics with System set
	// and Groups nil, which we set next
	all := api.systemMetrics.Report(reset)

	// Get list of metric group names. If no user-defined auth plugin, there
	// will be the default metric group: "etre". Else, the user-defined auth
	// plugin can specify zero or more groups.
	groups := api.metricsStore.Names()
	if len(groups) == 0 { // no user-defined metric groups
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(all)
		return
	}

	all.Groups = make([]etre.MetricsGroupReport, len(groups))

	// Get metrics for each group, which also returns an etre.Metrics with
	// System nil and Groups[0] = the group metrics
	for i, name := range groups {
		gm := api.metricsStore.Get(name)
		if gm == nil {
			log.Printf("No metrics for %s", name)
			continue
		}
		r := gm.Report(reset)
		r.Groups[0].Group = name
		all.Groups[i] = r.Groups[0]
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(all)
}

// statusHandler godoc
// @Summary Report service status
// @Description Report if the service is up, and what version of the Etre service.
// @ID statusHandler
// @Success 200 {string} string "returns a map[string]string"
// @Router /status [get]
func (api *API) statusHandler(w http.ResponseWriter, r *http.Request) {
	status := map[string]string{
		"ok":      "yes",
		"version": etre.VERSION,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// --------------------------------------------------------------------------
// Change feed
// --------------------------------------------------------------------------

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

// changesHandler godoc
// @Summary Starts a websocket response.
// @Description Starts streaming changes from the Etre CDC on a websocket interface.
// @Description See Etre documentation for details about consuming the changes stream.
// @ID changesHandler
// @Router /changes [get]
func (api *API) changesHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()             // query timeout
	rc := ctx.Value(reqKey).(*req) // Etre request context

	if api.cdcDisabled {
		api.readError(rc, w, ErrCDCDisabled)
		return
	}

	// Upgrade to a WebSocket connection.
	wsConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		api.readError(rc, w, ErrInternal.New(err.Error()))
		return
	}
	defer wsConn.Close()

	rc.gm.Inc(metrics.CDCClients, 1)
	defer rc.gm.Inc(metrics.CDCClients, -1)

	clientId := fmt.Sprintf("%s@%s", rc.caller.Name, r.RemoteAddr)
	log.Printf("CDC: %s: connected", clientId)

	stream := api.streamFactory.Make(clientId)
	client := changestream.NewWebsocketClient(clientId, wsConn, stream)
	if err := client.Run(); err != nil {
		switch err {
		case changestream.ErrWebsocketClosed:
			log.Printf("CDC: %s: closed connection", clientId)
		default:
			log.Printf("CDC: %s: lost connection: %s", clientId, err)
		}
	}
}

// Return error on read. Writes always return an etre.WriteResult by calling WriteResult.
func (api *API) readError(rc *req, w http.ResponseWriter, err error) {
	api.systemMetrics.Inc(metrics.Error, 1)
	var httpStatus = http.StatusInternalServerError
	var ret interface{}
	switch v := err.(type) {
	case etre.Error:
		maybeInc(metrics.ClientError, 1, rc.gm)
		httpStatus = v.HTTPStatus
		ret = err
	case entity.ValidationError:
		maybeInc(metrics.ClientError, 1, rc.gm)
		ret = etre.Error{
			Message:    v.Err.Error(),
			Type:       v.Type,
			HTTPStatus: http.StatusBadRequest,
		}
		httpStatus = http.StatusBadRequest
	case auth.Error:
		// Metric incremented by caller
		ret = etre.Error{
			Message:    v.Err.Error(),
			Type:       v.Type,
			HTTPStatus: v.HTTPStatus,
		}
		httpStatus = v.HTTPStatus
	case entity.DbError:
		dbErr := err.(entity.DbError)
		if dbErr.Err == context.DeadlineExceeded {
			maybeInc(metrics.QueryTimeout, 1, rc.gm)
		} else {
			log.Printf("DATABASE ERROR: %v", dbErr)
			maybeInc(metrics.DbError, 1, rc.gm)
		}
		ret = etre.Error{
			Message:    dbErr.Error(),
			Type:       dbErr.Type,
			HTTPStatus: http.StatusServiceUnavailable,
			EntityId:   dbErr.EntityId,
		}
		httpStatus = http.StatusServiceUnavailable
	default:
		log.Printf("API ERROR: %v", err)
		maybeInc(metrics.APIError, 1, rc.gm)
		httpStatus = http.StatusInternalServerError
	}

	w.WriteHeader(httpStatus)
	json.NewEncoder(w).Encode(ret)
}

// Return an etre.WriteResult for all writes, successful of not. ids are the
// writes from entity.Store calls, which is why it can be different types.
// ids and err are not mutually exclusive; writes can be partially successful.
func (api *API) WriteResult(rc *req, w http.ResponseWriter, ids interface{}, err error) {
	var httpStatus = http.StatusInternalServerError
	var wr etre.WriteResult
	var writes []etre.Write

	// Map error to etre.Error
	if err != nil {
		api.systemMetrics.Inc(metrics.Error, 1)
		switch v := err.(type) {
		case etre.Error:
			wr.Error = &v
			switch err {
			case ErrNotFound:
				// Not an error
			default:
				maybeInc(metrics.ClientError, 1, rc.gm)
			}
		case entity.ValidationError:
			maybeInc(metrics.ClientError, 1, rc.gm)
			wr.Error = &etre.Error{
				Message:    v.Err.Error(),
				Type:       v.Type,
				HTTPStatus: http.StatusBadRequest,
			}
		case entity.DbError:
			if err.(entity.DbError).Err == context.DeadlineExceeded {
				maybeInc(metrics.QueryTimeout, 1, rc.gm)
			} else {
				maybeInc(metrics.DbError, 1, rc.gm)
			}
			switch v.Type {
			case "duplicate-entity":
				dupeErr := ErrDuplicateEntity // copy
				dupeErr.EntityId = v.EntityId
				dupeErr.Message += " (db err: " + v.Err.Error() + ")"
				wr.Error = &dupeErr
			case "db-insert-one":
				insertErr := ErrDBInsertFailed
				insertErr.EntityId = v.EntityId
				insertErr.Message += " (db err: " + v.Err.Error() + ")"
				wr.Error = &insertErr
			case "db-update-one":
				updateErr := ErrDBUpdateFailed
				updateErr.EntityId = v.EntityId
				updateErr.Message += " (db err: " + v.Err.Error() + ")"
				wr.Error = &updateErr
			default:
				wr.Error = &etre.Error{
					Message:    v.Err.Error(),
					Type:       v.Type,
					HTTPStatus: http.StatusServiceUnavailable,
					EntityId:   v.EntityId,
				}
			}
		case auth.Error:
			// Metric incremented by caller
			wr.Error = &etre.Error{
				Message:    v.Err.Error(),
				Type:       v.Type,
				HTTPStatus: v.HTTPStatus,
			}
		default:
			maybeInc(metrics.APIError, 1, rc.gm)
			wr.Error = &etre.Error{
				Message:    err.Error(),
				Type:       "unhandled-error",
				HTTPStatus: http.StatusInternalServerError,
			}
		}
		httpStatus = wr.Error.HTTPStatus
	} else {
		httpStatus = http.StatusOK
	}

	// Map writes to []etre.Write
	if ids != nil {
		switch ids.(type) {
		case []etre.Entity:
			// Diffs from UpdateEntities and DeleteEntities
			diffs := ids.([]etre.Entity)
			writes = make([]etre.Write, len(diffs))
			for i, diff := range diffs {
				// _id from db is primitive.ObjectID, convert to string
				id := diff["_id"].(primitive.ObjectID).Hex()
				writes[i] = etre.Write{
					EntityId: id,
					URI:      api.addr + etre.API_ROOT + "/entity/" + id,
					Diff:     diff,
				}
			}
		case []string:
			// Entity _id from CreateEntities
			ids := ids.([]string)
			writes = make([]etre.Write, len(ids))
			for i, id := range ids {
				writes[i] = etre.Write{
					EntityId: id,
					URI:      api.addr + etre.API_ROOT + "/entity/" + id,
				}
			}
			// Partial write: got some writes + error, don't override error
			if err == nil {
				httpStatus = http.StatusCreated
			}
		case etre.Entity:
			// Entity from DeleteLabel
			diff := ids.(etre.Entity)
			// _id from db is primitive.ObjectID, convert to string
			id := diff["_id"].(primitive.ObjectID).Hex()
			writes = []etre.Write{
				{
					EntityId: id,
					URI:      api.addr + etre.API_ROOT + "/entity/" + id,
					Diff:     diff,
				},
			}
		default:
			panic(fmt.Sprintf("invalid arg type: %#v", ids))
		}
		wr.Writes = writes
	}

	w.WriteHeader(httpStatus)
	json.NewEncoder(w).Encode(wr)
}

func writeOp(r *http.Request, caller auth.Caller) entity.WriteOp {
	wo := entity.WriteOp{
		Caller:     caller.Name,
		EntityType: r.PathValue("type"),
		EntityId:   r.PathValue("id"),
	}

	qv := r.URL.Query()
	setOp := qv.Get("setOp")
	if setOp != "" {
		wo.SetOp = setOp
	}
	setId := qv.Get("setId")
	if setId != "" {
		wo.SetId = setId
	}
	setSize := qv.Get("setSize")
	if setSize != "" {
		i, _ := strconv.Atoi(setSize)
		wo.SetSize = i
	}

	return wo
}

func maybeInc(metric byte, n int64, v interface{}) {
	if v == nil {
		return
	}
	gm, ok := v.(metrics.Metrics)
	if !ok {
		return
	}
	gm.Inc(metric, n)
}

func parseQuery(r *http.Request) (query.Query, error) {
	var q query.Query
	var err error
	qv := r.URL.Query() // ?x=1&y=2&z -> https://godoc.org/net/url#Values
	labelSelector := qv.Get("query")
	if labelSelector == "" {
		return q, ErrInvalidQuery.New("query string is empty")
	}
	q, err = query.Translate(labelSelector)
	if err != nil {
		return q, ErrInvalidQuery.New("invalid query: %s", err)
	}
	return q, nil
}

func isWriteRequest(method string) bool {
	// Only these HTTP methods are writes
	// method != "GET" doesn't work because of "HEAD", "OPTIONS", etc.
	return method == "PUT" || method == "POST" || method == "DELETE"
}
