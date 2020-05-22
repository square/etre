// Copyright 2017-2020, Square, Inc.

// Package api provides API endpoints and controllers.
package api

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/square/etre"
	"github.com/square/etre/app"
	"github.com/square/etre/auth"
	"github.com/square/etre/cdc/changestream"
	"github.com/square/etre/entity"
	"github.com/square/etre/metrics"
	"github.com/square/etre/query"
)

func init() {
	rand.Seed(time.Now().UnixNano())
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
	defaultClientVersion     string
	queryTimeout             time.Duration
	queryLatencySLA          time.Duration
	queryProfSampleRate      int
	queryProfReportThreshold time.Duration
	// --
	echo *echo.Echo
}

var reVersion = regexp.MustCompile(`^v?(\d+\.\d+)`)

// NewAPI makes a new API.
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
		defaultClientVersion:     appCtx.Config.Server.DefaultClientVersion,
		queryTimeout:             queryTimeout,
		queryLatencySLA:          queryLatencySLA,
		queryProfSampleRate:      int(appCtx.Config.Metrics.QueryProfileSampleRate * 100),
		queryProfReportThreshold: queryProfReportThreshold,
		// --
		echo: echo.New(),
	}

	router := api.echo.Group(etre.API_ROOT)

	router.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			defer func() {
				if r := recover(); r != nil {
					// Don't leak context
					if v := c.Get("cancelFunc"); v != nil {
						cancel := v.(context.CancelFunc)
						cancel()
					}

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
					c.JSON(etreErr.HTTPStatus, etreErr)
					maybeInc(metrics.APIError, 1, c.Get("gm"))
				}
			}()
			return next(c)
		}
	})

	// Called before every route/controller
	router.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			entityType := c.Param("type")
			if entityType != "" {
				c.Set("t0", time.Now()) // query start time
			}

			// Instrument query_profile_sample_rate% of queries
			var inst app.Instrument
			if rand.Intn(100) < api.queryProfSampleRate {
				inst = app.NewTimerInstrument()
			} else {
				inst = app.NopInstrument
			}
			c.Set("inst", inst)

			api.systemMetrics.Inc(metrics.Query, 1)

			// Only these HTTP methods are writes. Be careful: method != "GET" doesn't
			// work because of "HEAD", "OPTIONS", etc.
			method := c.Request().Method
			write := method == "PUT" || method == "POST" || method == "DELETE"

			// --------------------------------------------------------------
			// Authenticate
			// --------------------------------------------------------------
			inst.Start("authenticate")
			caller, err := api.auth.Authenticate(c.Request())
			inst.Stop("authenticate")
			if err != nil {
				log.Printf("AUTH: failed to authenticate: %s (caller: %+v request: %+v)", err, caller, c.Request())
				api.systemMetrics.Inc(metrics.AuthenticationFailed, 1)
				c.Set("t0", time.Time{}) // don't skew latency samples toward zero
				authErr := auth.Error{
					Err:        err,
					Type:       "access-denied",
					HTTPStatus: http.StatusUnauthorized,
				}
				if write {
					return c.JSON(api.WriteResult(c, nil, authErr))
				}
				return readError(c, authErr)
			}
			c.Set("caller", caller)

			// --------------------------------------------------------------
			// Metrics
			// --------------------------------------------------------------

			// This makes a metrics.Group which is a 1-to-many proxy for every
			// metric group in caller.MetricGroups. E.g. if the groups are "ods"
			// and "finch", then every metric is recorded in both groups.
			gm := api.metricsFactory.Make(caller.MetricGroups)
			c.Set("gm", gm)

			if entityType == "" {
				return next(c)
			}

			// If entity type is invalid, unset t0 so the LatencyMs metric isn't
			// skewed with artificially fast queries
			if err := api.validate.EntityType(entityType); err != nil {
				log.Printf("Invalid entity type: '%s': caller=%+v request=%+v", entityType, caller, c.Request())
				gm.Inc(metrics.InvalidEntityType, 1)
				c.Set("t0", time.Time{}) // don't skew latency samples toward zero
				if write {
					return c.JSON(api.WriteResult(c, nil, err))
				}
				return readError(c, err)
			}

			// Bind group metrics to entity type
			gm.EntityType(entityType)
			gm.Inc(metrics.Query, 1)

			// auth.Manager extracts trace values from X-Etre-Trace header
			if caller.Trace != nil {
				gm.Trace(caller.Trace)
			}

			// --------------------------------------------------------------
			// Authorize
			// --------------------------------------------------------------

			// Routes with an :entity param query the db, so increment query.Metrics
			// and query.Read or .Write depending on the route. Specific Read/Write
			// metrics are set in the controller.
			inst.Start("authorize")
			if write {
				gm.Inc(metrics.Write, 1)

				// Don't allow empty PUT or POST, client must provide entities for these
				if method != "DELETE" && c.Request().ContentLength == 0 {
					return c.JSON(api.WriteResult(c, nil, ErrNoContent))
				}

				// All writes require a write op
				wo := writeOp(c, caller)
				if err := api.validate.WriteOp(wo); err != nil {
					c.Set("t0", time.Time{}) // don't skew latency samples toward zero
					return c.JSON(api.WriteResult(c, nil, err))
				}
				c.Set("wo", wo)
				if wo.SetOp != "" {
					gm.Inc(metrics.SetOp, 1)
				}

				if err := api.auth.Authorize(caller, auth.Action{EntityType: entityType, Op: auth.OP_WRITE}); err != nil {
					log.Printf("AUTH: not authorized: %s (caller: %+v request: %+v)", err, caller, c.Request())
					gm.Inc(metrics.AuthorizationFailed, 1)
					authErr := auth.Error{
						Err:        err,
						Type:       "not-authorized",
						HTTPStatus: http.StatusForbidden,
					}
					c.Set("t0", time.Time{}) // don't skew latency samples toward zero
					return c.JSON(api.WriteResult(c, nil, authErr))
				}

			} else {
				gm.Inc(metrics.Read, 1)
				if err := api.auth.Authorize(caller, auth.Action{EntityType: entityType, Op: auth.OP_READ}); err != nil {
					log.Printf("AUTH: not authorized: %s (caller: %+v request: %+v)", err, caller, c.Request())
					gm.Inc(metrics.AuthorizationFailed, 1)
					authErr := auth.Error{
						Err:        err,
						Type:       "not-authorized",
						HTTPStatus: http.StatusForbidden,
					}
					c.Set("t0", time.Time{}) // don't skew latency samples toward zero
					return readError(c, authErr)
				}
			}
			inst.Stop("authorize")

			// --------------------------------------------------------------
			// Client options via headers
			// --------------------------------------------------------------
			inst.Start("client_headers")

			// Get client version ("vX.Y") from X-Etre-Version header, if set
			clientVersion := c.Request().Header.Get(etre.VERSION_HEADER) // explicit
			if clientVersion == "" {
				clientVersion = etre.VERSION // current
			}
			m := reVersion.FindAllStringSubmatch(clientVersion, 1) // v0.9.0-alpha -> [ [v0.9, 0.9] ]
			if len(m) != 1 {
				c.Set("t0", time.Time{}) // don't skew latency samples toward zero
				err := etre.Error{
					Message:    fmt.Sprintf("invalid %s header value: %s: does not match %s (%v)", etre.VERSION_HEADER, clientVersion, reVersion, m),
					Type:       "invalid-client-version",
					HTTPStatus: http.StatusBadRequest,
				}
				if write {
					return c.JSON(api.WriteResult(c, nil, err))
				} else {
					return readError(c, err)
				}
			}
			c.Set("clientVersion", m[0][1]) // 0.9

			var ctx context.Context
			var cancel context.CancelFunc
			queryTimeout := c.Request().Header.Get(etre.QUERY_TIMEOUT_HEADER) // explicit
			if queryTimeout == "" {
				ctx, cancel = context.WithTimeout(context.Background(), api.queryTimeout)
			} else {
				d, err := time.ParseDuration(queryTimeout)
				if err != nil {
					c.Set("t0", time.Time{}) // don't skew latency samples toward zero
					err := etre.Error{
						Message:    fmt.Sprintf("invalid %s header: %s: %s", etre.QUERY_TIMEOUT_HEADER, queryTimeout, err),
						Type:       "invalid-query-timeout",
						HTTPStatus: http.StatusBadRequest,
					}
					if write {
						return c.JSON(api.WriteResult(c, nil, err))
					} else {
						return readError(c, err)
					}
				}
				ctx, cancel = context.WithTimeout(context.Background(), d)
			}
			c.Set("ctx", ctx)
			c.Set("cancelFunc", cancel)

			inst.Stop("client_headers")

			return next(c)
		}
	})

	// /////////////////////////////////////////////////////////////////////
	// Query
	// /////////////////////////////////////////////////////////////////////
	router.GET("/entities/:type", api.getEntitiesHandler)

	// /////////////////////////////////////////////////////////////////////
	// Bulk Write
	// /////////////////////////////////////////////////////////////////////
	router.POST("/entities/:type", api.postEntitiesHandler)
	router.PUT("/entities/:type", api.putEntitiesHandler)
	router.DELETE("/entities/:type", api.deleteEntitiesHandler)

	// /////////////////////////////////////////////////////////////////////
	// Single Entity
	// /////////////////////////////////////////////////////////////////////
	router.POST("/entity/:type", api.postEntityHandler)
	router.GET("/entity/:type/:id", api.getEntityHandler)
	router.PUT("/entity/:type/:id", api.putEntityHandler)
	router.DELETE("/entity/:type/:id", api.deleteEntityHandler)
	router.GET("/entity/:type/:id/labels", api.getLabelsHandler)
	router.DELETE("/entity/:type/:id/labels/:label", api.deleteLabelHandler)

	// /////////////////////////////////////////////////////////////////////
	// Metrics and status
	// /////////////////////////////////////////////////////////////////////
	router.GET("/metrics", api.metricsHandler)
	router.GET("/status", api.statusHandler)

	// /////////////////////////////////////////////////////////////////////
	// Changes
	// /////////////////////////////////////////////////////////////////////
	router.GET("/changes", api.changesHandler)

	// Called after every route/controller (even if 404)
	api.echo.Use((func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if err := next(c); err != nil {
				c.Error(err)
			}

			// Caller is nil on authenticate error, which means no metrics
			var caller auth.Caller
			if v := c.Get("caller"); v == nil {
				return nil
			} else {
				caller = v.(auth.Caller)
			}

			// Same as above: if the route has :entity param, it queried the db,
			// so finish what the pre-route middleware started
			entityType := c.Param("type")
			if entityType == "" {
				return nil
			}

			// Cancel the context
			if v := c.Get("cancelFunc"); v != nil {
				cancel := v.(context.CancelFunc)
				cancel()
			}

			t0 := c.Get("t0").(time.Time) // query start time
			if t0.IsZero() {
				return nil
			}

			// Record query latency (response time) in milliseconds
			queryLatency := time.Now().Sub(t0)
			gm := c.Get("gm").(metrics.Metrics)
			gm.Val(metrics.LatencyMs, int64(queryLatency/time.Millisecond))

			inst := c.Get("inst").(app.Instrument)

			// Did the query take too long (miss SLA)?
			if api.queryLatencySLA > 0 && queryLatency > api.queryLatencySLA {
				gm.Inc(metrics.MissSLA, 1)
				req := c.Request()
				profile := inst != app.NopInstrument
				log.Printf("Missed SLA: %s %s %s (profile: %t caller=%+v request=%+v)", req.Method, req.URL.String(), queryLatency,
					profile, caller, req)
			}

			// Print query profile if it was timed and exceeds the report threshold
			if inst != app.NopInstrument && queryLatency > api.queryProfReportThreshold {
				req := c.Request()
				log.Printf("Query profile: %s %s %s %+v (caller=%+v)", req.Method, req.URL.String(), queryLatency, inst.Report(), caller)
			}

			return nil
		}
	}))

	return api
}

// ServeHTTP allows the API to statisfy the http.HandlerFunc interface.
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	api.echo.ServeHTTP(w, r)
}

// Use adds middleware to the echo web server in the API. See
// https://echo.labstack.com/middleware for more details.
func (api *API) Use(middleware ...echo.MiddlewareFunc) {
	api.echo.Use(middleware...)
}

func (api *API) Router() *echo.Echo {
	return api.echo
}

func (api *API) Run() error {
	if api.crt != "" && api.key != "" {
		log.Printf("Listening on %s with TLS", api.addr)
		return http.ListenAndServeTLS(api.addr, api.crt, api.key, api)
	}
	log.Printf("Listening on %s", api.addr)
	return http.ListenAndServe(api.addr, api)
}

func (api *API) Stop() error {
	if api.crt != "" && api.key != "" {
		return api.echo.TLSServer.Shutdown(context.TODO())
	}
	return api.echo.Server.Shutdown(context.TODO())
}

// //////////////////////////////////////////////////////////////////////////
// Query
// //////////////////////////////////////////////////////////////////////////

func (api *API) getEntitiesHandler(c echo.Context) error {
	inst := c.Get("inst").(app.Instrument)
	inst.Start("handler")
	defer inst.Stop("handler")

	gm := c.Get("gm").(metrics.Metrics)
	gm.Inc(metrics.ReadQuery, 1)

	// Translate query string to struct
	requestLabelSelector := c.QueryParam("query")
	if requestLabelSelector == "" {
		return readError(c, ErrInvalidQuery.New("query string is empty"))
	}
	q, err := query.Translate(requestLabelSelector)
	if err != nil {
		return readError(c, ErrInvalidQuery.New("invalid query: %s", err))
	}

	// Label metrics
	gm.Val(metrics.Labels, int64(len(q.Predicates)))
	for _, p := range q.Predicates {
		gm.IncLabel(metrics.LabelRead, p.Label)
	}

	// Query Filter
	f := etre.QueryFilter{}
	queryParam := c.QueryParams() // ?x=1&y=2&z -> https://godoc.org/net/url#Values
	if csv, ok := queryParam["labels"]; ok {
		f.ReturnLabels = strings.Split(csv[0], ",")
	}
	if _, ok := queryParam["distinct"]; ok {
		f.Distinct = true
	}
	if f.Distinct && len(f.ReturnLabels) > 1 {
		return readError(c, ErrInvalidQuery.New("distinct requires only 1 return label but %d specified: %v", len(f.ReturnLabels), f.ReturnLabels))
	}

	inst.Start("db")
	ctx := c.Get("ctx").(context.Context)
	entities, err := api.es.WithContext(ctx).ReadEntities(c.Param("type"), q, f)
	inst.Stop("db")
	if err != nil {
		return readError(c, err)
	}
	gm.Val(metrics.ReadMatch, int64(len(entities)))
	return c.JSON(http.StatusOK, entities)
}

// //////////////////////////////////////////////////////////////////////////
// Bulk Write
// //////////////////////////////////////////////////////////////////////////

func (api *API) postEntitiesHandler(c echo.Context) error {
	gm := c.Get("gm").(metrics.Metrics)
	gm.Inc(metrics.CreateMany, 1)

	// Read new entities, incr metrics, and validate
	var entities []etre.Entity
	if err := c.Bind(&entities); err != nil {
		return c.JSON(api.WriteResult(c, nil, ErrInvalidContent))
	}
	if len(entities) == 0 {
		return c.JSON(api.WriteResult(c, nil, ErrNoContent))
	}
	gm.Val(metrics.CreateBulk, int64(len(entities))) // inc before validating
	if err := api.validate.Entities(entities, entity.VALIDATE_ON_CREATE); err != nil {
		return c.JSON(api.WriteResult(c, nil, err))
	}

	wo := c.Get("wo").(entity.WriteOp)
	ctx := c.Get("ctx").(context.Context)
	ids, err := api.es.WithContext(ctx).CreateEntities(wo, entities)
	gm.Inc(metrics.Created, int64(len(ids)))
	return c.JSON(api.WriteResult(c, ids, err))
}

func (api *API) putEntitiesHandler(c echo.Context) error {
	gm := c.Get("gm").(metrics.Metrics)
	gm.Inc(metrics.UpdateQuery, 1)

	// Translate query string to struct
	requestLabelSelector := c.QueryParam("query")
	if requestLabelSelector == "" {
		return c.JSON(api.WriteResult(c, nil, ErrInvalidQuery.New("query string is empty")))
	}
	q, err := query.Translate(requestLabelSelector)
	if err != nil {
		return c.JSON(api.WriteResult(c, nil, ErrInvalidQuery.New("invalid query: %s", err)))
	}

	// Read and validate patch entity
	var patch etre.Entity
	if err := c.Bind(&patch); err != nil {
		return c.JSON(api.WriteResult(c, nil, ErrInvalidContent))
	}
	if len(patch) == 0 {
		return c.JSON(api.WriteResult(c, nil, ErrNoContent))
	}
	if err := api.validate.Entities([]etre.Entity{patch}, entity.VALIDATE_ON_UPDATE); err != nil {
		return c.JSON(api.WriteResult(c, nil, err))
	}

	// Label metrics (read and update)
	gm.Val(metrics.Labels, int64(len(q.Predicates)))
	for _, p := range q.Predicates {
		gm.IncLabel(metrics.LabelRead, p.Label)
	}
	for label := range patch {
		gm.IncLabel(metrics.LabelUpdate, label)
	}

	// Patch all entities matching query
	wo := c.Get("wo").(entity.WriteOp)
	ctx := c.Get("ctx").(context.Context)
	entities, err := api.es.WithContext(ctx).UpdateEntities(wo, q, patch)
	gm.Val(metrics.UpdateBulk, int64(len(entities)))
	gm.Inc(metrics.Updated, int64(len(entities)))
	return c.JSON(api.WriteResult(c, entities, err))
}

func (api *API) deleteEntitiesHandler(c echo.Context) error {
	gm := c.Get("gm").(metrics.Metrics)
	gm.Inc(metrics.DeleteQuery, 1)

	// Translate query string to struct
	requestLabelSelector := c.QueryParam("query")
	if requestLabelSelector == "" {
		return c.JSON(api.WriteResult(c, nil, ErrInvalidQuery.New("query string is empty")))
	}
	q, err := query.Translate(requestLabelSelector)
	if err != nil {
		return c.JSON(api.WriteResult(c, nil, ErrInvalidQuery.New("invalid query: %s", err)))
	}

	// Label metrics
	gm.Val(metrics.Labels, int64(len(q.Predicates)))
	for _, p := range q.Predicates {
		gm.IncLabel(metrics.LabelRead, p.Label)
	}

	wo := c.Get("wo").(entity.WriteOp)
	ctx := c.Get("ctx").(context.Context)
	entities, err := api.es.WithContext(ctx).DeleteEntities(wo, q)
	gm.Val(metrics.DeleteBulk, int64(len(entities)))
	gm.Inc(metrics.Deleted, int64(len(entities)))
	return c.JSON(api.WriteResult(c, entities, err))
}

// //////////////////////////////////////////////////////////////////////////
// Single Enitity
// //////////////////////////////////////////////////////////////////////////

// Get one entity by _id
func (api *API) getEntityHandler(c echo.Context) error {
	gm := c.Get("gm").(metrics.Metrics)
	gm.Inc(metrics.ReadId, 1)

	// Get and validate entity id from URL (:id)
	oid, err := entityId(c)
	if err != nil {
		return readError(c, err)
	}

	// Query Filter
	f := etre.QueryFilter{}
	csv := c.QueryParam("labels")
	if csv != "" {
		f.ReturnLabels = strings.Split(csv, ",")
	}

	// Read the entity by ID
	q, _ := query.Translate("_id=" + oid.Hex())
	ctx := c.Get("ctx").(context.Context)
	entities, err := api.es.WithContext(ctx).ReadEntities(c.Param("type"), q, f)
	if err != nil {
		return readError(c, err)
	}
	if len(entities) == 0 {
		return c.JSON(http.StatusNotFound, nil)
	}
	return c.JSON(http.StatusOK, entities[0])
}

// Getting all labels for a single entity.
func (api *API) getLabelsHandler(c echo.Context) error {
	gm := c.Get("gm").(metrics.Metrics)
	gm.Inc(metrics.ReadLabels, 1)

	// Get and validate entity id from URL (:id)
	oid, err := entityId(c)
	if err != nil {
		return readError(c, err)
	}

	q, _ := query.Translate("_id=" + oid.Hex())
	ctx := c.Get("ctx").(context.Context)
	entities, err := api.es.WithContext(ctx).ReadEntities(c.Param("type"), q, etre.QueryFilter{})
	if err != nil {
		return readError(c, err)
	}
	if len(entities) == 0 {
		return c.JSON(http.StatusNotFound, nil)
	}
	return c.JSON(http.StatusOK, entities[0].Labels())
}

// --------------------------------------------------------------------------
// Single entity writes
// --------------------------------------------------------------------------

// Create one entity
func (api *API) postEntityHandler(c echo.Context) error {
	gm := c.Get("gm").(metrics.Metrics)
	gm.Inc(metrics.CreateOne, 1)

	// Read and validate new entity
	var newEntity etre.Entity
	if err := c.Bind(&newEntity); err != nil {
		return c.JSON(api.WriteResult(c, nil, ErrInvalidContent))
	}
	if len(newEntity) == 0 {
		return c.JSON(api.WriteResult(c, nil, ErrNoContent))
	}
	entities := []etre.Entity{newEntity}
	if err := api.validate.Entities(entities, entity.VALIDATE_ON_CREATE); err != nil {
		return c.JSON(api.WriteResult(c, nil, err))
	}

	// Create new entity
	wo := c.Get("wo").(entity.WriteOp)
	ctx := c.Get("ctx").(context.Context)
	ids, err := api.es.WithContext(ctx).CreateEntities(wo, entities)
	if err == nil {
		gm.Inc(metrics.Created, 1)
	}
	return c.JSON(api.WriteResult(c, ids, err))
}

// Patch one entity by _id
func (api *API) putEntityHandler(c echo.Context) error {
	gm := c.Get("gm").(metrics.Metrics)
	gm.Inc(metrics.UpdateId, 1)

	// Get and validate entity id from URL (:id).
	// wo has the same (string) value but didn't validate it.
	oid, err := entityId(c)
	if err != nil {
		return c.JSON(api.WriteResult(c, nil, err))
	}

	// Read and validate patch entity
	var patch etre.Entity
	if err := c.Bind(&patch); err != nil {
		return c.JSON(api.WriteResult(c, nil, ErrInvalidContent))
	}
	if len(patch) == 0 {
		return c.JSON(api.WriteResult(c, nil, ErrNoContent))
	}
	if err := api.validate.Entities([]etre.Entity{patch}, entity.VALIDATE_ON_UPDATE); err != nil {
		return c.JSON(api.WriteResult(c, nil, err))
	}

	// Label metrics (update)
	for label := range patch {
		gm.IncLabel(metrics.LabelUpdate, label)
	}

	// Patch one entity by ID
	wo := c.Get("wo").(entity.WriteOp)
	q, _ := query.Translate("_id=" + oid.Hex())
	ctx := c.Get("ctx").(context.Context)
	entities, err := api.es.WithContext(ctx).UpdateEntities(wo, q, patch)
	if err == nil && len(entities) == 0 {
		return c.JSON(api.WriteResult(c, nil, ErrNotFound))
	}
	if len(entities) == 1 {
		gm.Inc(metrics.Updated, 1)
	}
	return c.JSON(api.WriteResult(c, entities, err))
}

// Delete one entity by _id
func (api *API) deleteEntityHandler(c echo.Context) error {
	gm := c.Get("gm").(metrics.Metrics)
	gm.Inc(metrics.DeleteId, 1)

	// Get and validate entity id from URL (:id).
	// wo has the same (string) value but didn't validate it.
	oid, err := entityId(c)
	if err != nil {
		return c.JSON(api.WriteResult(c, nil, err))
	}

	// Delete one entity by ID
	wo := c.Get("wo").(entity.WriteOp)
	q, _ := query.Translate("_id=" + oid.Hex())
	ctx := c.Get("ctx").(context.Context)
	entities, err := api.es.WithContext(ctx).DeleteEntities(wo, q)
	if err == nil && len(entities) == 0 {
		return c.JSON(api.WriteResult(c, nil, ErrNotFound))
	}
	if len(entities) == 1 {
		gm.Inc(metrics.Deleted, 1)
	}
	return c.JSON(api.WriteResult(c, entities, err))
}

// Delete one label from one entity by _id
func (api *API) deleteLabelHandler(c echo.Context) error {
	gm := c.Get("gm").(metrics.Metrics)
	gm.Inc(metrics.DeleteLabel, 1)

	// Get and validate entity id from URL (:id).
	// wo has the same (string) value but didn't validate it.
	if _, err := entityId(c); err != nil {
		return c.JSON(api.WriteResult(c, nil, err))
	}

	label := c.Param("label")
	if label == "" {
		return c.JSON(api.WriteResult(c, nil, ErrMissingParam.New("missing label param")))
	}
	gm.IncLabel(metrics.LabelDelete, label)
	if err := api.validate.DeleteLabel(label); err != nil {
		return c.JSON(api.WriteResult(c, nil, err))
	}

	// Delete label from entity
	wo := c.Get("wo").(entity.WriteOp)
	ctx := c.Get("ctx").(context.Context)
	diff, err := api.es.WithContext(ctx).DeleteLabel(wo, label)
	if err != nil && err == etre.ErrEntityNotFound {
		return c.JSON(api.WriteResult(c, nil, ErrNotFound))
	}
	return c.JSON(api.WriteResult(c, diff, err))
}

// --------------------------------------------------------------------------
// Metrics and status
// --------------------------------------------------------------------------

func (api *API) metricsHandler(c echo.Context) error {
	// If &reset={true|false}, reset histogram samples
	reset := false
	switch strings.ToLower(c.Param("reset")) {
	case "yes", "true":
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
		return c.JSON(http.StatusOK, all)
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

	return c.JSON(http.StatusOK, all)
}

func (api *API) statusHandler(c echo.Context) error {
	status := map[string]string{
		"ok":      "yes",
		"version": etre.VERSION,
	}
	return c.JSON(http.StatusOK, status)
}

// --------------------------------------------------------------------------
// Change feed
// --------------------------------------------------------------------------

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func (api *API) changesHandler(c echo.Context) error {
	if api.cdcDisabled {
		return readError(c, ErrCDCDisabled)
	}

	// Upgrade to a WebSocket connection.
	wsConn, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		return readError(c, ErrInternal.New(err.Error()))
	}
	defer wsConn.Close()

	gm := c.Get("gm").(metrics.Metrics)
	gm.Inc(metrics.CDCClients, 1)
	defer gm.Inc(metrics.CDCClients, -1)

	var caller auth.Caller
	if v := c.Get("caller"); v != nil {
		caller = v.(auth.Caller)
	}

	clientId := fmt.Sprintf("%s@%s", caller.Name, c.Request().RemoteAddr)
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
	return nil
}

// Return error on read. Writes always return an etre.WriteResult by calling WriteResult.
func readError(c echo.Context, err error) *echo.HTTPError {
	gm := c.Get("gm") // DO NOT cast .(metrics.Metrics), only use maybeInc()

	switch v := err.(type) {
	case etre.Error:
		log.Printf("READ ERROR: %s: %s", v.Type, v.Message)
		maybeInc(metrics.ClientError, 1, gm)
		return echo.NewHTTPError(v.HTTPStatus, err)
	case entity.ValidationError:
		maybeInc(metrics.ClientError, 1, gm)
		etreError := etre.Error{
			Message:    v.Err.Error(),
			Type:       v.Type,
			HTTPStatus: http.StatusBadRequest,
		}
		return echo.NewHTTPError(etreError.HTTPStatus, etreError)
	case auth.Error:
		// Metric incremented by caller
		etreError := etre.Error{
			Message:    v.Err.Error(),
			Type:       v.Type,
			HTTPStatus: v.HTTPStatus,
		}
		return echo.NewHTTPError(etreError.HTTPStatus, etreError)
	case entity.DbError:
		maybeInc(metrics.DbError, 1, gm)
		etreError := etre.Error{
			Message:    v.Err.Error(),
			Type:       v.Type,
			HTTPStatus: http.StatusServiceUnavailable,
			EntityId:   v.EntityId,
		}
		return echo.NewHTTPError(etreError.HTTPStatus, etreError)
	default:
		maybeInc(metrics.APIError, 1, gm)
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}
}

// Return an etre.WriteResult for all writes, successful of not. v are the writes,
// if any, from entity.Store calls, which is why it can be different types.
// v and err are _not_ mutually exclusive; writes can be partially successful.
func (api *API) WriteResult(c echo.Context, v interface{}, err error) (int, interface{}) {
	var httpStatus = http.StatusInternalServerError
	var wr etre.WriteResult
	var writes []etre.Write

	gm := c.Get("gm") // DO NOT cast .(metrics.Metrics), only use maybeInc()

	// Map error to etre.Error
	if err != nil {
		switch v := err.(type) {
		case etre.Error:
			wr.Error = &v
			switch err {
			case ErrNotFound:
				// Not an error
			default:
				maybeInc(metrics.ClientError, 1, gm)
			}
		case entity.ValidationError:
			maybeInc(metrics.ClientError, 1, gm)
			wr.Error = &etre.Error{
				Message:    v.Err.Error(),
				Type:       v.Type,
				HTTPStatus: http.StatusBadRequest,
			}
		case entity.DbError:
			maybeInc(metrics.DbError, 1, gm)
			switch v.Type {
			case "duplicate-entity":
				dupeErr := ErrDuplicateEntity // copy
				dupeErr.EntityId = v.EntityId
				dupeErr.Message += " (db err: " + v.Err.Error() + ")"
				wr.Error = &dupeErr
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
			maybeInc(metrics.APIError, 1, gm)
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
	if v != nil {
		switch v.(type) {
		case []etre.Entity:
			// Diffs from UpdateEntities and DeleteEntities
			diffs := v.([]etre.Entity)
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
			ids := v.([]string)
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
			diff := v.(etre.Entity)
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
			msg := fmt.Sprintf("invalid arg type: %#v", v)
			panic(msg)
		}
		wr.Writes = writes
	}

	return httpStatus, wr
}

func writeOp(c echo.Context, caller auth.Caller) entity.WriteOp {
	wo := entity.WriteOp{
		Caller:     caller.Name,
		EntityType: c.Param("type"),
		EntityId:   c.Param("id"),
	}

	setOp := c.QueryParam("setOp")
	if setOp != "" {
		wo.SetOp = setOp
	}
	setId := c.QueryParam("setId")
	if setId != "" {
		wo.SetId = setId
	}
	setSize := c.QueryParam("setSize")
	if setSize != "" {
		i, _ := strconv.Atoi(setSize)
		wo.SetSize = i
	}

	return wo
}

func entityId(c echo.Context) (primitive.ObjectID, error) {
	id := c.Param("id")
	if id == "" {
		return primitive.ObjectID{}, ErrMissingParam.New("missing id param")
	}
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return primitive.ObjectID{}, ErrInvalidParam.New("id %s is not a valid ObjectID", id)
	}
	return oid, nil
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
