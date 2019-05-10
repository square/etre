// Copyright 2017-2019, Square, Inc.

// Package api provides API endpoints and controllers.
package api

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/square/etre"
	"github.com/square/etre/app"
	"github.com/square/etre/auth"
	"github.com/square/etre/cdc"
	"github.com/square/etre/entity"
	"github.com/square/etre/metrics"
	"github.com/square/etre/query"

	"github.com/globalsign/mgo/bson"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
)

// API provides controllers for endpoints it registers with a router.
type API struct {
	addr                 string
	crt                  string
	key                  string
	es                   entity.Store
	validate             entity.Validator
	ff                   cdc.FeedFactory
	auth                 auth.Plugin
	metricsStore         metrics.Store
	metricsFactory       metrics.Factory
	systemMetrics        metrics.Metrics
	defaultClientVersion string
	queryLatencySLA      time.Duration
	// --
	echo *echo.Echo
}

var reVersion = regexp.MustCompile(`^v?(\d+\.\d+)`)

const longQueryPath = etre.API_ROOT + "/query/:type"

// NewAPI makes a new API.
func NewAPI(appCtx app.Context) *API {
	queryLatencySLA, _ := time.ParseDuration(appCtx.Config.Metrics.QueryLatencySLA)
	api := &API{
		addr:                 appCtx.Config.Server.Addr,
		crt:                  appCtx.Config.Server.TLSCert,
		key:                  appCtx.Config.Server.TLSKey,
		es:                   appCtx.EntityStore,
		validate:             appCtx.EntityValidator,
		ff:                   appCtx.FeedFactory,
		auth:                 appCtx.Auth,
		metricsFactory:       appCtx.MetricsFactory,
		metricsStore:         appCtx.MetricsStore,
		systemMetrics:        appCtx.SystemMetrics,
		defaultClientVersion: appCtx.Config.Server.DefaultClientVersion,
		queryLatencySLA:      queryLatencySLA,
		// --
		echo: echo.New(),
	}

	router := api.echo.Group(etre.API_ROOT)

	router.Use(middleware.Recover()) // catch all panics

	// Called before every route/controller
	router.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			entityType := c.Param("type")
			if entityType != "" {
				c.Set("t0", time.Now()) // query start time
			}

			api.systemMetrics.Inc(metrics.Query, 1)

			method := c.Request().Method
			write := method == "PUT" || (method == "POST" && c.Path() != longQueryPath) || method == "DELETE"

			// -----------------------------------------------------------------------
			// Client version
			// -----------------------------------------------------------------------
			// Get client version ("vX.Y") from X-Etre-Version header, if set
			clientVersion := c.Request().Header.Get(etre.VERSION_HEADER) // explicit
			vf := "X-Etre-Version header"
			if clientVersion == "" {
				if api.defaultClientVersion != "" {
					clientVersion = api.defaultClientVersion // default
					vf = "config.server.default_client_version"
				} else {
					clientVersion = etre.VERSION // current
					vf = "etre.VERSION"
				}
			}
			m := reVersion.FindAllStringSubmatch(clientVersion, 1) // v0.9.0-alpha -> [ [v0.9, 0.9] ]
			if len(m) != 1 {
				errMsg := fmt.Sprintf("invalid client (es) version from %s: '%s', does not match %s (%v)", vf, clientVersion, reVersion, m)
				log.Println(errMsg)
				c.Set("t0", time.Time{}) // don't skew latency samples toward zero
				return echo.NewHTTPError(http.StatusBadRequest, errMsg)
			}
			c.Set("clientVersion", m[0][1]) // 0.9

			// -----------------------------------------------------------------------
			// Authenticate
			// -----------------------------------------------------------------------
			caller, err := api.auth.Authenticate(c.Request())
			if err != nil {
				log.Printf("Authenticate error: %s", err)
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
				return readError(authErr)
			}
			c.Set("caller", caller)

			// -----------------------------------------------------------------------
			// Metrics
			// -----------------------------------------------------------------------
			if entityType == "" {
				return next(c)
			}

			// This makes a metrics.Group which is a 1-to-many proxy for every
			// metric group in caller.MetricGroups. E.g. if the groups are "ods"
			// and "finch", then every metric is recorded in both groups.
			gm := api.metricsFactory.Make(caller.MetricGroups)
			c.Set("gm", gm)

			// If entity type is invalid, unset t0 so the LatencyMs metric isn't
			// skewed with artificially fast queries
			if err := api.validate.EntityType(entityType); err != nil {
				log.Printf("Invalid entity type: '%s': caller=%+v request=%+v", entityType, caller, c.Request())
				gm.Inc(metrics.InvalidEntityType, 1)
				c.Set("t0", time.Time{}) // don't skew latency samples toward zero
				if write {
					return c.JSON(api.WriteResult(c, nil, err))
				}
				return readError(err)
			}

			// Bind group metrics to entity type
			gm.EntityType(entityType)
			gm.Inc(metrics.Query, 1)

			// auth.Manager extracts trace values from X-Etre-Trace header
			if caller.Trace != nil {
				gm.Trace(caller.Trace)
			}

			// Routes with an :entity param query the db, so increment query.Metrics
			// and query.Read or .Write depending on the route. Specific Read/Write
			// metrics are set in the controller.
			if write {
				gm.Inc(metrics.Write, 1)

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
					gm.Inc(metrics.AuthorizationFailed, 1)
					authErr := auth.Error{
						Err:        err,
						Type:       "not-authorized",
						HTTPStatus: http.StatusForbidden,
					}
					c.Set("t0", time.Time{}) // don't skew latency samples toward zero
					return readError(authErr)
				}
			}
			return next(c)
		}
	})

	// /////////////////////////////////////////////////////////////////////
	// Query
	// /////////////////////////////////////////////////////////////////////
	router.GET("/entities/:type", api.getEntitiesHandler)
	router.POST("/query/:type", api.queryHandler)

	// /////////////////////////////////////////////////////////////////////
	// Bulk
	// /////////////////////////////////////////////////////////////////////
	router.POST("/entities/:type", api.postEntitiesHandler)
	router.PUT("/entities/:type", api.putEntitiesHandler)
	router.DELETE("/entities/:type", api.deleteEntitiesHandler)

	// /////////////////////////////////////////////////////////////////////
	// Entity
	// /////////////////////////////////////////////////////////////////////
	router.POST("/entity/:type", api.postEntityHandler)
	router.GET("/entity/:type/:id", api.getEntityHandler)
	router.PUT("/entity/:type/:id", api.putEntityHandler)
	router.DELETE("/entity/:type/:id", api.deleteEntityHandler)
	router.GET("/entity/:type/:id/labels", api.entityLabelsHandler)
	router.DELETE("/entity/:type/:id/labels/:label", api.entityDeleteLabelHandler)

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

			// Record query latency (response time) in milliseconds
			t0 := c.Get("t0").(time.Time) // query start time
			if !t0.IsZero() {
				queryLatency := time.Now().Sub(t0)
				gm := c.Get("gm").(metrics.Metrics)
				gm.Val(metrics.LatencyMs, int64(queryLatency/time.Millisecond))

				// Did the query take too long (miss SLA)?
				if api.queryLatencySLA > 0 && queryLatency > api.queryLatencySLA {
					log.Printf("Missed SLA: '%s': caller=%+v request=%+v", entityType, caller, c.Request())
					gm.Inc(metrics.MissSLA, 1)
				}
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

// -----------------------------------------------------------------------------
// Query
// -----------------------------------------------------------------------------

func (api *API) getEntitiesHandler(c echo.Context) error {
	gm := c.Get("gm").(metrics.Metrics)
	gm.Inc(metrics.ReadQuery, 1)

	// Validate
	if err := validateParams(c, false); err != nil {
		return readError(err)
	}

	// Translate query string to struct
	requestLabelSelector := c.QueryParam("query")
	if requestLabelSelector == "" {
		return readError(ErrInvalidQuery.New("query string is empty"))
	}
	q, err := query.Translate(requestLabelSelector)
	if err != nil {
		return readError(ErrInvalidQuery.New("invalid query: %s", err))
	}

	// Label metrics
	gm.Val(metrics.Labels, int64(len(q.Predicates)))
	for _, p := range q.Predicates {
		gm.IncLabel(metrics.LabelRead, p.Label)
	}

	// Query Filter
	f := etre.QueryFilter{}
	csv := c.QueryParam("labels")
	if csv != "" {
		f.ReturnLabels = strings.Split(csv, ",")
	}

	entityType := c.Param("type")
	entities, err := api.es.ReadEntities(entityType, q, f)
	if err != nil {
		return readError(ErrDb.New(err.Error()))
	}
	gm.Val(metrics.ReadMatch, int64(len(entities)))
	return c.JSON(http.StatusOK, entities)
}

// Handles an edge case of having a query >2k characters.
func (api *API) queryHandler(c echo.Context) error {
	return echo.NewHTTPError(http.StatusNotImplemented, nil) // @todo
}

// -----------------------------------------------------------------------------
// Bulk
// -----------------------------------------------------------------------------

func (api *API) postEntitiesHandler(c echo.Context) error {
	gm := c.Get("gm").(metrics.Metrics)
	gm.Inc(metrics.CreateMany, 1)

	if err := validateParams(c, false); err != nil {
		return c.JSON(api.WriteResult(c, nil, err))
	}

	// Read new entities, incr metris, and validate
	var entities []etre.Entity
	if err := c.Bind(&entities); err != nil {
		return c.JSON(api.WriteResult(c, nil, ErrInternal.New(err.Error())))
	}
	gm.Val(metrics.CreateBulk, int64(len(entities))) // inc before validating
	if err := api.validate.Entities(entities, entity.VALIDATE_ON_CREATE); err != nil {
		return c.JSON(api.WriteResult(c, nil, err))
	}

	wo := c.Get("wo").(entity.WriteOp)
	ids, err := api.es.CreateEntities(wo, entities)
	gm.Inc(metrics.Created, int64(len(ids)))
	return c.JSON(api.WriteResult(c, ids, err))
}

func (api *API) putEntitiesHandler(c echo.Context) error {
	gm := c.Get("gm").(metrics.Metrics)
	gm.Inc(metrics.UpdateQuery, 1)

	if err := validateParams(c, false); err != nil {
		return c.JSON(api.WriteResult(c, nil, err))
	}

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

	// Read and validate patch entity
	var patch etre.Entity
	if err := c.Bind(&patch); err != nil {
		return c.JSON(api.WriteResult(c, nil, ErrInternal.New(err.Error())))
	}
	if err := api.validate.Entities([]etre.Entity{patch}, entity.VALIDATE_ON_UPDATE); err != nil {
		return c.JSON(api.WriteResult(c, nil, err))
	}

	// Label metrics (update)
	for label := range patch {
		gm.IncLabel(metrics.LabelUpdate, label)
	}

	// Patch all entities matching query
	wo := c.Get("wo").(entity.WriteOp)
	entities, err := api.es.UpdateEntities(wo, q, patch)
	gm.Val(metrics.UpdateBulk, int64(len(entities)))
	gm.Inc(metrics.Updated, int64(len(entities)))
	return c.JSON(api.WriteResult(c, entities, err))
}

func (api *API) deleteEntitiesHandler(c echo.Context) error {
	gm := c.Get("gm").(metrics.Metrics)
	gm.Inc(metrics.DeleteQuery, 1)

	if err := validateParams(c, false); err != nil {
		return c.JSON(api.WriteResult(c, nil, err))
	}

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
	entities, err := api.es.DeleteEntities(wo, q)
	gm.Val(metrics.DeleteBulk, int64(len(entities)))
	gm.Inc(metrics.Deleted, int64(len(entities)))
	return c.JSON(api.WriteResult(c, entities, err))
}

// -----------------------------------------------------------------------------
// Enitity
// -----------------------------------------------------------------------------

// Create one entity
func (api *API) postEntityHandler(c echo.Context) error {
	gm := c.Get("gm").(metrics.Metrics)
	gm.Inc(metrics.CreateOne, 1)

	if err := validateParams(c, false); err != nil {
		return c.JSON(api.WriteResult(c, nil, err))
	}

	// Read and validate new entity
	var newEntity etre.Entity
	if err := c.Bind(&newEntity); err != nil {
		return c.JSON(api.WriteResult(c, nil, ErrInternal.New(err.Error())))
	}
	entities := []etre.Entity{newEntity}
	if err := api.validate.Entities(entities, entity.VALIDATE_ON_CREATE); err != nil {
		return c.JSON(api.WriteResult(c, nil, err))
	}

	// Create new entity
	wo := c.Get("wo").(entity.WriteOp)
	ids, err := api.es.CreateEntities(wo, entities)
	if err == nil {
		gm.Inc(metrics.Created, 1)
	}
	return c.JSON(api.WriteResult(c, ids, err))
}

// Get one entity by _id
func (api *API) getEntityHandler(c echo.Context) error {
	gm := c.Get("gm").(metrics.Metrics)
	gm.Inc(metrics.ReadId, 1)

	// Validate
	if err := validateParams(c, true); err != nil {
		return readError(err)
	}

	// Query Filter
	f := etre.QueryFilter{}
	csv := c.QueryParam("labels")
	if csv != "" {
		f.ReturnLabels = strings.Split(csv, ",")
	}

	// Read the entity by ID
	entityType := c.Param("type")
	entityId := c.Param("id")
	entities, err := api.es.ReadEntities(entityType, query.IdEqual(entityId), f)
	if err != nil {
		return readError(ErrDb.New(err.Error()))
	}
	if len(entities) == 0 {
		return c.JSON(http.StatusNotFound, nil)
	}
	return c.JSON(http.StatusOK, entities[0])
}

// Patch one entity by _id
func (api *API) putEntityHandler(c echo.Context) error {
	gm := c.Get("gm").(metrics.Metrics)
	gm.Inc(metrics.UpdateId, 1)

	if err := validateParams(c, true); err != nil {
		return c.JSON(api.WriteResult(c, nil, err))
	}

	// Read and validate patch entity
	var patch etre.Entity
	if err := c.Bind(&patch); err != nil {
		return c.JSON(api.WriteResult(c, nil, ErrInternal.New(err.Error())))
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
	entities, err := api.es.UpdateEntities(wo, query.IdEqual(wo.EntityId), patch)
	if err == nil && len(entities) == 0 {
		return c.JSON(http.StatusNotFound, nil)
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

	if err := validateParams(c, true); err != nil {
		return c.JSON(api.WriteResult(c, nil, err))
	}

	// Delete one entity by ID
	wo := c.Get("wo").(entity.WriteOp)
	entities, err := api.es.DeleteEntities(wo, query.IdEqual(wo.EntityId))
	if err == nil && len(entities) == 0 {
		return c.JSON(http.StatusNotFound, nil)
	}
	if len(entities) == 1 {
		gm.Inc(metrics.Deleted, 1)
	}
	return c.JSON(api.WriteResult(c, entities, err))
}

// Getting all labels for a single entity.
func (api *API) entityLabelsHandler(c echo.Context) error {
	gm := c.Get("gm").(metrics.Metrics)
	gm.Inc(metrics.ReadLabels, 1)

	// Validate
	if err := validateParams(c, true); err != nil {
		return readError(err)
	}

	entityType := c.Param("type")
	entityId := c.Param("id")
	entities, err := api.es.ReadEntities(entityType, query.IdEqual(entityId), etre.QueryFilter{})
	if err != nil {
		return readError(ErrDb.New(err.Error()))
	}
	if len(entities) == 0 {
		return c.JSON(http.StatusNotFound, nil)
	}
	return c.JSON(http.StatusOK, entities[0].Labels())
}

// Delete one label from one entity by _id
func (api *API) entityDeleteLabelHandler(c echo.Context) error {
	gm := c.Get("gm").(metrics.Metrics)
	gm.Inc(metrics.DeleteLabel, 1)

	// Validate
	if err := validateParams(c, true); err != nil {
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
	diff, err := api.es.DeleteLabel(wo, label)
	if err != nil && err == etre.ErrEntityNotFound {
		return c.JSON(http.StatusNotFound, nil)
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
	status := map[string]interface{}{
		"ok":      true,
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
	if api.ff == nil {
		return readError(ErrCDCDisabled)
	}

	gm := c.Get("gm").(metrics.Metrics)
	gm.Inc(metrics.CDCClients, 1)
	defer gm.Inc(metrics.CDCClients, -1)

	// Upgrade to a WebSocket connection.
	wsConn, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		return readError(ErrInternal.New(err.Error()))
	}

	// Create and run a feed.
	f := api.ff.MakeWebsocket(wsConn)
	if err := f.Run(); err != nil {
		return readError(ErrInternal.New(err.Error()))
	}

	return nil
}

// Return error on read. Writes always return an etre.WriteResult by calling WriteResult.
func readError(err error) *echo.HTTPError {
	switch v := err.(type) {
	case etre.Error:
		return echo.NewHTTPError(v.HTTPStatus, err)
	case entity.ValidationError:
		etreError := etre.Error{
			Message:    v.Err.Error(),
			Type:       v.Type,
			HTTPStatus: http.StatusBadRequest,
		}
		return echo.NewHTTPError(etreError.HTTPStatus, etreError)
	case auth.Error:
		etreError := etre.Error{
			Message:    v.Err.Error(),
			Type:       v.Type,
			HTTPStatus: v.HTTPStatus,
		}
		return echo.NewHTTPError(etreError.HTTPStatus, etreError)
	default:
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

	// Map error to etre.Error
	if err != nil {
		switch v := err.(type) {
		case etre.Error:
			wr.Error = &v
		case entity.ValidationError:
			wr.Error = &etre.Error{
				Message:    v.Err.Error(),
				Type:       v.Type,
				HTTPStatus: http.StatusBadRequest,
			}
		case entity.DbError:
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
					HTTPStatus: http.StatusInternalServerError,
					EntityId:   v.EntityId,
				}
			}
		case auth.Error:
			wr.Error = &etre.Error{
				Message:    v.Err.Error(),
				Type:       v.Type,
				HTTPStatus: v.HTTPStatus,
			}
		default:
			wr.Error = &etre.Error{
				Message:    err.Error(),
				Type:       "unhandled-error",
				HTTPStatus: http.StatusInternalServerError,
			}
		}
		httpStatus = wr.Error.HTTPStatus

		// Increment metrics for specific errors
		gm := c.Get("gm").(metrics.Metrics)
		switch err {
		case ErrInvalidQuery, ErrMissingParam, ErrInvalidParam:
			gm.Inc(metrics.ClientError, 1)
		case ErrDb:
			gm.Inc(metrics.DbError, 1)
		default:
			gm.Inc(metrics.APIError, 1)
		}
	} else {
		httpStatus = http.StatusOK
	}

	// No writes, probably error before call to entity.Store
	if v == nil {
		if c.Get("clientVersion") == "0.8" {
			// v0.8 clients expect only []etre.Write or etre.Write if there's an entity ID
			writes = []etre.Write{}
			if err != nil {
				writes = append(writes, etre.Write{Id: wr.Error.EntityId, Error: err.Error()})
			}
			if c.Param("id") != "" {
				return httpStatus, writes[0]
			}
			return httpStatus, writes
		}
		return httpStatus, wr
	}

	// Map writes to []etre.Write
	switch v.(type) {
	case []etre.Entity:
		// Diffs from UpdateEntities and DeleteEntities
		diffs := v.([]etre.Entity)
		writes = make([]etre.Write, len(diffs))
		for i, diff := range diffs {
			// _id from db is bson.ObjectId, convert to string
			id := hex.EncodeToString([]byte(diff["_id"].(bson.ObjectId)))
			writes[i] = etre.Write{
				Id:   id,
				URI:  api.addr + etre.API_ROOT + "/entity/" + id,
				Diff: diff,
			}
		}
	case []string:
		// Entity _id from CreateEntities
		ids := v.([]string)
		writes = make([]etre.Write, len(ids))
		for i, id := range ids {
			writes[i] = etre.Write{
				Id:  id,
				URI: api.addr + etre.API_ROOT + "/entity/" + id,
			}
		}
		httpStatus = http.StatusCreated
	case etre.Entity:
		// Entity from DeleteLabel
		diff := v.(etre.Entity)
		// _id from db is bson.ObjectId, convert to string
		id := hex.EncodeToString([]byte(diff["_id"].(bson.ObjectId)))
		writes = []etre.Write{
			{
				Id:   id,
				URI:  api.addr + etre.API_ROOT + "/entity/" + id,
				Diff: diff,
			},
		}
	default:
		msg := fmt.Sprintf("invalid arg type: %#v", v)
		panic(msg)
	}
	wr.Writes = writes

	if c.Get("clientVersion") == "0.8" {
		// v0.8 clients expect only []etre.Write or etre.Write if there's an entity ID
		if err != nil {
			writes = append(writes, etre.Write{Id: wr.Error.EntityId, Error: err.Error()})
		}
		if c.Param("id") != "" {
			return httpStatus, writes[0]
		}
		return httpStatus, writes
	}
	return httpStatus, wr
}

func writeOp(c echo.Context, caller auth.Caller) entity.WriteOp {
	wo := entity.WriteOp{
		User:       caller.Name,
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

func validateParams(c echo.Context, needEntityId bool) error {
	if c.Param("type") == "" {
		return ErrMissingParam.New("missing type param")
	}
	if !needEntityId {
		return nil
	}
	id := c.Param("id")
	if id == "" {
		return ErrMissingParam.New("missing id param")
	}
	if !bson.IsObjectIdHex(id) {
		return ErrInvalidParam.New("id %s is not a valid bson.ObjectId", id)
	}
	return nil
}
