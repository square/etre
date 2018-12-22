// Copyright 2017-2018, Square, Inc.

// Package api provides API endpoints and controllers.
package api

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/square/etre"
	"github.com/square/etre/app"
	"github.com/square/etre/cdc"
	"github.com/square/etre/entity"
	"github.com/square/etre/query"
	"github.com/square/etre/team"

	"github.com/globalsign/mgo/bson"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
)

// API provides controllers for endpoints it registers with a router.
type API struct {
	addr                 string
	es                   entity.Store
	validate             entity.Validator
	ff                   cdc.FeedFactory
	teamAuth             team.Authorizer
	defaultClientVersion string
	// --
	echo *echo.Echo
}

var reVersion = regexp.MustCompile(`^v?(\d+\.\d+)`)

// NewAPI makes a new API.
func NewAPI(appCtx app.Context) API {
	api := API{
		addr:                 appCtx.Config.Server.Addr,
		es:                   appCtx.EntityStore,
		validate:             appCtx.EntityValidator,
		ff:                   appCtx.FeedFactory,
		teamAuth:             appCtx.TeamAuth,
		defaultClientVersion: appCtx.Config.Server.DefaultClientVersion,
		// --
		echo: echo.New(),
	}

	router := api.echo.Group(etre.API_ROOT)

	router.Use(middleware.Recover()) // catch all panics

	// Called before every route/controller
	router.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			// -----------------------------------------------------------------------
			// Client version
			// -----------------------------------------------------------------------
			// Get client version ("vX.Y") from X-Etre-Version header, if set
			clientVersion := c.Request().Header.Get("X-Etre-Version") // explicit
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
				return echo.NewHTTPError(http.StatusBadRequest, errMsg)
			}
			c.Set("clientVersion", m[0][1]) // 0.9

			// -----------------------------------------------------------------------
			// WriteOp
			// -----------------------------------------------------------------------
			// All writes (PUT, POST, DELETE) require a write op
			method := c.Request().Method
			if (method == "PUT" || method == "POST" || method == "DELETE") && (c.Path() != etre.API_ROOT+"/query/:type") {
				wo := writeOp(c)
				if err := api.validate.WriteOp(wo); err != nil {
					return c.JSON(api.WriteResult(c, nil, err))
				}
				c.Set("wo", wo)
			}

			// -----------------------------------------------------------------------
			// Team
			// -----------------------------------------------------------------------
			// Before every route, get and set the team identified by the header:
			t, err := api.teamAuth.Team(c.Request().Header.Get("X-Etre-Team"))
			if err != nil {
				return echo.NewHTTPError(http.StatusUnauthorized, err.Error())
			}
			c.Set("team", t)

			// -----------------------------------------------------------------------
			// Metrics
			// -----------------------------------------------------------------------
			// All routes with an :entity param query the db, so increment
			// the "all" metric for them so this line is repeated in every
			// controller. Also set t0 for measuring query latency.
			entityType := c.Param("type")
			if entityType != "" {
				t.Metrics.Entity[entityType].Query.All.Inc(1)
				c.Set("t0", time.Now()) // query start time

				// GET with :entity is a read. PUT, POST, and DELETE are writes,
				// with one exception.
				switch c.Request().Method {
				case "GET":
					t.Metrics.Entity[entityType].Query.Read.Inc(1)
					if err := api.teamAuth.Allowed(t, team.OP_READ, entityType); err != nil {
						return echo.NewHTTPError(http.StatusUnauthorized, err.Error())
					}
				case "PUT", "POST", "DELETE":
					if c.Path() != etre.API_ROOT+"/query/:type" {
						t.Metrics.Entity[entityType].Query.Write.Inc(1)
					} else {
						t.Metrics.Entity[entityType].Query.Read.Inc(1) // POST /query/:type is a read
					}
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
	// Stats and status
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

			// Same as above: if the route has :entity param, it queried the db,
			// so finish what the pre-route middleware started
			entityType := c.Param("type")
			if entityType == "" {
				return nil
			}

			// Record query latency (response time) in milliseconds
			t0 := c.Get("t0").(time.Time) // query start time
			queryLatencyMs := int64(time.Now().Sub(t0) / time.Millisecond)
			t := c.Get("team").(team.Team)
			t.Metrics.Entity[entityType].Query.Latency.Update(queryLatencyMs)

			// Did the query take too long (miss SLA)?
			if t.QueryLatencySLA > 0 && uint(queryLatencyMs) > t.QueryLatencySLA {
				t.Metrics.Entity[entityType].Query.MissSLA.Inc(1)
			}
			return nil
		}
	}))

	return api
}

// ServeHTTP allows the API to statisfy the http.HandlerFunc interface.
func (api API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	api.echo.ServeHTTP(w, r)
}

// Use adds middleware to the echo web server in the API. See
// https://echo.labstack.com/middleware for more details.
func (api API) Use(middleware ...echo.MiddlewareFunc) {
	api.echo.Use(middleware...)
}

func (api API) Router() *echo.Echo {
	return api.echo
}

// -----------------------------------------------------------------------------
// Query
// -----------------------------------------------------------------------------

func (api API) getEntitiesHandler(c echo.Context) error {
	entityType := c.Param("type")      // from resource path
	t := c.Get("team").(team.Team)     // team.Team from middleware
	em := t.Metrics.Entity[entityType] // entity metrics
	em.Query.ReadQuery.Inc(1)

	// Validate
	if err := validateParams(c, false); err != nil {
		return readError(err)
	}
	if err := api.validate.EntityType(entityType); err != nil {
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
	for _, p := range q.Predicates {
		em.ReadLabel(p.Label)
	}

	// Query Filter
	f := etre.QueryFilter{}
	csv := c.QueryParam("labels")
	if csv != "" {
		f.ReturnLabels = strings.Split(csv, ",")
	}

	entities, err := api.es.ReadEntities(entityType, q, f)
	if err != nil {
		return readError(ErrDb.New(err.Error()))
	}
	return c.JSON(http.StatusOK, entities)
}

// Handles an edge case of having a query >2k characters.
func (api API) queryHandler(c echo.Context) error {
	return echo.NewHTTPError(http.StatusNotImplemented, nil) // @todo
}

// --------------------------------------------------------------------------
// Bulk
// --------------------------------------------------------------------------

func (api API) postEntitiesHandler(c echo.Context) error {
	entityType := c.Param("type")      // from resource path
	t := c.Get("team").(team.Team)     // team.Team from middleware
	em := t.Metrics.Entity[entityType] // entity metrics

	if err := validateParams(c, false); err != nil {
		return c.JSON(api.WriteResult(c, nil, err))
	}

	// Read new entities, incr metris, and validate
	var entities []etre.Entity
	if err := c.Bind(&entities); err != nil {
		return c.JSON(api.WriteResult(c, nil, ErrInternal.New(err.Error())))
	}
	em.Query.InsertBulk.Inc(int64(len(entities)))
	if err := api.validate.Entities(entities, entity.VALIDATE_ON_CREATE); err != nil {
		return c.JSON(api.WriteResult(c, nil, err))
	}

	wo := c.Get("wo").(entity.WriteOp)
	ids, err := api.es.CreateEntities(wo, entities)
	return c.JSON(api.WriteResult(c, ids, err))
}

func (api API) putEntitiesHandler(c echo.Context) error {
	entityType := c.Param("type")      // from resource path
	t := c.Get("team").(team.Team)     // team.Team from middleware
	em := t.Metrics.Entity[entityType] // entity metrics

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
	for _, p := range q.Predicates {
		em.ReadLabel(p.Label)
	}

	// Read and validate patch entity
	var patch etre.Entity
	if err := c.Bind(&patch); err != nil {
		return c.JSON(api.WriteResult(c, nil, ErrInternal.New(err.Error())))
	}
	if err := api.validate.Entities([]etre.Entity{patch}, entity.VALIDATE_ON_UPDATE); err != nil {
		return c.JSON(api.WriteResult(c, nil, err))
	}

	// Patch all entities matching query
	wo := c.Get("wo").(entity.WriteOp)
	entities, err := api.es.UpdateEntities(wo, q, patch)
	em.Query.UpdateBulk.Inc(int64(len(entities)))
	return c.JSON(api.WriteResult(c, entities, err))
}

func (api API) deleteEntitiesHandler(c echo.Context) error {
	entityType := c.Param("type")      // from resource path
	t := c.Get("team").(team.Team)     // team.Team from middleware
	em := t.Metrics.Entity[entityType] // entity metrics

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
	for _, p := range q.Predicates {
		em.ReadLabel(p.Label)
	}

	wo := c.Get("wo").(entity.WriteOp)
	entities, err := api.es.DeleteEntities(wo, q)
	em.Query.DeleteBulk.Inc(int64(len(entities)))
	return c.JSON(api.WriteResult(c, entities, err))
}

// -----------------------------------------------------------------------------
// Enitity
// -----------------------------------------------------------------------------

// Create one entity
func (api API) postEntityHandler(c echo.Context) error {
	entityType := c.Param("type")      // from resource path
	t := c.Get("team").(team.Team)     // team.Team from middleware
	em := t.Metrics.Entity[entityType] // entity metrics
	em.Query.Insert.Inc(1)

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
	return c.JSON(api.WriteResult(c, ids, err))
}

// Get one entity by _id
func (api *API) getEntityHandler(c echo.Context) error {
	entityType := c.Param("type")
	t := c.Get("team").(team.Team)
	em := t.Metrics.Entity[entityType]
	em.Query.ReadId.Inc(1)

	// Validate
	if err := validateParams(c, true); err != nil {
		return readError(err)
	}
	if err := api.validate.EntityType(entityType); err != nil {
		return readError(err)
	}

	// Query Filter
	f := etre.QueryFilter{}
	csv := c.QueryParam("labels")
	if csv != "" {
		f.ReturnLabels = strings.Split(csv, ",")
	}

	// Read the entity by ID
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
	entityType := c.Param("type")
	t := c.Get("team").(team.Team)
	em := t.Metrics.Entity[entityType]
	em.Query.Update.Inc(1)

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

	// Patch one entity by ID
	wo := c.Get("wo").(entity.WriteOp)
	entities, err := api.es.UpdateEntities(wo, query.IdEqual(wo.EntityId), patch)
	if err == nil && len(entities) == 0 {
		return c.JSON(http.StatusNotFound, nil)
	}
	return c.JSON(api.WriteResult(c, entities, err))
}

// Delete one entity by _id
func (api *API) deleteEntityHandler(c echo.Context) error {
	entityType := c.Param("type")
	t := c.Get("team").(team.Team)
	em := t.Metrics.Entity[entityType]
	em.Query.Delete.Inc(1)

	if err := validateParams(c, true); err != nil {
		return c.JSON(api.WriteResult(c, nil, err))
	}

	// Delete one entity by ID
	wo := c.Get("wo").(entity.WriteOp)
	entities, err := api.es.DeleteEntities(wo, query.IdEqual(wo.EntityId))
	if err == nil && len(entities) == 0 {
		return c.JSON(http.StatusNotFound, nil)
	}
	return c.JSON(api.WriteResult(c, entities, err))
}

// Getting all labels for a single entity.
func (api API) entityLabelsHandler(c echo.Context) error {
	entityType := c.Param("type")
	entityId := c.Param("id")
	t := c.Get("team").(team.Team)
	em := t.Metrics.Entity[entityType]
	em.Query.ReadLabels.Inc(1)

	// Validate
	if err := validateParams(c, true); err != nil {
		return readError(err)
	}
	if err := api.validate.EntityType(entityType); err != nil {
		return readError(err)
	}

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
func (api API) entityDeleteLabelHandler(c echo.Context) error {
	entityType := c.Param("type")
	t := c.Get("team").(team.Team)
	em := t.Metrics.Entity[entityType]
	em.Query.DeleteLabel.Inc(1)

	// Validate
	if err := validateParams(c, true); err != nil {
		return c.JSON(api.WriteResult(c, nil, err))
	}
	label := c.Param("label")
	if label == "" {
		return c.JSON(api.WriteResult(c, nil, ErrMissingParam.New("missing label param")))
	}
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
// Stats
// --------------------------------------------------------------------------

func (api API) metricsHandler(c echo.Context) error {
	teams := api.teamAuth.List()
	all := etre.Metrics{
		Teams: make([]etre.MetricsReport, len(teams)),
	}
	for i, t := range teams {
		m := t.Metrics.Report()
		m.Team = t.Name
		all.Teams[i] = m
	}
	return c.JSON(http.StatusOK, all)
}

func (api API) statusHandler(c echo.Context) error {
	status := map[string]interface{}{
		"ok": true,
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

func (api API) changesHandler(c echo.Context) error {
	entityType := c.Param("type")
	t := c.Get("team").(team.Team)
	em := t.Metrics.Entity[entityType]

	if api.ff == nil {
		return readError(ErrCDCDisabled)
	}

	em.CDC.Clients.Inc(1)
	defer em.CDC.Clients.Dec(1)

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

	// Increment metrics for specific errors
	t := c.Get("team").(team.Team)
	switch err {
	case ErrInvalidQuery, ErrMissingParam, ErrInvalidParam:
		t.Metrics.Global.ClientError.Inc(1)
	case ErrDb:
		t.Metrics.Global.DbError.Inc(1)
	default:
		t.Metrics.Global.APIError.Inc(1)
	}

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
		default:
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

func writeOp(c echo.Context) entity.WriteOp {
	username := "?"
	if val := c.Get("username"); val != nil {
		if u, ok := val.(string); ok {
			username = u
		}
	}

	wo := entity.WriteOp{
		User:       username,
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
