// Copyright 2017-2018, Square, Inc.

// Package api provides API endpoints and controllers.
package api

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"reflect"
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
	addr     string
	es       entity.Store
	ff       cdc.FeedFactory
	teamAuth team.Authorizer
	querySLA uint
	// --
	echo *echo.Echo
}

// NewAPI makes a new API.
func NewAPI(appCtx app.Context) *API {
	api := &API{
		addr:     appCtx.Config.Server.Addr,
		es:       appCtx.Store,
		ff:       appCtx.FeedFactory,
		teamAuth: appCtx.TeamAuth,
		echo:     echo.New(),
	}

	router := api.echo.Group(etre.API_ROOT)

	// Called before every route/controller
	router.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			fmt.Println("--- TEAM", c.Path())
			// Before every route, get and set the team identified by the header:
			t, err := api.teamAuth.Team(c.Request().Header.Get("X-Etre-Team"))
			if err != nil {
				return echo.NewHTTPError(http.StatusUnauthorized, err.Error())
			}
			c.Set("team", t)

			// All routes with an :entity param query the db, so increment
			// the "all" metric for them so this line is repeated in every
			// controller. Also set t0 for measuring query latency.
			entityType := c.Param("type")
			if entityType != "" {
				t.Metrics.Entity[entityType].Query.All.Inc(1)

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

				c.Set("t0", time.Now()) // query start time
			}

			return next(c)
		}
	})

	// Called after every route/controller (even if 404)
	api.echo.Use((func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if err := next(c); err != nil {
				c.Error(err)
			}

			// Same as above: if the route has :entity param, it queried the db,
			// so finish what the pre-route middleware started
			entityType := c.Param("type")
			if entityType != "" {
				// Record query latency (response time) in milliseconds
				t0 := c.Get("t0").(time.Time) // query start time
				queryLatencyMs := int64(time.Now().Sub(t0) / time.Millisecond)
				t := c.Get("team").(team.Team)
				t.Metrics.Entity[entityType].Query.Latency.Update(queryLatencyMs)

				// Did the query take too long (miss SLA)?
				if t.QueryLatencySLA > 0 && uint(queryLatencyMs) > t.QueryLatencySLA {
					t.Metrics.Entity[entityType].Query.MissSLA.Inc(1)
				}
			}

			return nil
		}
	}))

	api.echo.Use(middleware.Recover()) // catch all panics

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

// /////////////////////////////////////////////////////////////////////////////
// Controllers
// /////////////////////////////////////////////////////////////////////////////

// -----------------------------------------------------------------------------
// Query
// -----------------------------------------------------------------------------

func (api *API) getEntitiesHandler(c echo.Context) error {
	entityType := c.Param("type")      // from resource path
	t := c.Get("team").(team.Team)     // team.Team from middleware
	em := t.Metrics.Entity[entityType] // entity metrics
	em.Query.ReadQuery.Inc(1)

	if err := validateParams(c, false); err != nil {
		return handleError(t, err)
	}

	// Translate URL query to query struct.
	requestLabelSelector := c.QueryParam("query")
	if requestLabelSelector == "" {
		return handleError(t, ErrInvalidQuery.New("query string is empty"))
	}
	q, err := query.Translate(requestLabelSelector)
	if err != nil {
		return handleError(t, ErrInvalidQuery.New("invalid query: %s", err))
	}
	for _, p := range q.Predicates {
		em.ReadLabel(p.Label) // label metrics
	}

	// Query Filter
	f := etre.QueryFilter{}
	csvReturnLabels := c.QueryParam("labels")
	if csvReturnLabels != "" {
		f.ReturnLabels = strings.Split(csvReturnLabels, ",")
	}

	entities, err := api.es.ReadEntities(entityType, q, f)
	if err != nil {
		return handleError(t, ErrDb.New("database error: %s", err))
	}

	return c.JSON(http.StatusOK, entities)
}

// Handles an edge case of having a query >2k characters.
func (api *API) queryHandler(c echo.Context) error {
	return echo.NewHTTPError(http.StatusNotImplemented, nil) // @todo
}

// --------------------------------------------------------------------------
// Bulk
// --------------------------------------------------------------------------

func (api *API) postEntitiesHandler(c echo.Context) error {
	entityType := c.Param("type")      // from resource path
	t := c.Get("team").(team.Team)     // team.Team from middleware
	em := t.Metrics.Entity[entityType] // entity metrics

	if err := validateParams(c, false); err != nil {
		return handleError(t, err)
	}

	wo := writeOp(c)

	// Get entities from request payload.
	var entities []etre.Entity
	if err := c.Bind(&entities); err != nil {
		return handleError(t, ErrInternal.New(err.Error()))
	}
	em.Query.InsertBulk.Inc(int64(len(entities)))

	for _, e := range entities {
		ConvertFloat64ToInt(e)
		for k, v := range e {
			if !validValueType(v) {
				return handleError(t, ErrBadRequest.New("Key %v has value %v with invalid type: %v. "+
					"Type of value must be a string or int.", k, v, reflect.TypeOf(v)))
			}
		}
	}

	ids, err := api.es.CreateEntities(wo, entities)
	if ids == nil && err != nil {
		return handleError(t, ErrDb.New(err.Error()))
	}
	wr := api.WriteResults(ids, err)

	return c.JSON(http.StatusCreated, wr)
}

func (api *API) putEntitiesHandler(c echo.Context) error {
	entityType := c.Param("type")      // from resource path
	t := c.Get("team").(team.Team)     // team.Team from middleware
	em := t.Metrics.Entity[entityType] // entity metrics

	if err := validateParams(c, false); err != nil {
		return handleError(t, err)
	}

	wo := writeOp(c)

	// Translate URL query to query struct.
	requestLabelSelector := c.QueryParam("query")
	if requestLabelSelector == "" {
		return handleError(t, ErrInvalidQuery.New("query string is empty"))
	}
	q, err := query.Translate(requestLabelSelector)
	if err != nil {
		return handleError(t, ErrInvalidQuery.New("invalid query: %s", err))
	}
	for _, p := range q.Predicates {
		em.ReadLabel(p.Label) // label metrics
	}

	// Get entities from request payload.
	var requestUpdate etre.Entity
	if err := c.Bind(&requestUpdate); err != nil {
		return handleError(t, ErrInternal.New(err.Error()))
	}

	entities, err := api.es.UpdateEntities(wo, q, requestUpdate)
	if entities == nil && err != nil {
		return handleError(t, ErrDb.New(err.Error()))
	}

	em.Query.UpdateBulk.Inc(int64(len(entities)))
	wr := api.WriteResults(entities, err)
	return c.JSON(http.StatusOK, wr)
}

func (api *API) deleteEntitiesHandler(c echo.Context) error {
	entityType := c.Param("type")      // from resource path
	t := c.Get("team").(team.Team)     // team.Team from middleware
	em := t.Metrics.Entity[entityType] // entity metrics

	if err := validateParams(c, false); err != nil {
		return handleError(t, err)
	}

	wo := writeOp(c)

	// Translate URL query to query struct.
	requestLabelSelector := c.QueryParam("query")
	if requestLabelSelector == "" {
		return handleError(t, ErrInvalidQuery.New("query string is empty"))
	}
	q, err := query.Translate(requestLabelSelector)
	if err != nil {
		return handleError(t, ErrInvalidQuery.New("invalid query: %s", err))
	}
	for _, p := range q.Predicates {
		em.ReadLabel(p.Label) // label metrics
	}

	entities, err := api.es.DeleteEntities(wo, q)
	if entities == nil && err != nil {
		return handleError(t, ErrDb.New(err.Error()))
	}

	em.Query.DeleteBulk.Inc(int64(len(entities)))
	wr := api.WriteResults(entities, err)
	return c.JSON(http.StatusOK, wr)
}

// -----------------------------------------------------------------------------
// Enitity
// -----------------------------------------------------------------------------

func (api *API) postEntityHandler(c echo.Context) error {
	entityType := c.Param("type")      // from resource path
	t := c.Get("team").(team.Team)     // team.Team from middleware
	em := t.Metrics.Entity[entityType] // entity metrics
	em.Query.Insert.Inc(1)

	if err := validateParams(c, false); err != nil {
		return handleError(t, err)
	}

	wo := writeOp(c)

	// Get entity from request payload.
	var entity etre.Entity
	if err := c.Bind(&entity); err != nil {
		return handleError(t, ErrInternal.New(err.Error()))
	}

	ConvertFloat64ToInt(entity)
	for k, v := range entity {
		if !validValueType(v) {
			return handleError(t, ErrBadRequest.New("Key %v has value %v with invalid type: %v. "+
				"Type of value must be a string or int.", k, v, reflect.TypeOf(v)))
		}
	}

	ids, err := api.es.CreateEntities(wo, []etre.Entity{entity})
	if ids == nil && err != nil {
		return handleError(t, ErrDb.New(err.Error()))
	}
	wr := api.WriteResults(ids, err)

	return c.JSON(http.StatusCreated, wr[0])
}

func (api *API) getEntityHandler(c echo.Context) error {
	entityType := c.Param("type")
	entityId := c.Param("id")
	t := c.Get("team").(team.Team)
	em := t.Metrics.Entity[entityType]
	em.Query.ReadId.Inc(1)

	if err := validateParams(c, true); err != nil {
		return handleError(t, err)
	}

	q := queryForId(entityId)

	// Query Filter
	f := etre.QueryFilter{}
	csvReturnLabels := c.QueryParam("labels")
	if csvReturnLabels != "" {
		f.ReturnLabels = strings.Split(csvReturnLabels, ",")
	}

	entities, err := api.es.ReadEntities(entityType, q, f)
	if err != nil {
		return handleError(t, ErrDb.New(err.Error()))
	}

	if len(entities) == 0 {
		return c.JSON(http.StatusNotFound, nil)
	}

	return c.JSON(http.StatusOK, entities[0])
}

func (api *API) putEntityHandler(c echo.Context) error {
	entityType := c.Param("type")
	t := c.Get("team").(team.Team)
	em := t.Metrics.Entity[entityType]
	em.Query.Update.Inc(1)

	if err := validateParams(c, true); err != nil {
		return handleError(t, err)
	}

	wo := writeOp(c)

	// Get entities from request payload.
	var requestUpdate etre.Entity
	if err := c.Bind(&requestUpdate); err != nil {
		return handleError(t, ErrInternal.New(err.Error()))
	}

	q := queryForId(wo.EntityId)

	entities, err := api.es.UpdateEntities(wo, q, requestUpdate)
	if entities == nil && err != nil {
		return handleError(t, ErrDb.New(err.Error()))
	}

	if len(entities) == 0 {
		return c.JSON(http.StatusNotFound, nil)
	}

	wr := api.WriteResults(entities, err)

	return c.JSON(http.StatusOK, wr[0])
}

func (api *API) deleteEntityHandler(c echo.Context) error {
	entityType := c.Param("type")
	t := c.Get("team").(team.Team)
	em := t.Metrics.Entity[entityType]
	em.Query.Delete.Inc(1)

	if err := validateParams(c, true); err != nil {
		return handleError(t, err)
	}

	wo := writeOp(c)

	q := queryForId(wo.EntityId)

	entities, err := api.es.DeleteEntities(wo, q)
	if entities == nil && err != nil {
		return handleError(t, ErrDb.New(err.Error()))
	}

	if len(entities) == 0 {
		return c.JSON(http.StatusNotFound, nil)
	}

	wr := api.WriteResults(entities, err)

	return c.JSON(http.StatusOK, wr[0])
}

// Getting all labels for a single entity.
func (api *API) entityLabelsHandler(c echo.Context) error {
	entityType := c.Param("type")
	entityId := c.Param("id")
	t := c.Get("team").(team.Team)
	em := t.Metrics.Entity[entityType]
	em.Query.ReadLabels.Inc(1)

	if err := validateParams(c, true); err != nil {
		return handleError(t, err)
	}

	q := queryForId(entityId)
	entities, err := api.es.ReadEntities(entityType, q, etre.QueryFilter{})
	if err != nil {
		return handleError(t, ErrDb.New(err.Error()))
	}

	if len(entities) == 0 {
		return c.JSON(http.StatusNotFound, nil)
	}

	return c.JSON(http.StatusOK, entities[0].Labels())
}

// Delete a label from a single entity.
func (api *API) entityDeleteLabelHandler(c echo.Context) error {
	entityType := c.Param("type")
	t := c.Get("team").(team.Team)
	em := t.Metrics.Entity[entityType]
	em.Query.DeleteLabel.Inc(1)

	if err := validateParams(c, true); err != nil {
		return handleError(t, err)
	}

	label := c.Param("label")
	if label == "" {
		return ErrMissingParam.New("missing label param")
	}
	em.DeleteLabel(label)

	// Don't allow deleting metalabel
	if etre.IsMetalabel(label) {
		errResp := etre.Error{
			Message:    "deleting metalabel " + label + " is not allowed",
			Type:       "delete-metalabel",
			HTTPStatus: http.StatusForbidden,
		}
		return c.JSON(http.StatusForbidden, errResp)

	}

	wo := writeOp(c)

	diff, err := api.es.DeleteLabel(wo, label)
	if err != nil {
		switch err {
		case entity.ErrNotFound:
			return c.JSON(http.StatusNotFound, nil)
		default:
			return handleError(t, ErrDb.New(err.Error()))
		}
	}

	wr := api.WriteResults(diff, err)
	return c.JSON(http.StatusOK, wr[0])
}

// --------------------------------------------------------------------------
// Stats
// --------------------------------------------------------------------------

func (api *API) metricsHandler(c echo.Context) error {
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

func (api *API) statusHandler(c echo.Context) error {
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

func (api *API) changesHandler(c echo.Context) error {
	entityType := c.Param("type")
	t := c.Get("team").(team.Team)
	em := t.Metrics.Entity[entityType]

	if api.ff == nil {
		return handleError(t, ErrCDCDisabled)
	}

	em.CDC.Clients.Inc(1)
	defer em.CDC.Clients.Dec(1)

	// Upgrade to a WebSocket connection.
	wsConn, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		return handleError(t, ErrInternal.New(err.Error()))
	}

	// Create and run a feed.
	f := api.ff.MakeWebsocket(wsConn)
	if err := f.Run(); err != nil {
		return handleError(t, ErrInternal.New(err.Error()))
	}

	return nil
}

// //////////////////////////////////////////////////////////////////////////
// Helper funcs
// //////////////////////////////////////////////////////////////////////////

// WriteResults makes a slice of etre.WriteResult for each item in v, which
// is either []string{<ids>} on POST/create or []etre.Entity{} on PUT/update
// and DELETE/delete. If err is not nil, it's the first (last and only)
// error on write which applies to the last WriteResult in the returned slice.
// The caller must handle this case:
//
//   if v == nil && err != nil {
//
// In that case, no writes were attempted, presumably because of a low-level db
// issue (e.g. db is offiline). In other words: v most not be nil.
func (api *API) WriteResults(v interface{}, err error) []etre.WriteResult {
	var wr []etre.WriteResult
	if diffs, ok := v.([]etre.Entity); ok {
		n := len(diffs)
		if err != nil {
			n += 1
		}
		wr = make([]etre.WriteResult, n)
		for i, diff := range diffs {
			// _id from db is a bson.ObjectId, which we need to encode
			// as a hex string.
			id := hex.EncodeToString([]byte(diff["_id"].(bson.ObjectId)))

			wr[i] = etre.WriteResult{
				Id:   id,
				URI:  api.addr + etre.API_ROOT + "/entity/" + id,
				Diff: diff,
			}
		}
		if err != nil {
			wr[len(wr)-1] = etre.WriteResult{
				Error: err.Error(),
			}
		}
	} else if ids, ok := v.([]string); ok {
		n := len(ids)
		if err != nil {
			n += 1
		}
		wr = make([]etre.WriteResult, n)
		for i, id := range ids {
			wr[i] = etre.WriteResult{
				Id:  id,
				URI: api.addr + etre.API_ROOT + "/entity/" + id,
			}
		}
		if err != nil {
			wr[len(wr)-1] = etre.WriteResult{
				Error: err.Error(),
			}
		}
	} else if diff, ok := v.(etre.Entity); ok {
		wr = make([]etre.WriteResult, 1)
		id := hex.EncodeToString([]byte(diff["_id"].(bson.ObjectId)))
		wr[0] = etre.WriteResult{
			Id:   id,
			URI:  api.addr + etre.API_ROOT + "/entity/" + id,
			Diff: diff,
		}
		if err != nil {
			wr[0] = etre.WriteResult{
				Error: err.Error(),
			}
		}
	} else {
		msg := fmt.Sprintf("api.WriteResults: invalid arg v: %v, expected []etre.Entity or []string",
			reflect.TypeOf(v))
		panic(msg)
	}
	return wr
}

// JSON treats all numbers as floats. Given this, when we see a float with
// decimal values of all 0, it is unclear if the user passed in 3.0 (type
// float) or 3 (type int). So, since we cannot tell the difference between a
// some float numbers and integer numbers, we cast all floats to ints. This
// means that floats with non-zero decimal values, such as 3.14 (type float),
// will get truncated to 3i (type int) in this case.
func ConvertFloat64ToInt(entity etre.Entity) {
	for k, v := range entity {
		if reflect.TypeOf(v).Kind() == reflect.Float64 {
			entity[k] = int(v.(float64))
		}
	}
}

// //////////////////////////////////////////////////////////////////////////
// Private funcs
// //////////////////////////////////////////////////////////////////////////

func writeOp(c echo.Context) entity.WriteOp {
	wo := entity.WriteOp{
		User:       getUsername(c),
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

// _id is not a valid field name to pass to query.Translate, so we manually
// create a query object.
func queryForId(id string) query.Query {
	return query.Query{
		Predicates: []query.Predicate{
			query.Predicate{
				Label:    "_id",
				Operator: "=",
				Value:    bson.ObjectIdHex(id),
			},
		},
	}
}

// Values in entity must be of type string or int. This is because the query
// language we use only supports querying by string or int. See more at:
// github.com/square/etre/query
func validValueType(v interface{}) bool {
	k := reflect.TypeOf(v).Kind()
	return k == reflect.String || k == reflect.Int || k == reflect.Bool
}

func validateParams(c echo.Context, needEntityId bool) error {
	if c.Param("type") == "" {
		return ErrMissingParam.New("missing type param")
	}

	if needEntityId {
		id := c.Param("id")
		if id == "" {
			return ErrMissingParam.New("missing id param")
		}

		if !bson.IsObjectIdHex(id) {
			return ErrInvalidParam.New("id %s is not a valid bson.ObjectId", id)
		}
	}

	return nil
}

func getUsername(c echo.Context) string {
	username := "?"
	if val := c.Get("username"); val != nil {
		if u, ok := val.(string); ok {
			username = u
		}
	}
	return username
}

func handleError(t team.Team, err error) *echo.HTTPError {
	// Increment metrics for specific errors
	switch err {
	case ErrInvalidQuery, ErrMissingParam, ErrInvalidParam, ErrBadRequest:
		t.Metrics.Global.ClientError.Inc(1)
	case ErrDb:
		t.Metrics.Global.DbError.Inc(1)
	default:
		t.Metrics.Global.APIError.Inc(1)
	}

	// If it's an etre.Error (it should be), then return with the
	// error-specific HTTP status
	switch v := err.(type) {
	case etre.Error:
		return echo.NewHTTPError(v.HTTPStatus, err)
	}

	// Error catchall: HTTP status 500
	return echo.NewHTTPError(http.StatusInternalServerError, err)
}
