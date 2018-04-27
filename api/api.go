// Copyright 2017, Square, Inc.

// Package api provides API endpoints and controllers.
package api

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/square/etre"
	"github.com/square/etre/cdc"
	"github.com/square/etre/entity"
	"github.com/square/etre/query"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo"
	"gopkg.in/mgo.v2/bson"
)

var (
	slugPattern   = `([\w-]+)`
	labelsPattern = `([\w-_]+)`
)

// API provides controllers for endpoints it registers with a router.
type API struct {
	addr string
	es   entity.Store
	ff   cdc.FeedFactory
	// --
	echo *echo.Echo
}

// NewAPI makes a new API.
func NewAPI(addr string, es entity.Store, ff cdc.FeedFactory) *API {
	api := &API{
		addr: addr,
		es:   es,
		ff:   ff,
		echo: echo.New(),
	}

	router := api.echo.Group(etre.API_ROOT)

	// /////////////////////////////////////////////////////////////////////
	// Query
	// /////////////////////////////////////////////////////////////////////
	router.GET("/entities/:type", api.getEntitiesHandler)
	router.POST("/query", api.queryHandler)

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
	router.DELETE("/entity/:type/:id/lables/:labels", api.entityDeleteLabelHandler)

	// /////////////////////////////////////////////////////////////////////
	// Stats and status
	// /////////////////////////////////////////////////////////////////////
	router.GET("/stats", api.statsHandler)
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
	if err := validateParams(c, false); err != nil {
		return handleError(err)
	}
	entityType := c.Param("type")

	// Translate URL query to query struct.
	requestLabelSelector := c.QueryParam("query")
	if requestLabelSelector == "" {
		return handleError(ErrInvalidQuery.New("query string is empty"))
	}

	q, err := query.Translate(requestLabelSelector)
	if err != nil {
		return handleError(ErrInvalidQuery.New("invalid query: %s", err))
	}

	// Query Filter
	f := etre.QueryFilter{}
	csvReturnLabels := c.QueryParam("labels")
	if csvReturnLabels != "" {
		f.ReturnLabels = strings.Split(csvReturnLabels, ",")
	}

	entities, err := api.es.ReadEntities(entityType, q, f)
	if err != nil {
		return handleError(ErrDb.New("database error: %s", err))
	}

	return c.JSON(http.StatusOK, entities)
}

// Handles an edge case of having a query >2k characters.
func (api *API) queryHandler(c echo.Context) error {
	// @todo: implement this
	return nil
}

// --------------------------------------------------------------------------
// Bulk
// --------------------------------------------------------------------------

func (api *API) postEntitiesHandler(c echo.Context) error {
	if err := validateParams(c, false); err != nil {
		return handleError(err)
	}

	wo := writeOp(c)

	// Get entities from request payload.
	var entities []etre.Entity
	if err := c.Bind(&entities); err != nil {
		return handleError(ErrInternal.New(err.Error()))
	}

	for _, e := range entities {
		ConvertFloat64ToInt(e)
		for k, v := range e {
			if !validValueType(v) {
				return handleError(ErrBadRequest.New("Key %v has value %v with invalid type: %v. "+
					"Type of value must be a string or int.", k, v, reflect.TypeOf(v)))
			}
		}
	}

	ids, err := api.es.CreateEntities(wo, entities)
	if ids == nil && err != nil {
		return handleError(ErrDb.New(err.Error()))
	}
	wr := api.WriteResults(ids, err)

	return c.JSON(http.StatusCreated, wr)
}

func (api *API) putEntitiesHandler(c echo.Context) error {
	if err := validateParams(c, false); err != nil {
		return handleError(err)
	}

	wo := writeOp(c)

	// Translate URL query to query struct.
	requestLabelSelector := c.QueryParam("query")
	if requestLabelSelector == "" {
		return handleError(ErrInvalidQuery.New("query string is empty"))
	}

	q, err := query.Translate(requestLabelSelector)
	if err != nil {
		return handleError(ErrInvalidQuery.New("invalid query: %s", err))
	}

	// Get entities from request payload.
	var requestUpdate etre.Entity
	if err := c.Bind(&requestUpdate); err != nil {
		return handleError(ErrInternal.New(err.Error()))
	}

	entities, err := api.es.UpdateEntities(wo, q, requestUpdate)
	if entities == nil && err != nil {
		return handleError(ErrDb.New(err.Error()))
	}
	wr := api.WriteResults(entities, err)

	return c.JSON(http.StatusOK, wr)
}

func (api *API) deleteEntitiesHandler(c echo.Context) error {
	if err := validateParams(c, false); err != nil {
		return handleError(err)
	}

	wo := writeOp(c)

	// Translate URL query to query struct.
	requestLabelSelector := c.QueryParam("query")
	if requestLabelSelector == "" {
		return handleError(ErrInvalidQuery.New("query string is empty"))
	}

	q, err := query.Translate(requestLabelSelector)
	if err != nil {
		return handleError(ErrInvalidQuery.New("invalid query: %s", err))
	}

	entities, err := api.es.DeleteEntities(wo, q)
	if entities == nil && err != nil {
		return handleError(ErrDb.New(err.Error()))
	}
	wr := api.WriteResults(entities, err)

	return c.JSON(http.StatusOK, wr)
}

// -----------------------------------------------------------------------------
// Enitity
// -----------------------------------------------------------------------------

func (api *API) postEntityHandler(c echo.Context) error {
	if err := validateParams(c, false); err != nil {
		return handleError(err)
	}

	wo := writeOp(c)

	// Get entity from request payload.
	var entity etre.Entity
	if err := c.Bind(&entity); err != nil {
		return handleError(ErrInternal.New(err.Error()))
	}

	ConvertFloat64ToInt(entity)
	for k, v := range entity {
		if !validValueType(v) {
			return handleError(ErrBadRequest.New("Key %v has value %v with invalid type: %v. "+
				"Type of value must be a string or int.", k, v, reflect.TypeOf(v)))
		}
	}

	ids, err := api.es.CreateEntities(wo, []etre.Entity{entity})
	if ids == nil && err != nil {
		return handleError(ErrDb.New(err.Error()))
	}
	wr := api.WriteResults(ids, err)

	return c.JSON(http.StatusCreated, wr[0])
}

func (api *API) getEntityHandler(c echo.Context) error {
	if err := validateParams(c, true); err != nil {
		return handleError(err)
	}
	entityType := c.Param("type")
	entityId := c.Param("id")

	q := queryForId(entityId)

	// Query Filter
	f := etre.QueryFilter{}
	csvReturnLabels := c.QueryParam("labels")
	if csvReturnLabels != "" {
		f.ReturnLabels = strings.Split(csvReturnLabels, ",")
	}

	entities, err := api.es.ReadEntities(entityType, q, f)
	if err != nil {
		return handleError(ErrDb.New(err.Error()))
	}

	if len(entities) == 0 {
		return c.JSON(http.StatusNotFound, nil)
	}

	return c.JSON(http.StatusOK, entities[0])
}

func (api *API) putEntityHandler(c echo.Context) error {
	if err := validateParams(c, true); err != nil {
		return handleError(err)
	}

	wo := writeOp(c)

	// Get entities from request payload.
	var requestUpdate etre.Entity
	if err := c.Bind(&requestUpdate); err != nil {
		return handleError(ErrInternal.New(err.Error()))
	}

	q := queryForId(wo.EntityId)

	entities, err := api.es.UpdateEntities(wo, q, requestUpdate)
	if entities == nil && err != nil {
		return handleError(ErrDb.New(err.Error()))
	}

	if len(entities) == 0 {
		return c.JSON(http.StatusNotFound, nil)
	}

	wr := api.WriteResults(entities, err)

	return c.JSON(http.StatusOK, wr[0])
}

func (api *API) deleteEntityHandler(c echo.Context) error {
	if err := validateParams(c, true); err != nil {
		return handleError(err)
	}

	wo := writeOp(c)

	q := queryForId(wo.EntityId)

	entities, err := api.es.DeleteEntities(wo, q)
	if entities == nil && err != nil {
		return handleError(ErrDb.New(err.Error()))
	}

	if len(entities) == 0 {
		return c.JSON(http.StatusNotFound, nil)
	}

	wr := api.WriteResults(entities, err)

	return c.JSON(http.StatusOK, wr[0])
}

// Getting all labels for a single entity.
func (api *API) entityLabelsHandler(c echo.Context) error {
	// @todo: implement this
	return nil
}

// Delete a label from a single entity.
func (api *API) entityDeleteLabelHandler(c echo.Context) error {
	// @todo: implement this
	return nil
}

// --------------------------------------------------------------------------
// Stats
// --------------------------------------------------------------------------

func (api *API) statsHandler(c echo.Context) error {
	// @todo: implement this
	return nil
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
	if api.ff == nil {
		return handleError(ErrCDCDisabled)
	}

	// Upgrade to a WebSocket connection.
	wsConn, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		return handleError(ErrInternal.New(err.Error()))
	}

	// Create and run a feed.
	f := api.ff.MakeWebsocket(wsConn)
	if err := f.Run(); err != nil {
		return handleError(ErrInternal.New(err.Error()))
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
		[]query.Predicate{
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
			return handleError(ErrInvalidParam.New("invalid id: %s", id))
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

func handleError(err error) *echo.HTTPError {
	switch v := err.(type) {
	case etre.Error:
		return echo.NewHTTPError(v.HTTPStatus, err)
	default:
		switch err {
		default:
			return echo.NewHTTPError(http.StatusInternalServerError, err)
		}
	}

	return nil
}
