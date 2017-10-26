// Copyright 2017, Square, Inc.

// Package api provides API endpoints and controllers.
package api

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/square/etre"
	"github.com/square/etre/cdc"
	"github.com/square/etre/entity"
	"github.com/square/etre/query"

	"github.com/gorilla/websocket"
	"gopkg.in/mgo.v2/bson"
)

var (
	slugPattern   = `([\w-]+)`
	labelsPattern = `([\w-_]+)`
)

// API provides controllers for endpoints it registers with a router.
// @todo: instead of writing our own router, use the echo package
// (https://github.com/labstack/echo). That should help make the API
// code a lot more concise.
type API struct {
	addr   string
	router *Router
	es     entity.Store
	ff     cdc.FeedFactory
}

// NewAPI makes a new API.
func NewAPI(addr string, router *Router, es entity.Store, ff cdc.FeedFactory) *API {
	api := &API{
		addr:   addr,
		router: router,
		es:     es,
		ff:     ff,
	}

	api.router.AddRoute(etre.API_ROOT+"/entity/"+slugPattern, api.entityHandler, "api-entity")
	api.router.AddRoute(etre.API_ROOT+"/entity/"+slugPattern+"/"+slugPattern, api.entityHandler, "api-entity")
	api.router.AddRoute(etre.API_ROOT+"/entity/"+slugPattern+"/"+slugPattern+"/labels", api.entityLabelsHandler, "api-entity-labels")
	api.router.AddRoute(etre.API_ROOT+"/entity/"+slugPattern+"/"+slugPattern+"/labels/"+labelsPattern, api.entityDeleteLabelHandler, "api-entity-delete-label")
	api.router.AddRoute(etre.API_ROOT+"/entities/"+slugPattern, api.entitiesHandler, "api-entities")
	api.router.AddRoute(etre.API_ROOT+"/query", api.queryHandler, "query-entity")
	api.router.AddRoute(etre.API_ROOT+"/stats", api.statsHandler, "api-stats")
	api.router.AddRoute(etre.API_ROOT+"changes", api.changesHandler, "api-changes")

	return api
}

func (api *API) Router() *Router {
	return api.router
}

// //////////////////////////////////////////////////////////////////////////
// Controllers
// //////////////////////////////////////////////////////////////////////////

// {POST,GET,PUT,DELETE} /entity/{_id}
// Managing a single entity
func (api *API) entityHandler(ctx HTTPContext) {
	switch ctx.Request.Method {
	case "POST":
		api.postEntityHandler(ctx)
	case "GET":
		api.getEntityHandler(ctx)
	case "PUT":
		api.putEntityHandler(ctx)
	case "DELETE":
		api.deleteEntityHandler(ctx)
	default:
		ctx.UnsupportedAPIMethod()
	}
}

// GET /entity/{_id}/labels
// Getting all labels for a single entity
func (api *API) entityLabelsHandler(ctx HTTPContext) {
	switch ctx.Request.Method {
	case "GET":
		// TODO: fill in
	default:
		ctx.UnsupportedAPIMethod()
	}
}

// DELETE /entity/{_id}/labels/{label}
// Delete a label from a single entity
func (api *API) entityDeleteLabelHandler(ctx HTTPContext) {
	switch ctx.Request.Method {
	case "DELETE":
		// TODO: fill in
	default:
		ctx.UnsupportedAPIMethod()
	}
}

// {POST,GET,PUT,DELETE} /entity/{_id}/labels/{label}
// Manage one or more entities
func (api *API) entitiesHandler(ctx HTTPContext) {
	switch ctx.Request.Method {
	case "POST":
		api.postEntitiesHandler(ctx)
	case "GET":
		api.getEntitiesHandler(ctx)
	case "PUT":
		api.putEntitiesHandler(ctx)
	case "DELETE":
		api.deleteEntitiesHandler(ctx)
	default:
		ctx.UnsupportedAPIMethod()
	}
}

// POST /query
// Handles an edge case of having a query >2k characters
func (api *API) queryHandler(ctx HTTPContext) {
	switch ctx.Request.Method {
	case "POST":
		// TODO: fill in
	default:
		ctx.UnsupportedAPIMethod()
	}
}

// {GET,DELETE} /stats
// Manage stats
func (api *API) statsHandler(ctx HTTPContext) {
	switch ctx.Request.Method {
	case "GET":
		// TODO: fill in
	case "DELETE":
		// TODO: fill in
	default:
		ctx.UnsupportedAPIMethod()
	}
}

// --------------------------------------------------------------------------
// Single
// --------------------------------------------------------------------------

func (api *API) postEntityHandler(ctx HTTPContext) {
	if !validParams(&ctx, false) {
		return
	}

	var entity etre.Entity
	err := json.NewDecoder(ctx.Request.Body).Decode(&entity)
	if err != nil {
		ctx.APIError(ErrInternal.New("cannot decode reponse body: %v", err))
		return
	}

	ConvertFloat64ToInt(entity)

	for k, v := range entity {
		if !validValueType(v) {
			ctx.APIError(ErrBadRequest.New("Key %v has value %v with invalid type: %v. Type of value must be a string or int.", k, v, reflect.TypeOf(v)))
			return
		}
	}

	ids, err := api.es.CreateEntities(ctx.EntityType, []etre.Entity{entity}, ctx.Username())
	if ids == nil && err != nil {
		ctx.APIError(ErrDb.New(err.Error()))
		return
	}
	wr := api.WriteResults(ids, err)
	ctx.WriteCreated(wr[0])
}

func (api *API) getEntityHandler(ctx HTTPContext) {
	if !validParams(&ctx, true) {
		return
	}

	q := queryForId(ctx.EntityId)

	entities, err := api.es.ReadEntities(ctx.EntityType, q)
	if err != nil {
		ctx.APIError(ErrDb.New("database error: %s", err))
		return
	}

	if len(entities) == 0 {
		ctx.WriteNotFound()
	} else {
		ctx.WriteOK(entities[0])
	}
}

func (api *API) putEntityHandler(ctx HTTPContext) {
	if !validParams(&ctx, true) {
		return
	}

	var requestUpdate etre.Entity
	err := json.NewDecoder(ctx.Request.Body).Decode(&requestUpdate)
	if err != nil {
		ctx.APIError(ErrInternal.New("cannot decode reponse body: %v", err))
		return
	}

	q := queryForId(ctx.EntityId)

	entities, err := api.es.UpdateEntities(ctx.EntityType, q, requestUpdate, ctx.Username())
	if entities == nil && err != nil {
		ctx.APIError(ErrDb.New(err.Error()))
		return
	}
	wr := api.WriteResults(entities, err)
	ctx.WriteOK(wr[0])
}

func (api *API) deleteEntityHandler(ctx HTTPContext) {
	if !validParams(&ctx, true) {
		return
	}

	q := queryForId(ctx.EntityId)

	entities, err := api.es.DeleteEntities(ctx.EntityType, q, ctx.Username())
	if entities == nil && err != nil {
		ctx.APIError(ErrDb.New(err.Error()))
		return
	}
	wr := api.WriteResults(entities, err)
	ctx.WriteOK(wr[0])
}

// --------------------------------------------------------------------------
// Bulk
// --------------------------------------------------------------------------

func (api *API) postEntitiesHandler(ctx HTTPContext) {
	if !validParams(&ctx, false) {
		return
	}

	var entities []etre.Entity
	err := json.NewDecoder(ctx.Request.Body).Decode(&entities)
	if err != nil {
		ctx.APIError(ErrInternal.New("cannot decode reponse body: %v", err))
		return
	}

	for _, e := range entities {
		ConvertFloat64ToInt(e)

		for k, v := range e {
			if !validValueType(v) {
				ctx.APIError(ErrBadRequest.New("Key %v has value %v with invalid type: %v. Type of value must be a string or int.", k, v, reflect.TypeOf(v)))
				return
			}
		}
	}

	ids, err := api.es.CreateEntities(ctx.EntityType, entities, ctx.Username())
	if ids == nil && err != nil {
		ctx.APIError(ErrDb.New(err.Error()))
		return
	}
	wr := api.WriteResults(ids, err)
	ctx.WriteCreated(wr)
}

func (api *API) getEntitiesHandler(ctx HTTPContext) {
	if !validParams(&ctx, false) {
		return
	}

	// Translate URL query to query struct
	queryParam := ctx.Request.Form["query"]
	if queryParam == nil {
		ctx.APIError(ErrMissingParam.New("missing param: query"))
		return
	}

	requestLabelSelector := queryParam[0]
	if requestLabelSelector == "" {
		ctx.APIError(ErrInvalidQuery.New("query string is empty"))
		return
	}

	q, err := query.Translate(requestLabelSelector)
	if err != nil {
		ctx.APIError(ErrInvalidQuery.New("invalid query: %s", err))
		return
	}

	entities, err := api.es.ReadEntities(ctx.EntityType, q)
	if err != nil {
		ctx.APIError(ErrDb.New("database error: %s", err))
		return
	}

	ctx.WriteOK(entities)
}

func (api *API) putEntitiesHandler(ctx HTTPContext) {
	if !validParams(&ctx, false) {
		return
	}

	// Translate URL query to query struct
	queryParam := ctx.Request.Form["query"]
	if queryParam == nil {
		ctx.APIError(ErrMissingParam.New("missing param: query"))
		return
	}

	requestLabelSelector := queryParam[0]
	if requestLabelSelector == "" {
		ctx.APIError(ErrInvalidQuery.New("query string is empty"))
		return
	}

	q, err := query.Translate(requestLabelSelector)
	if err != nil {
		ctx.APIError(ErrInvalidQuery.New("invalid query: %s", err))
		return
	}

	// Decode request update
	var requestUpdate etre.Entity
	err = json.NewDecoder(ctx.Request.Body).Decode(&requestUpdate)
	if err != nil {
		ctx.APIError(ErrInternal.New("cannot decode reponse body: %v", err))
		return
	}

	entities, err := api.es.UpdateEntities(ctx.EntityType, q, requestUpdate, ctx.Username())
	if entities == nil && err != nil {
		ctx.APIError(ErrDb.New(err.Error()))
		return
	}
	wr := api.WriteResults(entities, err)
	ctx.WriteOK(wr)
}

func (api *API) deleteEntitiesHandler(ctx HTTPContext) {
	if !validParams(&ctx, false) {
		return
	}

	// Translate URL query to query struct
	queryParam := ctx.Request.Form["query"]
	if queryParam == nil {
		ctx.APIError(ErrMissingParam.New("missing param: query"))
		return
	}

	requestLabelSelector := queryParam[0]
	if requestLabelSelector == "" {
		ctx.APIError(ErrInvalidQuery.New("query string is empty"))
		return
	}

	q, err := query.Translate(requestLabelSelector)
	if err != nil {
		ctx.APIError(ErrInvalidQuery.New("invalid query: %s", err))
		return
	}

	entities, err := api.es.DeleteEntities(ctx.EntityType, q, ctx.Username())
	if entities == nil && err != nil {
		ctx.APIError(ErrDb.New(err.Error()))
		return
	}
	wr := api.WriteResults(entities, err)
	ctx.WriteOK(wr)
}

// --------------------------------------------------------------------------
// Change feed
// --------------------------------------------------------------------------

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

// GET /changes
// Consume change feed
func (api *API) changesHandler(ctx HTTPContext) {
	switch ctx.Request.Method {
	case "GET":
		// Upgrade to a WebSocket connection.
		wsConn, err := upgrader.Upgrade(ctx.Response, ctx.Request, nil)
		if err != nil {
			ctx.APIError(ErrInternal.New(err.Error()))
			return
		}

		// Create and run a feed.
		f := api.ff.MakeWebsocket(wsConn)
		if err := f.Run(); err != nil {
			ctx.APIError(ErrInternal.New(err.Error()))
			return
		}
	default:
		ctx.UnsupportedAPIMethod()
	}
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
	return reflect.TypeOf(v).Kind() == reflect.String || reflect.TypeOf(v).Kind() == reflect.Int
}

func validParams(ctx *HTTPContext, needEntityId bool) bool {
	// Arguments = []string{
	//  "/api/v1/entity/nodes/59efdf425669fc0217553d1d",
	//  "nodes",
	//   "59efdf425669fc0217553d1d",
	// }
	ctx.EntityType = ctx.Arguments[1]
	// @todo: validate ^
	if needEntityId {
		if len(ctx.Arguments) != 3 {
			ctx.APIError(ErrMissingParam.New("missing entityId param, got: %s", ctx.Arguments))
			return false
		}

		ctx.EntityId = ctx.Arguments[2]
		if ctx.EntityId == "" {
			ctx.APIError(ErrMissingParam.New("entityId param is empty"))
			return false
		}

		if !bson.IsObjectIdHex(ctx.EntityId) {
			ctx.APIError(ErrInvalidParam.New("invalid entityId: %s", ctx.EntityId))
			return false
		}
	}
	return true
}
