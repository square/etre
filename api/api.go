// Copyright 2017, Square, Inc.

package api

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/square/etre"
	"github.com/square/etre/entity"
	"github.com/square/etre/feed"
	"github.com/square/etre/query"
	"github.com/square/etre/router"

	"github.com/gorilla/websocket"
)

const (
	API_ROOT                    = "/api/v1/"
	ENTITY_ID_PATTERN           = "([A-Za-z0-9]+)"
	REQUEST_LABEL_PATTERN       = "([A-Za-z0-9]+)"
	REQUEST_QUERY_PATTERN       = "([\\s\\S]*)"
	REQUEST_ENTITY_TYPE_PATTERN = "([\\s\\S]*)"
)

// API provides controllers for endpoints it registers with a router.
//
// @todo: instead of writing our own router, use the echo package
// (https://github.com/labstack/echo). That should help make the API
// code a lot more concise.
type API struct {
	Router *router.Router
	em     entity.Manager
	ff     feed.FeedFactory
}

// NewAPI makes a new API.
func NewAPI(router *router.Router, em entity.Manager, ff feed.FeedFactory) *API {
	api := &API{
		Router: router,
		em:     em,
		ff:     ff,
	}

	api.Router.AddRoute(API_ROOT+"entity/"+REQUEST_ENTITY_TYPE_PATTERN, api.entityHandler, "api-entity")
	api.Router.AddRoute(API_ROOT+"entity/"+REQUEST_ENTITY_TYPE_PATTERN+"/"+ENTITY_ID_PATTERN, api.entityHandler, "api-entity")
	api.Router.AddRoute(API_ROOT+"entity/"+REQUEST_ENTITY_TYPE_PATTERN+"/"+ENTITY_ID_PATTERN+"/labels", api.entityLabelsHandler, "api-entity-labels")
	api.Router.AddRoute(API_ROOT+"entity/"+REQUEST_ENTITY_TYPE_PATTERN+"/"+ENTITY_ID_PATTERN+"/labels"+REQUEST_LABEL_PATTERN, api.entityDeleteLabelHandler, "api-entity-delete-label")
	api.Router.AddRoute(API_ROOT+"entities/"+REQUEST_ENTITY_TYPE_PATTERN, api.entitiesHandler, "api-entities")
	api.Router.AddRoute(API_ROOT+"query", api.queryHandler, "query-entity")
	api.Router.AddRoute(API_ROOT+"stats", api.statsHandler, "api-stats")
	api.Router.AddRoute(API_ROOT+"changes", api.changesHandler, "api-changes")

	return api
}

// ============================== CONTROLLERS ============================== //

// {POST,GET,PUT,DELETE} /entity/{_id}
// Managing a single entity
func (api *API) entityHandler(ctx router.HTTPContext) {
	// Handle the request.
	switch ctx.Request.Method {
	case "POST":
		postEntityHandler(ctx, api.em)
	case "PUT":
		putEntityHandler(ctx, api.em)
	case "DELETE":
		deleteEntityHandler(ctx, api.em)
	case "GET":
		getEntityHandler(ctx, api.em)
	default:
		ctx.UnsupportedAPIMethod()
	}
}

// GET /entity/{_id}/labels
// Getting all labels for a single entity
func (api *API) entityLabelsHandler(ctx router.HTTPContext) {
	switch ctx.Request.Method {
	case "GET":
		// TODO: fill in
	default:
		ctx.UnsupportedAPIMethod()
	}
}

// DELETE /entity/{_id}/labels/{label}
// Delete a label from a single entity
func (api *API) entityDeleteLabelHandler(ctx router.HTTPContext) {
	switch ctx.Request.Method {
	case "DELETE":
		// TODO: fill in
	default:
		ctx.UnsupportedAPIMethod()
	}
}

// {POST,GET,PUT,DELETE} /entity/{_id}/labels/{label}
// Manage one or more entities
func (api *API) entitiesHandler(ctx router.HTTPContext) {
	// Handle the request.
	switch ctx.Request.Method {
	case "POST":
		postEntitiesHandler(ctx, api.em)
	case "PUT":
		putEntitiesHandler(ctx, api.em)
	case "DELETE":
		deleteEntitiesHandler(ctx, api.em)
	case "GET":
		getEntitiesHandler(ctx, api.em)
	default:
		ctx.UnsupportedAPIMethod()
	}
}

// POST /query
// Handles an edge case of having a query >2k characters
func (api *API) queryHandler(ctx router.HTTPContext) {
	switch ctx.Request.Method {
	case "POST":
		// TODO: fill in
	default:
		ctx.UnsupportedAPIMethod()
	}
}

// {GET,DELETE} /stats
// Manage stats
func (api *API) statsHandler(ctx router.HTTPContext) {
	switch ctx.Request.Method {
	case "GET":
		// TODO: fill in
	case "DELETE":
		// TODO: fill in
	default:
		ctx.UnsupportedAPIMethod()
	}
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

// GET /changes
// Consume change feed
func (api *API) changesHandler(ctx router.HTTPContext) {
	switch ctx.Request.Method {
	case "GET":
		// Upgrade to a WebSocket connection.
		wsConn, err := upgrader.Upgrade(ctx.Response, ctx.Request, nil)
		if err != nil {
			ctx.APIError(router.ErrInternal, err.Error())
			return
		}

		f := api.ff.Make(wsConn) // make a feed
		f.Start()                // start the feed
		f.Wait()                 // wait for the feed to finish
	default:
		ctx.UnsupportedAPIMethod()
	}
}

// ============================== HELPER FUNCTIONS ============================== //

func postEntityHandler(ctx router.HTTPContext, em entity.Manager) {
	if len(ctx.Arguments) != 2 {
		ctx.APIError(router.ErrMissingParam, "Missing param: request entity type")
		return
	}

	requestEntityType := ctx.Arguments[1]

	// Decode request body into entity var
	var e etre.Entity
	err := json.NewDecoder(ctx.Request.Body).Decode(&e)
	if err != nil {
		ctx.APIError(router.ErrInternal, "Can't decode request body (error: %s)", err)
		return
	}

	ConvertFloat64ToInt(e)

	for k, v := range e {
		if !validValueType(v) {
			ctx.APIError(router.ErrBadRequest, "Key (%v) has value (%v) with invalid type (%v). Type of value must be a string or int.", k, v, reflect.TypeOf(v))
			return
		}
	}

	ids, err := em.CreateEntities(requestEntityType, []etre.Entity{e}, ctx.Username())
	if err != nil {
		if _, ok := err.(entity.ErrCreate); ok {
			ctx.APIError(router.ErrInternal, "Error creating entity: %s", err)
			return
		} else {
			ctx.APIError(router.ErrInternal, "Uknown error when creating entity: %s", err)
			return
		}
	}

	out, err := json.Marshal(ids[0])
	if err != nil {
		ctx.APIError(router.ErrInternal, "Can't encode response (error: %s)", err)
		return
	}

	fmt.Fprintln(ctx.Response, string(out))
}

func getEntityHandler(ctx router.HTTPContext, em entity.Manager) {
	if len(ctx.Arguments) != 2 {
		ctx.APIError(router.ErrMissingParam, "Missing params")
		return
	}

	args := splitArgs(ctx.Arguments[1], "/")
	if len(args) != 2 {
		ctx.APIError(router.ErrMissingParam, "Missing params")
		return
	}
	for _, a := range args {
		if a == "" {
			ctx.APIError(router.ErrMissingParam, "Missing params")
			return
		}
	}

	requestEntityType := args[0]
	entityId := args[1]

	q := queryForId(entityId)

	entities, err := em.ReadEntities(requestEntityType, q)
	if err != nil {
		if _, ok := err.(entity.ErrRead); ok {
			ctx.APIError(router.ErrInternal, "Error reading entity: %s", err)
		} else {
			ctx.APIError(router.ErrInternal, "Uknown error when reading entity: %s", err)
		}

		return
	}

	if entities == nil {
		ctx.APIError(router.ErrNotFound, "No entity with id: %s", entityId)
		return
	}

	out, err := json.Marshal(entities[0])
	if err != nil {
		ctx.APIError(router.ErrInternal, "Can't encode response (error: %s)", err)
		return
	}

	fmt.Fprintln(ctx.Response, string(out))
}

func putEntityHandler(ctx router.HTTPContext, em entity.Manager) {
	if len(ctx.Arguments) != 2 {
		ctx.APIError(router.ErrMissingParam, "Missing params")
		return
	}

	args := splitArgs(ctx.Arguments[1], "/")
	if len(args) != 2 {
		ctx.APIError(router.ErrMissingParam, "Missing params")
		return
	}
	for _, a := range args {
		if a == "" {
			ctx.APIError(router.ErrMissingParam, "Missing params")
			return
		}
	}

	requestEntityType := args[0]
	entityId := args[1]

	var requestUpdate etre.Entity
	err := json.NewDecoder(ctx.Request.Body).Decode(&requestUpdate)
	if err != nil {
		ctx.APIError(router.ErrInternal, "Can't decode request body (error: %s)", err)
		return
	}

	q := queryForId(entityId)

	entities, err := em.UpdateEntities(requestEntityType, q, requestUpdate, ctx.Username())
	if err != nil {
		if _, ok := err.(entity.ErrUpdate); ok {
			ctx.APIError(router.ErrInternal, "Error updating entity: %s", err)
		} else {
			ctx.APIError(router.ErrInternal, "Uknown error when updating entity: %s", err)
		}

		return
	}

	out, err := json.Marshal(entities[0])
	if err != nil {
		ctx.APIError(router.ErrInternal, "Can't encode response (error: %s)", err)
		return
	}

	fmt.Fprintln(ctx.Response, string(out))
}

func deleteEntityHandler(ctx router.HTTPContext, em entity.Manager) {
	if len(ctx.Arguments) != 2 {
		ctx.APIError(router.ErrMissingParam, "Missing params")
		return
	}

	args := splitArgs(ctx.Arguments[1], "/")
	if len(args) != 2 {
		ctx.APIError(router.ErrMissingParam, "Missing params")
		return
	}
	for _, a := range args {
		if a == "" {
			ctx.APIError(router.ErrMissingParam, "Missing params")
			return
		}
	}
	requestEntityType := args[0]
	entityId := args[1]

	q := queryForId(entityId)

	entities, err := em.DeleteEntities(requestEntityType, q, ctx.Username())
	if err != nil {
		if _, ok := err.(entity.ErrDelete); ok {
			ctx.APIError(router.ErrInternal, "Error deleting entity: %s", err)
		} else {
			ctx.APIError(router.ErrInternal, "Uknown error when deleting entity: %s", err)
		}

		return
	}

	out, err := json.Marshal(entities[0])
	if err != nil {
		ctx.APIError(router.ErrInternal, "Can't encode response (error: %s)", err)
		return
	}

	fmt.Fprintln(ctx.Response, string(out))
}

func postEntitiesHandler(ctx router.HTTPContext, em entity.Manager) {
	if len(ctx.Arguments) != 2 {
		ctx.APIError(router.ErrMissingParam, "Missing param")
		return
	}

	args := splitArgs(ctx.Arguments[1], "/")
	if args[0] == "" {
		ctx.APIError(router.ErrMissingParam, "Missing param")
		return
	}

	requestEntityType := args[0]

	var entities []etre.Entity
	err := json.NewDecoder(ctx.Request.Body).Decode(&entities)
	if err != nil {
		ctx.APIError(router.ErrInternal, "Can't decode request body (error: %s)", err)
		return
	}

	for _, e := range entities {
		ConvertFloat64ToInt(e)

		for k, v := range e {
			if !validValueType(v) {
				ctx.APIError(router.ErrBadRequest, "Key (%v) has value (%v) with invalid type (%v). Type of value must be a string or int.", k, v, reflect.TypeOf(v))
			}
		}
	}

	ids, err := em.CreateEntities(requestEntityType, entities, ctx.Username())
	if err != nil {
		if _, ok := err.(entity.ErrCreate); ok {
			ctx.APIError(router.ErrInternal, "Error creating entities: %s", err)
		} else {
			ctx.APIError(router.ErrInternal, "Uknown error when creating entities: %s", err)
		}

		return
	}

	out, err := json.Marshal(ids)
	if err != nil {
		ctx.APIError(router.ErrInternal, "Can't encode response (error: %s)", err)
		return
	}

	fmt.Fprintln(ctx.Response, string(out))
}

func getEntitiesHandler(ctx router.HTTPContext, em entity.Manager) {
	if len(ctx.Arguments) != 2 {
		ctx.APIError(router.ErrMissingParam, "Missing param: id")
		return
	}

	requestEntityType := ctx.Arguments[1]

	// Translate URL query to query struct
	queryParam := ctx.Request.Form["query"]
	if queryParam == nil {
		ctx.APIError(router.ErrMissingParam, "Missing param: query")
		return
	}

	requestLabelSelector := queryParam[0]
	if requestLabelSelector == "" {
		ctx.APIError(router.ErrMissingParam, "Missing param: query string is empty")
		return
	}

	q, err := query.Translate(requestLabelSelector)
	if err != nil {
		ctx.APIError(router.ErrInternal, "Invalid query:  %s", err)
		return
	}

	entities, err := em.ReadEntities(requestEntityType, q)
	if err != nil {
		if _, ok := err.(entity.ErrRead); ok {
			ctx.APIError(router.ErrInternal, "Error reading entities: %s", err)
		} else {
			ctx.APIError(router.ErrInternal, "Uknown error when reading entities: %s", err)
		}

		return
	}

	if entities == nil {
		ctx.APIError(router.ErrNotFound, "No entities match query: %s", requestLabelSelector)
		return
	}

	// If no error, this endpoint always returns 200 OK and a list, even an empty list.
	out, err := json.Marshal(entities)
	if err != nil {
		ctx.APIError(router.ErrInternal, "Can't encode response (error: %s)", err)
		return
	}

	fmt.Fprintln(ctx.Response, string(out))
}

func putEntitiesHandler(ctx router.HTTPContext, em entity.Manager) {
	if len(ctx.Arguments) != 2 {
		ctx.APIError(router.ErrMissingParam, "Missing param: id")
		return
	}

	requestEntityType := ctx.Arguments[1]

	// Translate URL query to query struct
	queryParam := ctx.Request.Form["query"]
	if queryParam == nil {
		ctx.APIError(router.ErrMissingParam, "Missing param: query")
		return
	}

	requestLabelSelector := queryParam[0]
	if requestLabelSelector == "" {
		ctx.APIError(router.ErrMissingParam, "Missing param: query string is empty")
		return
	}

	q, err := query.Translate(requestLabelSelector)
	if err != nil {
		ctx.APIError(router.ErrInternal, "Invalid query: %s", err)
		return
	}

	// Decode request update
	var requestUpdate etre.Entity
	err = json.NewDecoder(ctx.Request.Body).Decode(&requestUpdate)
	if err != nil {
		ctx.APIError(router.ErrInternal, "Can't decode request body (error: %s)", err)
		return
	}

	entities, err := em.UpdateEntities(requestEntityType, q, requestUpdate, ctx.Username())
	if err != nil {
		if _, ok := err.(entity.ErrUpdate); ok {
			ctx.APIError(router.ErrInternal, "Error updating entities: %s", err)
		} else {
			ctx.APIError(router.ErrInternal, "Uknown error when updating entities: %s", err)
		}
		return
	}

	out, err := json.Marshal(entities)
	if err != nil {
		ctx.APIError(router.ErrInternal, "Can't encode response (error: %s)", err)
		return
	}

	fmt.Fprintln(ctx.Response, string(out))
}

func deleteEntitiesHandler(ctx router.HTTPContext, em entity.Manager) {
	if len(ctx.Arguments) != 2 {
		ctx.APIError(router.ErrMissingParam, "Missing param: id")
		return
	}

	requestEntityType := ctx.Arguments[1]

	// Translate URL query to query struct
	queryParam := ctx.Request.Form["query"]
	if queryParam == nil {
		ctx.APIError(router.ErrMissingParam, "Missing param: query")
		return
	}

	requestLabelSelector := queryParam[0]
	if requestLabelSelector == "" {
		ctx.APIError(router.ErrMissingParam, "Missing param: query string is empty")
		return
	}

	q, err := query.Translate(requestLabelSelector)
	if err != nil {
		ctx.APIError(router.ErrInternal, "Invalid query: %s", err)
		return
	}

	entities, err := em.DeleteEntities(requestEntityType, q, ctx.Username())
	if err != nil {
		if _, ok := err.(entity.ErrDelete); ok {
			ctx.APIError(router.ErrInternal, "Error deleting entities: %s", err)
		} else {
			ctx.APIError(router.ErrInternal, "Uknown error when deleting entities: %s", err)
		}

		return
	}

	out, err := json.Marshal(entities)
	if err != nil {
		ctx.APIError(router.ErrInternal, "Can't encode response (error: %s)", err)
		return
	}

	fmt.Fprintln(ctx.Response, string(out))
}

// _id is not a valid field name to pass to query.Translate, so we manually
// create a query object.
func queryForId(id string) query.Query {
	return query.Query{
		[]query.Predicate{
			query.Predicate{
				Label:    "_id",
				Operator: "=",
				Value:    id,
			},
		},
	}
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

// Values in entity must be of type string or int. This is because the query
// language we use only supports querying by string or int. See more at:
// github.com/square/etre/query
func validValueType(v interface{}) bool {
	return reflect.TypeOf(v).Kind() == reflect.String || reflect.TypeOf(v).Kind() == reflect.Int
}

func splitArgs(args string, sep string) []string {
	return strings.Split(args, sep)
}
