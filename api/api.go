// Copyright 2017, Square, Inc.

package api

import "github.com/square/etre/router"

const (
	API_ROOT              = "/api/v1/"
	REQUEST_ID_PATTERN    = "([0-9]+)"
	REQUEST_LABEL_PATTERN = "([A-Za-z0-9]+)"
)

// API provides controllers for endpoints it registers with a router.
type API struct {
	Router *router.Router
}

// NewAPI makes a new API.
func NewAPI(router *router.Router) *API {
	api := &API{
		Router: router,
	}

	api.Router.AddRoute(API_ROOT+"entity/"+REQUEST_ID_PATTERN, api.entityHandler, "api-entity")
	api.Router.AddRoute(API_ROOT+"entity/"+REQUEST_ID_PATTERN+"/labels", api.entityLabelsHandler, "api-entity-labels")
	api.Router.AddRoute(API_ROOT+"entity/"+REQUEST_ID_PATTERN+"/labels"+REQUEST_LABEL_PATTERN, api.entityDeleteLabelHandler, "api-entity-delete-label")
	api.Router.AddRoute(API_ROOT+"entities", api.entitiesHandler, "api-entities")
	api.Router.AddRoute(API_ROOT+"query", api.queryHandler, "query-entity")
	api.Router.AddRoute(API_ROOT+"stats", api.statsHandler, "api-stats")

	return api
}

// ============================== CONTROLLERS ============================== //

// {POST,GET,PUT,DELETE} /entity/{_id}
// Managing a single entity
func (api *API) entityHandler(ctx router.HTTPContext) {
	switch ctx.Request.Method {
	case "POST":
		// TODO: fill in
	case "GET":
		// TODO: fill in
	case "PUT":
		// TODO: fill in
	case "DELETE":
		// TODO: fill in
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
	switch ctx.Request.Method {
	case "POST":
		// TODO: fill in
	case "GET":
		// TODO: fill in
	case "PUT":
		// TODO: fill in
	case "DELETE":
		// TODO: fill in
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
