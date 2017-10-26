// Copyright 2017, Square, Inc.

package api

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strings"

	"github.com/square/etre"
)

// Route represents a single endpoint matched by regex.
type Route struct {
	Name    string            // API endpoint name.
	Pattern *regexp.Regexp    // URL Path to match against.
	Handler func(HTTPContext) // Handler function.
}

// Router is a collection of routes.
type Router struct {
	UsernameHeader string  // the http header that the requestor's username is set in
	Routes         []Route // list of routes supported by the application.
}

// AddRoute adds an HTTP handler to the router. Any parameter {} is replaced to become
// a slash-component of the URL.
func (router *Router) AddRoute(pattern string, handler func(HTTPContext), name string) {
	processed := strings.Replace(pattern, "{}", "([^/]*)", -1)
	compiled := regexp.MustCompile("\\A" + processed + "/?\\z")
	router.Routes = append(router.Routes, Route{
		Name:    name,
		Pattern: compiled,
		Handler: handler,
	})
}

func (router *Router) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	if handler, _ := router.Handler(req); handler != nil {
		handler.ServeHTTP(rw, req)
		return
	}
	http.NotFound(rw, req)
}

// Handler returns the HTTP handler and associated pattern for the given request.
func (router *Router) Handler(req *http.Request) (h http.Handler, pattern string) {
	for _, route := range router.Routes {
		match := route.Pattern.FindStringSubmatch(req.URL.Path)
		if len(match) != 0 {
			return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				ctx := HTTPContext{
					Response:       rw,
					Request:        req,
					Arguments:      match,
					router:         router,
					usernameHeader: router.UsernameHeader,
				}
				ctx.Request.ParseForm()
				route.Handler(ctx)
			}), route.Name
		}
	}

	return nil, ""
}

// HTTPContext is an object that is passed around during the handling of the request.
type HTTPContext struct {
	Response       http.ResponseWriter // HTTP Response object.
	Request        *http.Request       // HTTP Request object.
	Arguments      []string            // Arguments matched by the wildcard portions ({}) in the URL pattern.
	router         *Router
	EntityType     string
	EntityId       string
	usernameHeader string // the http header that the requestor's username is set in

}

func (ctx HTTPContext) Username() string {
	return ctx.Request.Header.Get(ctx.usernameHeader)
}

func (ctx HTTPContext) WriteOK(v interface{}) error {
	return ctx.write(v, http.StatusOK)
}

func (ctx HTTPContext) WriteCreated(v interface{}) error {
	return ctx.write(v, http.StatusCreated)
}

func (ctx HTTPContext) WriteNotFound() {
	ctx.write(nil, http.StatusNotFound)
}

// APIError writes a custom error message in the JSON format.
func (ctx HTTPContext) APIError(e etre.Error) {
	ctx.write(e, e.HTTPStatus)
}

// UnsupportedAPIMethod writes an error message regarding unsupported HTTP method.
func (ctx HTTPContext) UnsupportedAPIMethod() {
	ctx.APIError(ErrBadRequest.New("unsupported method on endpoint: %s", ctx.Request.Method))
}

func (ctx HTTPContext) write(v interface{}, status int) error {
	ctx.Response.WriteHeader(status)
	if v == nil {
		return nil
	}
	bytes, err := json.Marshal(v)
	if err != nil {
		// @todo: handle error better (althought Marshal should never fail here)
		http.Error(ctx.Response, "Error while encoding JSON.", http.StatusInternalServerError)
		return err
	}
	ctx.Response.Header().Set("Content-Type", "application/json")
	_, err = ctx.Response.Write(bytes)
	if err != nil {
		http.Error(ctx.Response, "Error whie writing the error", http.StatusInternalServerError)
		return err
	}
	return nil
}
