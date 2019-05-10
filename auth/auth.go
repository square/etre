// Copyright 2018-2019, Square, Inc.

// Package auth provides team-based authentication and authorization.
package auth

import (
	"net/http"
)

const (
	DEFAULT_CALLER_NAME  = "etre"
	DEFAULT_METRIC_GROUP = "etre"
)

type ACL struct {
	// User-defined role. This must exactly match a Caller role for the ACL
	// to match.
	Role string

	// Role grants admin access to request. The Authorize plugin method is not
	// called. Authorization is always successful.
	Admin bool

	// Read entity types granted to the role. Does not apply to admin roles.
	Read []string

	// Write entity types granted to the role. Does not apply to admin roles.
	Write []string

	// Trace keys required to be set. Applies to admin roles.
	TraceKeysRequired []string
}

// Caller represents a client making a request. The Authentication method of the
// auth plugin determines the caller.
type Caller struct {
	Name         string            // name of the caller: username or app name
	Roles        []string          // caller roles to match against ACL roles
	MetricGroups []string          // metric groups to add metric values to
	Trace        map[string]string // key-value pairs to report in trace metrics
}

// Action is what a Caller is trying to do. The Authorize method of the auth plugin
// authorizes the action if, first, the caller has a role matching an ACL.
type Action struct {
	EntityType string
	Op         string
}

const (
	OP_READ  = "r"
	OP_WRITE = "w"
)

// Plugin is the auth plugin. Implement this interface to enable custom auth.
type Plugin interface {
	// Authenticate determines the Caller from the HTTP request. To allow, return
	// a non-zero Caller and nil error. To deny, return an error and Etre will
	// return HTTP status 401 (Unauthorized).
	Authenticate(*http.Request) (Caller, error)

	// Authorize authorizes the caller to do the action. To allow, return nil.
	// To deny, return an error and Etre will return HTTP status 403 (Forbidden).
	Authorize(Caller, Action) error
}

// AllowAll is the default Plugin which allows all callers and requests (no auth).
type AllowAll struct{}

func NewAllowAll() AllowAll {
	return AllowAll{}
}

func (a AllowAll) Authenticate(*http.Request) (Caller, error) {
	// Return a new caller each time because the auth manager might set
	// Trace if the client passed trace values via X-Etre-Trace header
	caller := Caller{
		Name:         DEFAULT_CALLER_NAME,
		MetricGroups: []string{DEFAULT_METRIC_GROUP},
	}
	return caller, nil
}

func (a AllowAll) Authorize(Caller, Action) error {
	return nil
}
