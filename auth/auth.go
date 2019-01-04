// Copyright 2019, Square, Inc.

// Package auth provides team-based authentication and authorization.
package auth

import (
	"net/http"
)

type ACL struct {
	// User-defined role. This must exactly match a Caller role for the ACL
	// to match.
	Role string

	// Role grants admin access (all ops) to request. Mutually exclusive with Ops.
	Admin bool

	// Read entity types granted to the role.
	Read []string

	// Write entity types granted to the role.
	Write []string

	// Trace keys required to be set.
	TraceKeysRequired []string
}

type Caller struct {
	Name         string
	Roles        []string
	MetricGroups []string
	Trace        map[string]string
}

type Action struct {
	EntityType string
	Op         string
}

const (
	OP_READ  = "r"
	OP_WRITE = "w"
)

type Plugin interface {
	Authenticate(*http.Request) (Caller, error)
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
		Name:         "etre",
		MetricGroups: []string{"etre"},
	}
	return caller, nil
}

func (a AllowAll) Authorize(Caller, Action) error {
	return nil
}
