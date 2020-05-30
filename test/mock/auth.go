// Copyright 2019-2020, Square, Inc.

package mock

import (
	"net/http"

	"github.com/square/etre/auth"
)

var _ auth.Plugin = AuthPlugin{}

type AuthPlugin struct {
	AuthenticateFunc func(*http.Request) (auth.Caller, error)
	AuthorizeFunc    func(auth.Caller, auth.Action) error
}

func (a AuthPlugin) Authenticate(req *http.Request) (auth.Caller, error) {
	if a.AuthenticateFunc != nil {
		return a.AuthenticateFunc(req)
	}
	return auth.Caller{Name: "test", MetricGroups: []string{"test"}}, nil
}

func (a AuthPlugin) Authorize(c auth.Caller, ac auth.Action) error {
	if a.AuthorizeFunc != nil {
		return a.AuthorizeFunc(c, ac)
	}
	return nil
}
