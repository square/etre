// Copyright 2019-2020, Square, Inc.

package mock

import (
	"net/http"

	"github.com/square/etre/auth"
)

var _ auth.Plugin = &AuthRecorder{}

type AuthRecorder struct {
	AuthenticateFunc func(*http.Request) (auth.Caller, error)
	AuthorizeFunc    func(auth.Caller, auth.Action) error
	AuthenticateArgs []AuthenticateArgs
	AuthorizeArgs    []AuthorizeArgs
}

type AuthenticateArgs struct {
	Req *http.Request
}

type AuthorizeArgs struct {
	Caller auth.Caller
	Action auth.Action
}

func (a *AuthRecorder) Authenticate(req *http.Request) (auth.Caller, error) {
	a.AuthenticateArgs = append(a.AuthenticateArgs, AuthenticateArgs{Req: req})
	if a.AuthenticateFunc != nil {
		return a.AuthenticateFunc(req)
	}
	return auth.Caller{Name: "test", MetricGroups: []string{"test"}}, nil
}

func (a *AuthRecorder) Authorize(c auth.Caller, ac auth.Action) error {
	a.AuthorizeArgs = append(a.AuthorizeArgs, AuthorizeArgs{Caller: c, Action: ac})
	if a.AuthorizeFunc != nil {
		return a.AuthorizeFunc(c, ac)
	}
	return nil
}

func (a *AuthRecorder) Reset() {
	a.AuthenticateArgs = []AuthenticateArgs{}
	a.AuthorizeArgs = []AuthorizeArgs{}
}
