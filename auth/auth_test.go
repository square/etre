// Copyright 2018-2019, Square, Inc.

package auth_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/go-test/deep"
	"github.com/square/etre"
	"github.com/square/etre/auth"
	"github.com/square/etre/test/mock"
)

func TestAllowAll(t *testing.T) {
	allowAll := auth.NewAllowAll()
	caller, err := allowAll.Authenticate(&http.Request{})
	if err != nil {
		t.Error(err)
	}
	expectCaller := auth.Caller{
		Name:         "etre",
		MetricGroups: []string{"etre"},
	}
	if diffs := deep.Equal(caller, expectCaller); diffs != nil {
		t.Error(diffs)
	}
	action := auth.Action{
		EntityType: "foo",
		Op:         auth.OP_READ,
	}
	err = allowAll.Authorize(caller, action)
	if err != nil {
		t.Error(err)
	}
	action.Op = auth.OP_WRITE
	err = allowAll.Authorize(caller, action)
	if err != nil {
		t.Error(err)
	}
}

func TestManager(t *testing.T) {
	acls := []auth.ACL{
		{
			Role:  "finch",
			Admin: true,
		},
		{
			Role:  "bar",
			Read:  []string{"bar", "foo"},
			Write: []string{"bar"},
		},
		{
			Role:              "foo",
			Read:              []string{"foo"},
			TraceKeysRequired: []string{"app"},
		},
	}
	var caller auth.Caller
	var authErr, authorErr error
	plugin := mock.AuthPlugin{
		AuthenticateFunc: func(req *http.Request) (auth.Caller, error) {
			return caller, authErr
		},
		AuthorizeFunc: func(caller auth.Caller, action auth.Action) error {
			return authorErr
		},
	}
	man := auth.NewManager(acls, plugin)

	// If plugin.Authenticate returns nil error, and caller has no required trace keys,
	// then manager just returns caller from plugin.
	gotCaller, err := man.Authenticate(&http.Request{})
	if err != authErr {
		t.Errorf("got Authenticate error '%v', expected '%v'", err, authErr)
	}
	if diffs := deep.Equal(gotCaller, caller); diffs != nil {
		t.Error(diffs)
	}

	// Trace key requirements
	// ---------------------------------------------------------------------------

	// Make caller match role foo which has required trace keys, but don't
	// define any, and manager should return an error. The other roles x and y
	// test that they're ignored because there's no ACL for x or y, i.e. callers
	// can have roles for which there are no ACLs.
	caller.Roles = []string{"foo", "x", "y"}
	_, err = man.Authenticate(&http.Request{})
	if err == nil {
		t.Errorf("no Authenticate error for missing trace keys, expected an error")
	}

	// Add a trace key but not the one we need: app
	caller.Trace = map[string]string{"user": "finch"}
	_, err = man.Authenticate(&http.Request{})
	if err == nil {
		t.Errorf("no Authenticate error for missing trace keys, expected an error")
	}

	// Add the needed key and it should work again
	caller.Trace["app"] = "etre"
	gotCaller, err = man.Authenticate(&http.Request{})
	if err != nil {
		t.Error(err)
	}
	if diffs := deep.Equal(gotCaller, caller); diffs != nil {
		t.Error(diffs)
	}

	// Read/write authorization
	// ---------------------------------------------------------------------------

	// bar can read foo and bar, and write bar entities
	caller.Roles = []string{"bar"}
	err = man.Authorize(caller, auth.Action{EntityType: "foo", Op: auth.OP_READ})
	if err != nil {
		t.Error(err)
	}
	err = man.Authorize(caller, auth.Action{EntityType: "bar", Op: auth.OP_READ})
	if err != nil {
		t.Error(err)
	}
	err = man.Authorize(caller, auth.Action{EntityType: "bar", Op: auth.OP_WRITE})
	if err != nil {
		t.Error(err)
	}

	// bar can't read foo but it cannot write it
	err = man.Authorize(caller, auth.Action{EntityType: "foo", Op: auth.OP_WRITE})
	if err == nil {
		t.Error("no Authorize error, expected one")
	}
	// bar can't read or write other entity types
	err = man.Authorize(caller, auth.Action{EntityType: "not-this-type", Op: auth.OP_READ})
	if err == nil {
		t.Error("no Authorize error, expected one")
	}
	err = man.Authorize(caller, auth.Action{EntityType: "not-this-type", Op: auth.OP_WRITE})
	if err == nil {
		t.Error("no Authorize error, expected one")
	}

	// Admin role finch can read/write anything
	caller.Roles = []string{"finch"}
	err = man.Authorize(caller, auth.Action{EntityType: "foo", Op: auth.OP_READ})
	if err != nil {
		t.Error(err)
	}
	err = man.Authorize(caller, auth.Action{EntityType: "foo", Op: auth.OP_WRITE})
	if err != nil {
		t.Error(err)
	}
	err = man.Authorize(caller, auth.Action{EntityType: "bar", Op: auth.OP_READ})
	if err != nil {
		t.Error(err)
	}
	err = man.Authorize(caller, auth.Action{EntityType: "bar", Op: auth.OP_WRITE})
	if err != nil {
		t.Error(err)
	}
	err = man.Authorize(caller, auth.Action{EntityType: "any-entity-type", Op: auth.OP_READ})
	if err != nil {
		t.Error(err)
	}
	err = man.Authorize(caller, auth.Action{EntityType: "any-entity-type", Op: auth.OP_WRITE})
	if err != nil {
		t.Error(err)
	}
}

func TestManagerNoACLs(t *testing.T) {
	// Without ACLs, auth is effectively disabled. Authenticate still calls
	// the plugin so that metric groups work, but it doesn't check required
	// trace keys and Authorize just calls the plugin Authorize.
	var caller auth.Caller
	var authorizeCalled bool
	plugin := mock.AuthPlugin{
		AuthenticateFunc: func(req *http.Request) (auth.Caller, error) {
			return caller, nil
		},
		AuthorizeFunc: func(caller auth.Caller, action auth.Action) error {
			authorizeCalled = true
			return nil
		},
	}
	man := auth.NewManager([]auth.ACL{}, plugin)

	gotCaller, err := man.Authenticate(&http.Request{})
	if err != nil {
		t.Errorf("got Authenticate error '%v', expected nil", err)
	}
	if diffs := deep.Equal(gotCaller, caller); diffs != nil {
		t.Error(diffs)
	}

	err = man.Authorize(caller, auth.Action{EntityType: "foo", Op: auth.OP_WRITE})
	if err != nil {
		t.Error(err)
	}
	if !authorizeCalled {
		t.Errorf("auth plugin Authorize called, expected it to be called without ACLs")
	}
}

func TestManagerAuthenticateError(t *testing.T) {
	// If plugin Authenticate returns error, manager should return it immediately
	// and check nothing else, which we test by missing required trace keys
	acls := []auth.ACL{
		{
			Role:              "foo",
			Admin:             true,
			TraceKeysRequired: []string{"app"},
		},
	}
	var caller auth.Caller
	var authErr error
	plugin := mock.AuthPlugin{
		AuthenticateFunc: func(req *http.Request) (auth.Caller, error) {
			return caller, authErr
		},
	}
	man := auth.NewManager(acls, plugin)

	caller.Roles = []string{"foo"}
	authErr = fmt.Errorf("forced test error")

	gotCaller, err := man.Authenticate(&http.Request{})
	if err != authErr {
		t.Errorf("got Authenticate error '%v', expected '%v'", err, authErr)
	}
	if diffs := deep.Equal(gotCaller, caller); diffs != nil {
		t.Error(diffs)
	}
}

func TestTraceHeader(t *testing.T) {
	plugin := auth.NewAllowAll()
	man := auth.NewManager(nil, plugin)

	// Good values are put in Trace automatically
	req, _ := http.NewRequest("GET", "http://example.com", nil)
	req.Header.Set(etre.TRACE_HEADER, "app=foo,host=bar")
	expectCaller := auth.Caller{
		Name:         "etre",
		MetricGroups: []string{"etre"},
		Trace: map[string]string{
			"app":  "foo",
			"host": "bar",
		},
	}
	gotCaller, err := man.Authenticate(req)
	if err != nil {
		t.Errorf("got Authenticate error '%v', expected nil", err)
	}
	if diffs := deep.Equal(gotCaller, expectCaller); diffs != nil {
		t.Error(diffs)
	}

	// Bad values are silently ignored
	req, _ = http.NewRequest("GET", "http://example.com", nil)
	req.Header.Set(etre.TRACE_HEADER, "app=foo,host") // "host" should be "host=val"
	expectCaller.Trace = map[string]string{
		"app": "foo",
	}
	gotCaller, err = man.Authenticate(req)
	if err != nil {
		t.Errorf("got Authenticate error '%v', expected nil", err)
	}
	if diffs := deep.Equal(gotCaller, expectCaller); diffs != nil {
		t.Error(diffs)
	}

	req, _ = http.NewRequest("GET", "http://example.com", nil)
	req.Header.Set(etre.TRACE_HEADER, "") // empty value
	expectCaller.Trace = nil
	gotCaller, err = man.Authenticate(req)
	if err != nil {
		t.Errorf("got Authenticate error '%v', expected nil", err)
	}
	if diffs := deep.Equal(gotCaller, expectCaller); diffs != nil {
		t.Error(diffs)
	}

	// Values set by plugin are not changed
	caller := auth.Caller{
		Name:  "foo",
		Trace: map[string]string{"app": "do-not-change"},
	}
	p2 := mock.AuthPlugin{
		AuthenticateFunc: func(req *http.Request) (auth.Caller, error) {
			return caller, nil
		},
	}
	man = auth.NewManager(nil, p2)
	req, _ = http.NewRequest("GET", "http://example.com", nil)
	req.Header.Set(etre.TRACE_HEADER, "app=foo,host=bar")
	expectCaller = auth.Caller{
		Name: "foo",
		Trace: map[string]string{
			"app":  "do-not-change", // from caller
			"host": "bar",           // from header
		},
	}
	gotCaller, err = man.Authenticate(req)
	if err != nil {
		t.Errorf("got Authenticate error '%v', expected nil", err)
	}
	if diffs := deep.Equal(gotCaller, expectCaller); diffs != nil {
		t.Error(diffs)
	}
}
