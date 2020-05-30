// Copyright 2017-2020, Square, Inc.

package api_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/go-test/deep"

	"github.com/square/etre"
	"github.com/square/etre/auth"
	"github.com/square/etre/test"
	"github.com/square/etre/test/mock"
)

func TestAuthAccessDenied(t *testing.T) {
	// Test that caller gets an HTTP 401 when Authenticate returns an error
	// which indicates the caller's authenticated is rejected/access denined.
	// Test both read and write because the returns are different.
	server := setup(t, defaultConfig, mock.EntityStore{})
	defer server.ts.Close()

	server.auth.AuthenticateFunc = func(req *http.Request) (auth.Caller, error) {
		return auth.Caller{}, fmt.Errorf("test deny")
	}

	// ----------------------------------------------------------------------
	// Read
	etreurl := server.url + etre.API_ROOT + "/entities/" + entityType + "?query=x"
	var etreErr etre.Error
	statusCode, err := test.MakeHTTPRequest("GET", etreurl, nil, &etreErr)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", etreErr)
	if statusCode != http.StatusUnauthorized {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusUnauthorized)
	}

	// ----------------------------------------------------------------------
	// Write
	etreurl = server.url + etre.API_ROOT + "/entity/" + entityType
	newEntity := etre.Entity{"host": "local"}
	payload, err := json.Marshal(newEntity)
	if err != nil {
		t.Fatal(err)
	}
	var gotWR etre.WriteResult
	statusCode, err = test.MakeHTTPRequest("POST", etreurl, payload, &gotWR)
	if err != nil {
		t.Fatal(err)
	}
	if statusCode != http.StatusUnauthorized {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusUnauthorized)
	}
	if gotWR.Error == nil {
		t.Errorf("WriteResult.Error is nil, expected error message")
	} else if gotWR.Error.Type != "access-denied" {
		t.Errorf("WriteResult.Error.Type = %s, expected access-denied", gotWR.Error.Type)
	}
}

func TestAuthNotAuthorizedWNoACLs(t *testing.T) {
	// Test that caller gets an HTTP 403 when Authorize returns an error
	// which indicates the caller isn't authorized to do the query.
	// Test both read and write because the returns are different.
	//
	// For this test, there are no ACLs from the config, so the auth.Manager
	// just calls auth.Plugin.Authorize().
	server := setup(t, defaultConfig, mock.EntityStore{})
	defer server.ts.Close()

	caller := auth.Caller{Name: "dev", Roles: []string{"test"}}

	var gotCaller auth.Caller
	var gotAction auth.Action
	server.auth.AuthenticateFunc = func(req *http.Request) (auth.Caller, error) {
		return caller, nil // allow
	}
	server.auth.AuthorizeFunc = func(caller auth.Caller, action auth.Action) error {
		gotCaller = caller
		gotAction = action
		return fmt.Errorf("test deny")
	}

	// ----------------------------------------------------------------------
	// Read
	etreurl := server.url + etre.API_ROOT + "/entities/" + entityType + "?query=x"
	var etreErr etre.Error
	statusCode, err := test.MakeHTTPRequest("GET", etreurl, nil, &etreErr)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", etreErr)
	if statusCode != http.StatusForbidden {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusForbidden)
	}

	if diff := deep.Equal(gotCaller, caller); diff != nil {
		t.Error(diff)
	}

	expectAction := auth.Action{
		EntityType: entityType,
		Op:         auth.OP_READ,
	}
	if diff := deep.Equal(gotAction, expectAction); diff != nil {
		t.Error(diff)
	}

	// ----------------------------------------------------------------------
	// Write
	gotCaller = auth.Caller{}
	gotAction = auth.Action{}

	etreurl = server.url + etre.API_ROOT + "/entity/" + entityType
	newEntity := etre.Entity{"host": "local"}
	payload, err := json.Marshal(newEntity)
	if err != nil {
		t.Fatal(err)
	}
	var gotWR etre.WriteResult
	statusCode, err = test.MakeHTTPRequest("POST", etreurl, payload, &gotWR)
	if err != nil {
		t.Fatal(err)
	}
	if statusCode != http.StatusForbidden {
		t.Errorf("response status = %d, expected %d", statusCode, http.StatusForbidden)
	}
	if gotWR.Error == nil {
		t.Errorf("WriteResult.Error is nil, expected error message")
	} else if gotWR.Error.Type != "not-authorized" {
		t.Errorf("WriteResult.Error.Type = %s, expected not-authorized", gotWR.Error.Type)
	}

	if diff := deep.Equal(gotCaller, caller); diff != nil {
		t.Error(diff)
	}

	expectAction = auth.Action{
		EntityType: entityType,
		Op:         auth.OP_WRITE,
	}
	if diff := deep.Equal(gotAction, expectAction); diff != nil {
		t.Error(diff)
	}
}
