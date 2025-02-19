// Copyright 2017-2020, Square, Inc.

package api_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, statusCode)

	// ----------------------------------------------------------------------
	// Write
	etreurl = server.url + etre.API_ROOT + "/entity/" + entityType
	newEntity := etre.Entity{"host": "local"}
	payload, err := json.Marshal(newEntity)
	require.NoError(t, err)

	var gotWR etre.WriteResult
	statusCode, err = test.MakeHTTPRequest("POST", etreurl, payload, &gotWR)
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, statusCode)
	require.NotNil(t, gotWR.Error)
	assert.Equal(t, "access-denied", gotWR.Error.Type)
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
	require.NoError(t, err)
	assert.Equal(t, http.StatusForbidden, statusCode)
	assert.Equal(t, caller, gotCaller)
	expectAction := auth.Action{
		EntityType: entityType,
		Op:         auth.OP_READ,
	}
	assert.Equal(t, expectAction, gotAction)

	// ----------------------------------------------------------------------
	// Write
	gotCaller = auth.Caller{}
	gotAction = auth.Action{}

	etreurl = server.url + etre.API_ROOT + "/entity/" + entityType
	newEntity := etre.Entity{"host": "local"}
	payload, err := json.Marshal(newEntity)
	require.NoError(t, err)
	var gotWR etre.WriteResult
	statusCode, err = test.MakeHTTPRequest("POST", etreurl, payload, &gotWR)
	require.NoError(t, err)
	assert.Equal(t, http.StatusForbidden, statusCode)
	require.NotNil(t, gotWR.Error)
	assert.Equal(t, "not-authorized", gotWR.Error.Type)
	assert.Equal(t, caller, gotCaller)

	expectAction = auth.Action{
		EntityType: entityType,
		Op:         auth.OP_WRITE,
	}
	assert.Equal(t, expectAction, gotAction)
}
