// Copyright 2017, Square, Inc.

package api

import (
	"net/http"

	"github.com/square/etre"
)

// These are default API-level error responses that should not be modified.
// See api.go for how they're used. The API writes them back to clients. It
// uses the fields to create a custom etre.Error and set the HTTP status code.

var ErrDuplicateEntity = etre.Error{
	Type:       "dupe-entity",
	HTTPStatus: http.StatusConflict,
	Message:    "duplicate entity",
}

var ErrNotFound = etre.Error{
	Type:       "entity-not-found",
	HTTPStatus: http.StatusNotFound,
	Message:    "entity not found",
}

var ErrMissingParam = etre.Error{
	Type:       "missing-param",
	HTTPStatus: http.StatusBadRequest,
	Message:    "missing parameter",
}

var ErrInvalidParam = etre.Error{
	Type:       "invalid-param",
	HTTPStatus: http.StatusBadRequest,
	Message:    "missing parameter",
}

var ErrInvalidQuery = etre.Error{
	Type:       "invalid-query",
	HTTPStatus: http.StatusBadRequest,
	Message:    "invalid query",
}

var ErrDb = etre.Error{
	Type:       "db-error",
	HTTPStatus: http.StatusInternalServerError,
	Message:    "internal server error",
}

var ErrInternal = etre.Error{
	Type:       "internal-error",
	HTTPStatus: http.StatusInternalServerError,
	Message:    "internal server error",
}

var ErrBadRequest = etre.Error{
	Type:       "bad-request",
	HTTPStatus: http.StatusBadRequest,
	Message:    "bad request",
}

var ErrCDCDisabled = etre.Error{
	Type:       "cdc-disabled",
	HTTPStatus: http.StatusNotImplemented,
	Message:    "CDC disabled",
}
