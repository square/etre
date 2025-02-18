// Copyright 2017-2018, Square, Inc.

package entity_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/square/etre"
	"github.com/square/etre/entity"
)

var validate = entity.NewValidator(entityTypes)

func TestValidateCreateEntitiesOK(t *testing.T) {
	// All ok
	entities := []etre.Entity{
		{"x": 0},
		{"y": 1},
	}
	err := validate.Entities(entities, entity.VALIDATE_ON_CREATE)
	require.NoError(t, err)
}

func TestValidateCreateEntitiesErrorsMetalabels(t *testing.T) {
	invalid := []etre.Entity{
		{"a": "b", "_id": "59f10d2a5669fc79103a1111"}, // _id not allowed
		{"a": "b", "_type": "node"},                   // _type not allowed
		{"a": "b", "_rev": int64(0)},                  // _rev not allowed
	}

	for _, e := range invalid {
		err := validate.Entities([]etre.Entity{e}, entity.VALIDATE_ON_CREATE)
		assert.Errorf(t, err, "no error creating entity, expected one: %+v", e)
		assertValidationError(t, err, "cannot-set-metalabel")
	}

	for _, e := range invalid {
		err := validate.Entities([]etre.Entity{e}, entity.VALIDATE_ON_UPDATE)
		require.Error(t, err, "no error creating entity, expected one: %+v", e)
		assertValidationError(t, err, "cannot-change-metalabel")
	}
}

func TestValidateCreateEntitiesErrorsWhitespace(t *testing.T) {
	invalid := []etre.Entity{
		{" ": "b"},   // label can't be space
		{" a": "b"},  // label can't have space
		{" a ": "b"}, // label can't have space
		{"a ": "b"},  // label can't have space
	}

	for _, e := range invalid {
		err := validate.Entities([]etre.Entity{e}, entity.VALIDATE_ON_CREATE)
		require.Error(t, err, "no error creating entity, expected one: %+v", e)
		assertValidationError(t, err, "label-has-whitespace")
	}

	for _, e := range invalid {
		err := validate.Entities([]etre.Entity{e}, entity.VALIDATE_ON_UPDATE)
		require.Error(t, err, "no error creating entity, expected one: %+v", e)
		assertValidationError(t, err, "label-has-whitespace")
	}

	invalid = []etre.Entity{
		{"": "b"}, // label can't be empty string
	}

	for _, e := range invalid {
		err := validate.Entities([]etre.Entity{e}, entity.VALIDATE_ON_CREATE)
		require.Error(t, err, "no error creating entity, expected one: %+v", e)
		assertValidationError(t, err, "empty-string-label")
	}

	for _, e := range invalid {
		err := validate.Entities([]etre.Entity{e}, entity.VALIDATE_ON_UPDATE)
		require.Error(t, err, "no error creating entity, expected one: %+v", e)
		assertValidationError(t, err, "empty-string-label")
	}
}

func TestValidateWriteOpOK(t *testing.T) {
	wo := entity.WriteOp{
		EntityType: "grue",
		Caller:     "dn",
	}
	err := validate.WriteOp(wo)
	require.Error(t, err)
	assertValidationError(t, err, "invalid-entity-type")
}

func TestValidateDeleteLabel(t *testing.T) {
	err := validate.DeleteLabel("foo")
	require.NoError(t, err)
	err = validate.DeleteLabel("_id")
	require.Error(t, err)
}

// assertValidationError asserts the error to be a non-nil ValidationError and asserts the expected type.
func assertValidationError(t *testing.T, err error, expectedType string) {
	// Ugly asserts and returns instead of require so that the test can continue
	t.Helper()
	assert.Error(t, err)
	if err == nil {
		return
	}
	ve, ok := err.(entity.ValidationError)
	assert.True(t, ok)
	if !ok {
		return
	}
	assert.Equal(t, expectedType, ve.Type)
}
