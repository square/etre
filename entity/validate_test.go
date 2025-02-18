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
		etre.Entity{"x": 0},
		etre.Entity{"y": 1},
	}
	err := validate.Entities(entities, entity.VALIDATE_ON_CREATE)
	require.NoError(t, err)
}

func TestValidateCreateEntitiesErrorsMetalabels(t *testing.T) {
	invalid := []etre.Entity{
		etre.Entity{"a": "b", "_id": "59f10d2a5669fc79103a1111"}, // _id not allowed
		etre.Entity{"a": "b", "_type": "node"},                   // _type not allowed
		etre.Entity{"a": "b", "_rev": int64(0)},                  // _rev not allowed
	}

	for _, e := range invalid {
		err := validate.Entities([]etre.Entity{e}, entity.VALIDATE_ON_CREATE)
		assert.Errorf(t, err, "no error creating entity, expected one: %+v", e)

		ve, ok := err.(entity.ValidationError)
		if !ok {
			t.Errorf("error is type %T, expected entity.ValidationError", err)
		} else if ve.Type != "cannot-set-metalabel" {
			t.Errorf("entity.ValidationError.Type = %s, expected cannot-set-metalabel", ve.Type)
		}
	}

	for _, e := range invalid {
		err := validate.Entities([]etre.Entity{e}, entity.VALIDATE_ON_UPDATE)
		require.Error(t, err, "no error creating entity, expected one: %+v", e)

		ve, ok := err.(entity.ValidationError)
		if !ok {
			t.Errorf("error is type %T, expected entity.ValidationError", err)
		} else if ve.Type != "cannot-change-metalabel" {
			t.Errorf("entity.ValidationError.Type = %s, expected cannot-change-metalabel", ve.Type)
		}
	}
}

func TestValidateCreateEntitiesErrorsWhitespace(t *testing.T) {
	invalid := []etre.Entity{
		etre.Entity{" ": "b"},   // label can't be space
		etre.Entity{" a": "b"},  // label can't have space
		etre.Entity{" a ": "b"}, // label can't have space
		etre.Entity{"a ": "b"},  // label can't have space
	}

	for _, e := range invalid {
		err := validate.Entities([]etre.Entity{e}, entity.VALIDATE_ON_CREATE)
		require.Error(t, err, "no error creating entity, expected one: %+v", e)

		ve, ok := err.(entity.ValidationError)
		if !ok {
			t.Errorf("error is type %T, expected entity.ValidationError", err)
		} else if ve.Type != "label-has-whitespace" {
			t.Errorf("entity.ValidationError.Type = %s, expected label-has-whitespace", ve.Type)
		}
	}

	for _, e := range invalid {
		err := validate.Entities([]etre.Entity{e}, entity.VALIDATE_ON_UPDATE)
		require.Error(t, err, "no error creating entity, expected one: %+v", e)

		ve, ok := err.(entity.ValidationError)
		if !ok {
			t.Errorf("error is type %T, expected entity.ValidationError", err)
		} else if ve.Type != "label-has-whitespace" {
			t.Errorf("entity.ValidationError.Type = %s, expected label-has-whitespace", ve.Type)
		}
	}

	invalid = []etre.Entity{
		etre.Entity{"": "b"}, // label can't be empty string
	}

	for _, e := range invalid {
		err := validate.Entities([]etre.Entity{e}, entity.VALIDATE_ON_CREATE)
		require.Error(t, err, "no error creating entity, expected one: %+v", e)

		ve, ok := err.(entity.ValidationError)
		if !ok {
			t.Errorf("error is type %T, expected entity.ValidationError", err)
		} else if ve.Type != "empty-string-label" {
			t.Errorf("entity.ValidationError.Type = %s, expected empty-string-label", ve.Type)
		}
	}

	for _, e := range invalid {
		err := validate.Entities([]etre.Entity{e}, entity.VALIDATE_ON_UPDATE)
		require.Error(t, err, "no error creating entity, expected one: %+v", e)

		ve, ok := err.(entity.ValidationError)
		if !ok {
			t.Errorf("error is type %T, expected entity.ValidationError", err)
		} else if ve.Type != "empty-string-label" {
			t.Errorf("entity.ValidationError.Type = %s, expected cannot-change-metalabel", ve.Type)
		}
	}
}

func TestValidateWriteOpOK(t *testing.T) {
	wo := entity.WriteOp{
		EntityType: "grue",
		Caller:     "dn",
	}
	err := validate.WriteOp(wo)
	require.Error(t, err)

	v, ok := err.(entity.ValidationError)
	if !ok {
		t.Fatalf("err is type %#v, expected entity.ValidationError", err)
	}
	if v.Type != "invalid-entity-type" {
		t.Errorf("Type = %s, expected invalid-entity-type", v.Type)
	}
}

func TestValidateDeleteLabel(t *testing.T) {
	err := validate.DeleteLabel("foo")
	require.NoError(t, err)
	err = validate.DeleteLabel("_id")
	require.Error(t, err)
}
