// Copyright 2017-2018, Square, Inc.

package entity_test

import (
	"testing"

	"github.com/square/etre"
	"github.com/square/etre/entity"
)

var validate = entity.NewValidator(entityTypes)

func TestValidateEntities(t *testing.T) {
	// All ok
	entities := []etre.Entity{
		etre.Entity{"x": 0},
		etre.Entity{"y": 1},
	}
	err := validate.Entities(entities, entity.VALIDATE_ON_CREATE)
	if err != nil {
		t.Errorf("got err '%v', expected nil", err)
	}
}

func TestValidateWriteOp(t *testing.T) {
	wo := entity.WriteOp{
		EntityType: "grue",
		Caller:     "dn",
	}
	err := validate.WriteOp(wo)
	if err == nil {
		t.Fatal("err is nill, expected an enitty.ValidationError")
	}
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
	if err != nil {
		t.Errorf("got err '%v', expected nil", err)
	}
	err = validate.DeleteLabel("_id")
	if err == nil {
		t.Fatal("err is nill, expected an enitty.ValidationError")
	}
}
