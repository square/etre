// Copyright 2018, Square, Inc.
package entity

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/square/etre"
)

const (
	VALIDATE_ON_CREATE byte = iota
	VALIDATE_ON_UPDATE
	VALIDATE_ON_DELETE
)

type ValidationError struct {
	Err  error
	Type string
}

func (e ValidationError) Error() string {
	return e.Err.Error()
}

type Validator interface {
	EntityType(string) error
	Entities([]etre.Entity, byte) error
	WriteOp(WriteOp) error
	DeleteLabel(string) error
}

type validator struct {
	entityTypes []string
	validType   map[string]bool
}

func NewValidator(entityTypes []string) validator {
	validType := map[string]bool{}
	for _, t := range entityTypes {
		validType[t] = true
	}
	return validator{
		entityTypes: entityTypes,
		validType:   validType,
	}
}

func (v validator) EntityType(entityType string) error {
	if !v.validType[entityType] {
		return ValidationError{
			Err:  fmt.Errorf("invalid entity type: %s; valid types (config.entity.types): %s", entityType, strings.Join(v.entityTypes, ", ")),
			Type: "invalid-entity-type",
		}
	}
	return nil
}

// Valid returns nils if all the entities are valid.
func (v validator) Entities(entities []etre.Entity, op byte) error {
	for i, e := range entities {

		// Cannot use {} (empty entity) to patch or create because empty entities
		// aren't allowed. Maybe caller means to delete the entity?
		if len(e) == 0 && (op == VALIDATE_ON_UPDATE || op == VALIDATE_ON_CREATE) {
			return ValidationError{
				Err:  fmt.Errorf("entity at index %d is empty (no labels); empty entities are not allowed on create or patch", i),
				Type: "empty-entity",
			}
		}

		for label, val := range e {
			if label == "" {
				return ValidationError{
					Err:  fmt.Errorf("empty string label (entity index %d)", i),
					Type: "empty-string-label",
				}
			}
			if strings.IndexAny(label, " \t") != -1 {
				return ValidationError{
					Err:  fmt.Errorf("label cannot have whitesspace: '%s' (entity index %d)", label, i),
					Type: "label-has-whitespace",
				}
			}
			switch op {
			case VALIDATE_ON_CREATE:
				// User cannot set these metalabels on create
				for _, ml := range []string{"_id", "_type", "_rev", "_ts"} {
					if label != ml {
						continue
					}
					if _, ok := e[ml]; ok {
						return ValidationError{
							Err:  fmt.Errorf("cannot set metalabel %s on create (entity index %d)", ml, i),
							Type: "cannot-set-metalabel",
						}
					}
				}
			case VALIDATE_ON_UPDATE:
				// Cannot patch (change) metalabel values
				if etre.IsMetalabel(label) {
					return ValidationError{
						Err:  fmt.Errorf("cannot change metalabel %s on patch (entity index %d)", label, i),
						Type: "cannot-change-metalabel",
					}
				}
			}

			// JSON treats all numbers as floats. Given this, when we see a float with
			// decimal values of all 0, it is unclear if the user passed in 3.0 (type
			// float) or 3 (type int). So, since we cannot tell the difference between a
			// some float numbers and integer numbers, we cast all floats to ints. This
			// means that floats with non-zero decimal values, such as 3.14 (type float),
			// will get truncated to 3 (type int).
			if val == nil {
				continue
			}
			if reflect.TypeOf(val).Kind() == reflect.Float64 {
				entities[i][label] = int(val.(float64))
			} else {
				// Values in entity must be of type string or int. This is because the query
				// language we use only supports querying by string or int. See more at:
				// github.com/square/etre/query
				k := reflect.TypeOf(val).Kind()
				valid := k == reflect.String || k == reflect.Int || k == reflect.Bool
				if !valid {
					return ValidationError{
						Err:  fmt.Errorf("invalid value type %s for key %v (value: %v); valid types: string, int, bool (entity index %d)", reflect.TypeOf(val), label, val, i),
						Type: "invalid-value-type",
					}
				}
			}
		}
	}
	return nil
}

func (v validator) WriteOp(wo WriteOp) error {
	if err := v.EntityType(wo.EntityType); err != nil {
		return err
	}
	return nil
}

func (v validator) DeleteLabel(label string) error {
	if etre.IsMetalabel(label) {
		return ValidationError{
			Err:  fmt.Errorf("cannot delete metalabel %s", label),
			Type: "cannot-delete-metalabel",
		}
	}
	return nil
}
