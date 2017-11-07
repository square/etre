// Copyright 2017, Square, Inc.

package query_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/square/etre/query"
)

func TestTranslateSingle(t *testing.T) {
	expectedQuery := query.Query{
		[]query.Predicate{
			query.Predicate{
				Label:    "foo",
				Operator: "=",
				Value:    "bar",
			},
		},
	}

	labelSelector := "foo=bar"
	actualQuery, err := query.Translate(labelSelector)

	if !reflect.DeepEqual(actualQuery, expectedQuery) {
		t.Errorf("query = %v, expected %v", actualQuery, expectedQuery)
	}
	if err != nil {
		t.Errorf("err = %s, expected nil", err)
	}
}

func TestTranslateMultiple(t *testing.T) {
	expectedQuery := query.Query{
		[]query.Predicate{
			query.Predicate{
				Label:    "foo",
				Operator: "=",
				Value:    "bar",
			},
			query.Predicate{
				Label:    "name",
				Operator: "notin",
				Value:    []string{"baz", "qux"},
			},
		},
	}

	labelSelector := "foo=bar, name notin (baz,qux)"
	actualQuery, err := query.Translate(labelSelector)

	if !reflect.DeepEqual(actualQuery, expectedQuery) {
		t.Errorf("query = %v, expected %v", actualQuery, expectedQuery)
	}
	if err != nil {
		t.Errorf("err = %s, expected nil", err)
	}
}

func TestTranslateError(t *testing.T) {
	t.Skip("add more validation to ksl.Parse()")
	labelSelector := "foo~~bar"
	_, err := query.Translate(labelSelector)

	expectedErrMsg := "unable to parse requirement"

	if !strings.Contains(err.Error(), expectedErrMsg) {
		t.Errorf("err = %s, expected to contain: %s", err, expectedErrMsg)
	}
}
