// Copyright 2017-2020, Square, Inc.

package query_test

import (
	"testing"

	"github.com/go-test/deep"

	"github.com/square/etre/query"
)

type test struct {
	query        string
	expect       query.Query
	returnsError bool
}

func TestQueryTranslate(t *testing.T) {
	testCases := []test{
		// Valid
		// ------------------------------------------------------------------
		{
			query: "foo=bar",
			expect: query.Query{
				Predicates: []query.Predicate{
					query.Predicate{
						Label:    "foo",
						Operator: "=",
						Value:    "bar",
					},
				},
			},
		},
		{
			query: "_id=5ec543505c222dbd2ad74720",
			expect: query.Query{
				Predicates: []query.Predicate{
					query.Predicate{
						Label:    "_id",
						Operator: "=",
						Value:    "5ec543505c222dbd2ad74720",
					},
				},
			},
		},
		{
			query: "foo",
			expect: query.Query{
				Predicates: []query.Predicate{
					query.Predicate{
						Label:    "foo",
						Operator: "exists",
					},
				},
			},
		},
		{
			query: "!foo",
			expect: query.Query{
				Predicates: []query.Predicate{
					query.Predicate{
						Label:    "foo",
						Operator: "notexists",
					},
				},
			},
		},
		{
			query: "foo=bar, name notin (baz,qux)",
			expect: query.Query{
				Predicates: []query.Predicate{
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
			},
		},

		// Invalid
		// ------------------------------------------------------------------
		{
			query:        "foo=", // missing value
			returnsError: true,
		},
		{
			query:        "=val", // missing label
			returnsError: true,
		},
	}
	for _, tc := range testCases {
		got, err := query.Translate(tc.query)
		if tc.returnsError && err == nil {
			t.Errorf("query '%s' should return an error but didn't", tc.query)
		}
		if diff := deep.Equal(got, tc.expect); diff != nil {
			t.Errorf("query '%s': %v", tc.query, diff)
		}
	}
}
