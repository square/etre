// Copyright 2017, Square, Inc.

// Package query is a wrapper for Kubernetes Labels Selector (KLS). Wrapping
// KLS allows us to expose a simpler struct to other packages in this project.
// The simpler struct is also more flexible as it is agnostic to KLS, meaning
// we can add another query language in the future and not have to modify other
// packages in this project that import this query package. Lastly, the simpler
// struct is easier to mock and test with.
package query

import (
	"k8s.io/apimachinery/pkg/labels"
)

// Query is a list of predicates.
type Query struct {
	Predicates []Predicate
}

// Predicate is a label, operator, and value. If using KLS syntax,
// "foo=bar", would translate to a Predicate where Label="foo", Operator="=",
// Values=["bar"].
type Predicate struct {
	Label    string
	Operator string
	Values   []string
}

// Translate parses KLS and wraps it in Query struct.
// It returns a Query and an error if encountered while parsing KLS.
func Translate(labelSelectors string) (Query, error) {
	query := Query{}

	req, err := labels.ParseToRequirements(labelSelectors)
	if err != nil {
		return query, err
	}

	for _, r := range req {
		predicate := Predicate{
			Label:    r.Key(),
			Operator: string(r.Operator()),
			Values:   r.Values().List(),
		}
		query.Predicates = append(query.Predicates, predicate)
	}

	return query, err
}
