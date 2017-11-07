// Copyright 2017, Square, Inc.

// Package query is a wrapper for Kubernetes Labels Selector (KLS). Wrapping
// KLS allows us to expose a simpler struct to other packages in this project.
// The simpler struct is also more flexible as it is agnostic to KLS, meaning
// we can add another query language in the future and not have to modify other
// packages in this project that import this query package. Lastly, the simpler
// struct is easier to mock and test with.
package query

import (
	"strconv"

	"github.com/square/etre/kls"
)

// Query is a list of predicates.
type Query struct {
	Predicates []Predicate
}

// Predicate represents a predicate in a Query.
type Predicate struct {
	Label    string
	Operator string
	Value    interface{}
}

// Translate parses KLS and wraps it in Query struct.
// It returns a Query and an error if encountered while parsing KLS.
func Translate(labelSelectors string) (Query, error) {
	query := Query{}

	req, err := kls.Parse(labelSelectors)
	if err != nil {
		return query, err
	}

	for _, r := range req {
		p := Predicate{
			Label:    r.Label,
			Operator: r.Op,
			Value:    translateValues(r.Op, r.Values),
		}
		query.Predicates = append(query.Predicates, p)
	}

	return query, err
}

// We can make certain assumptions on values for labels.Requirement based on
// the operator. Read more here:
// https://github.com/kubernetes/apimachinery/blob/master/pkg/labels/selector.go#L104-L110.).
// We choose to translate data here to keep data consistent between db package
// and audit log package.
func translateValues(operator string, values []string) interface{} {
	var value interface{}

	switch operator {
	case "in", "notin":
		// Values set must be non-empty.
		value = values
	case "=", "==", "!=":
		// Values set must contain one value.
		value = values[0]
	case ">", ">=", "<", "<=":
		// Values set must contain only one value, which was interpreted as an integer, so convert from string to integer
		value, _ = strconv.Atoi(values[0])
	case "exists", "notexists":
		// Values set must be empty
		value = []string{}
	}

	return value
}
