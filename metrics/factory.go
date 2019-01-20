// Copyright 2018-2019, Square, Inc.

package metrics

type Factory interface {
	Make(groupNames []string) Metrics
}

type GroupFactory struct {
	Store Store
}

func (f GroupFactory) Make(groupNames []string) Metrics {
	return NewGroup(groupNames, f.Store)
}
