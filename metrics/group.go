package metrics

import (
	"github.com/square/etre"
)

type Group struct {
	groups []*entityTypeMetrics
}

var _ = &Group{} // ensure Group implements Metrics

func NewGroup(names []string, store Store) Metrics {
	groups := make([]*entityTypeMetrics, len(names))
	for i, groupName := range names {
		sm := store.Get(groupName)
		if sm == nil {
			sm = NewMetrics()
			store.Add(sm, groupName)
		}
		groups[i] = NewEntityMetrics(sm.(*metrics))
	}
	return Group{
		groups: groups,
	}
}

func (mg Group) EntityType(entityType string) {
	for _, m := range mg.groups {
		m.EntityType(entityType)
	}
}

func (mg Group) Inc(mn byte, n int64) {
	for _, m := range mg.groups {
		m.Inc(mn, n)
	}
}

func (mg Group) IncLabel(mn byte, label string) {
	for _, m := range mg.groups {
		m.IncLabel(mn, label)
	}
}

func (mg Group) IncError(mn byte) {
	for _, m := range mg.groups {
		m.IncError(mn)
	}
}

func (mg Group) Val(mn byte, n int64) {
	for _, m := range mg.groups {
		m.Val(mn, n)
	}
}

func (mg Group) Trace(trace map[string]string) {
	for _, m := range mg.groups {
		m.Trace(trace)
	}
}

func (mg Group) Report() etre.MetricsReport {
	panic("do not call metrics.Group.Report() directly")
}
