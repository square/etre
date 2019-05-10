// Copyright 2019, Square, Inc.

package metrics

import (
	"fmt"
	"sync"

	gm "github.com/daniel-nichter/go-metrics"
	"github.com/square/etre"
)

type systemMetrics struct {
	*sync.Mutex

	// Fields in an etre.MetricsSystemReport:
	query             *gm.Counter
	authFail          *gm.Counter
	invalidEntityType *gm.Counter
}

var _ Metrics = &systemMetrics{} // ensure systemMetrics implements Metrics

func NewSystemMetrics() *systemMetrics {
	return &systemMetrics{
		Mutex:             &sync.Mutex{},
		query:             gm.NewCounter(),
		authFail:          gm.NewCounter(),
		invalidEntityType: gm.NewCounter(),
	}
}

func (m *systemMetrics) EntityType(entityType string) {
	panic("EntityType() called on systemMetrics")
}

func (m *systemMetrics) Inc(mn byte, n int64) {
	switch mn {
	case Query:
		m.query.Add(n)
	case AuthenticationFailed:
		m.authFail.Add(n)
	default:
		errMsg := fmt.Sprintf("non-counter metric number passed to Inc: %d", mn)
		panic(errMsg)
	}
}

func (m *systemMetrics) IncLabel(mn byte, label string) {
	panic("IncLabel() called on systemMetrics")
}

func (m *systemMetrics) Val(mn byte, n int64) {
	panic("Val() called on systemMetrics")
}

func (m *systemMetrics) Trace(trace map[string]string) {
	panic("Trace() called on systemMetrics")
}

func (m *systemMetrics) Report(reset bool) etre.Metrics {
	// reset currently only works on samples, not counters
	m.Lock()
	defer m.Unlock()
	r := &etre.MetricsSystemReport{
		Query:                m.query.Count(),
		AuthenticationFailed: m.authFail.Count(),
	}
	return etre.Metrics{System: r}
}
