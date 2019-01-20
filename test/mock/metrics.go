// Copyright 2019, Square, Inc.

package mock

import (
	"github.com/square/etre"
	"github.com/square/etre/metrics"
)

type MetricsFactory struct {
	MetricRecorder *MetricRecorder
}

func (f MetricsFactory) Make(groupNames []string) metrics.Metrics {
	return f.MetricRecorder
}

type MetricMethodArgs struct {
	Method    string
	Metric    byte
	IntVal    int64
	StringVal string
}

// MetricRecorder records the called methods and values.
type MetricRecorder struct {
	Called []MetricMethodArgs
}

func NewMetricsRecorder() *MetricRecorder {
	return &MetricRecorder{
		Called: []MetricMethodArgs{},
	}
}

func (m *MetricRecorder) Reset() {
	m.Called = []MetricMethodArgs{}
}

func (m *MetricRecorder) EntityType(et string) {
	m.Called = append(m.Called, MetricMethodArgs{
		Method:    "EntityType",
		StringVal: et,
	})
}

func (m *MetricRecorder) Inc(mn byte, n int64) {
	m.Called = append(m.Called, MetricMethodArgs{
		Method: "Inc",
		Metric: mn,
		IntVal: n,
	})
}

func (m *MetricRecorder) IncLabel(mn byte, label string) {
	m.Called = append(m.Called, MetricMethodArgs{
		Method:    "IncLabel",
		Metric:    mn,
		StringVal: label,
	})
}

func (m *MetricRecorder) IncError(mn byte) {
	m.Called = append(m.Called, MetricMethodArgs{
		Method: "IncError",
		Metric: mn,
	})
}

func (m *MetricRecorder) Val(mn byte, n int64) {
	m.Called = append(m.Called, MetricMethodArgs{
		Method: "Val",
		Metric: mn,
		IntVal: n,
	})
}

func (m *MetricRecorder) Trace(map[string]string) {
}

func (m *MetricRecorder) Report() etre.MetricsReport {
	return etre.MetricsReport{}
}
