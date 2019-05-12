// Copyright 2019, Square, Inc.

package mock

import (
	"github.com/square/etre"
	"github.com/square/etre/metrics"
)

type MetricsFactory struct {
	MetricRecorder *MetricRecorder
}

var _ metrics.Factory = MetricsFactory{}

func (f MetricsFactory) Make(groupNames []string) metrics.Metrics {
	return f.MetricRecorder
}

// --------------------------------------------------------------------------

type MetricMethodArgs struct {
	Method    string
	Metric    byte
	IntVal    int64
	StringVal string
}

var _ metrics.Metrics = &MetricRecorder{}

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

func (m *MetricRecorder) Val(mn byte, n int64) {
	m.Called = append(m.Called, MetricMethodArgs{
		Method: "Val",
		Metric: mn,
		IntVal: n,
	})
}

func (m *MetricRecorder) Trace(map[string]string) {
}

func (m *MetricRecorder) Report(reset bool) etre.Metrics {
	return etre.Metrics{}
}

// --------------------------------------------------------------------------

type MetricsStore struct {
	AddFunc   func(m metrics.Metrics, name string) error
	GetFunc   func(name string) metrics.Metrics
	NamesFunc func() []string
}

var _ metrics.Store = MetricsStore{}

func (s MetricsStore) Add(m metrics.Metrics, name string) error {
	if s.AddFunc != nil {
		return s.AddFunc(m, name)
	}
	return nil
}

func (s MetricsStore) Get(name string) metrics.Metrics {
	if s.GetFunc != nil {
		return s.GetFunc(name)
	}
	return nil
}

func (s MetricsStore) Names() []string {
	if s.NamesFunc != nil {
		return s.NamesFunc()
	}
	return []string{"etre"} // auth.DEFAULT_METRIC_GROUP
}
