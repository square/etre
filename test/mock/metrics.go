// Copyright 2019, Square, Inc.

package mock

import (
	"github.com/square/etre"
	"github.com/square/etre/metrics"
)

type MetricsFactory struct {
	MetricRecorder *MetricRecorder
	Real           metrics.Factory
}

var _ metrics.Factory = MetricsFactory{}

func NewMetricsFactory(real metrics.Factory, recorder *MetricRecorder) MetricsFactory {
	return MetricsFactory{
		MetricRecorder: recorder,
		Real:           real,
	}
}

func (f MetricsFactory) Make(groupNames []string) metrics.Metrics {
	return metricsMultiplexer{
		real:     f.Real.Make(groupNames),
		recorder: f.MetricRecorder,
	}
}

func NewSystemMetrics(real metrics.Metrics, recorder *MetricRecorder) metrics.Metrics {
	return metricsMultiplexer{
		real:     real,
		recorder: recorder,
	}
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

var _ metrics.Metrics = &metricsMultiplexer{}

// metricsMultiplexer is a metrics.Metrics that calls both the real and recorder.
// The actual metrics implementation is complex and difficult to properly mock. This multiplexer
// is used so we can exercise the real metrics code, but also get the metrics back in an easy to
// test form with the recorder.
type metricsMultiplexer struct {
	real     metrics.Metrics
	recorder metrics.Metrics
}

func (m metricsMultiplexer) EntityType(s string) {
	m.real.EntityType(s)
	m.recorder.EntityType(s)
}

func (m metricsMultiplexer) Inc(mn byte, n int64) {
	m.real.Inc(mn, n)
	m.recorder.Inc(mn, n)
}

func (m metricsMultiplexer) IncLabel(mn byte, label string) {
	m.real.IncLabel(mn, label)
	m.recorder.IncLabel(mn, label)
}

func (m metricsMultiplexer) Val(mn byte, n int64) {
	m.real.Val(mn, n)
	m.recorder.Val(mn, n)
}

func (m metricsMultiplexer) Trace(m2 map[string]string) {
	m.real.Trace(m2)
	m.recorder.Trace(m2)
}

func (m metricsMultiplexer) Report(reset bool) etre.Metrics {
	r := m.real.Report(reset)
	m.recorder.Report(reset)
	return r
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
