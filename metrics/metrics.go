// Copyright 2017, Square, Inc.

// Package metrics provides a threadsafe way to manage counters and timers.
package metrics

import (
	gometrics "github.com/rcrowley/go-metrics"
)

// Metrics holds a registry where you can get and register counters and timers
// in a threadsafe way.
type Metrics interface {
	GetOrRegisterCounter(string) Counter
	GetOrRegisterTimer(string) Timer
}

// TODO: add docs
type metrics struct {
	registry gometrics.Registry
}

// A Counter holds an int64 value that can be incremented and decremented.
type Counter interface {
	Inc(int64)
	Dec(int64)
	Count() int64
	Clear()
}

// A counter is an internal representation of Counter. It is not exported to
// hide implementation details.
type counter struct {
	counter gometrics.Counter
}

// A Timer can time the runtime of functions and store those timed values as
// well as calculate the mean, count, and various percentiles of those values.
type Timer interface {
	Time(func())
	Percentile(float64) float64
	Mean() float64
	Count() int64
}

// A timer is an internal representation of Timer. It is not exported to hide
// implementation details.
type timer struct {
	timer gometrics.Timer
}

func NewMetrics() Metrics {
	return &metrics{
		registry: gometrics.NewRegistry(),
	}
}

// GetOrRegisterCounter returns an existing Counter or constructs and registers
// a new Counter.
func (m *metrics) GetOrRegisterCounter(n string) Counter {
	c := gometrics.GetOrRegisterCounter(n, m.registry)
	return &counter{counter: c}
}

func (c *counter) Inc(i int64) {
	c.counter.Inc(i)
}

// Dec decrements the counter by the given amount.
func (c *counter) Dec(i int64) {
	c.counter.Dec(i)
}

// Count returns the current count.
func (c *counter) Count() int64 {
	return c.counter.Count()
}

// Clear sets the counter to zero.
func (c *counter) Clear() {
	c.counter.Clear()
}

// GetOrRegisterTimer returns an existing Timer or constructs and registers a
// new Timer.
func (m *metrics) GetOrRegisterTimer(n string) Timer {
	t := gometrics.GetOrRegisterTimer(n, m.registry)
	return &timer{timer: t}
}

// Record the duration of the execution of the given function.
func (t *timer) Time(f func()) {
	// This saves in nanoseconds
	t.timer.Time(f)
}

// Percentile returns an arbitrary percentile of the values in the sample.
func (t *timer) Percentile(f float64) float64 {
	return t.timer.Percentile(f)
}

// Mean returns the mean of the values in the sample.
func (t *timer) Mean() float64 {
	return t.timer.Mean()
}

// Count returns the number of events recorded.
func (t *timer) Count() int64 {
	return t.timer.Count()
}
