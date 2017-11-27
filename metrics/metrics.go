// Copyright 2017, Square, Inc.

// Package metrics is a wrapper of github.com/rcrowley/go-metrics.
package metrics

import (
	gometrics "github.com/rcrowley/go-metrics"
)

type Metrics interface {
	GetOrRegister(string, interface{})
}

type metrics struct {
	registry gometrics.Registry
}

type Counter interface {
	Inc(int64)
	Dec(int64)
	Count() int64
	Clear()
}

type counter struct {
	counter gometrics.Counter
}

type Timer interface {
	Time(func())
	Percentile(float64) float64
	Mean() float64
	Count() int64
}

type timer struct {
	timer gometrics.Timer
}

func NewMetrics() Metrics {
	return &metrics{
		registry: gometrics.NewRegistry(),
	}
}

// Gets an existing metric or creates and registers a new one.
func (m *metrics) GetOrRegister(n string, i interface{}) {
	m.registry.GetOrRegister(n, i)
}

func NewCounter() Counter {
	return &counter{
		counter: gometrics.NewCounter(),
	}
}

// Inc increments the counter by the given amount.
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

func NewTimer() Timer {
	return &timer{
		timer: gometrics.NewTimer(),
	}
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
