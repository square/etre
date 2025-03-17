package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMutexUnlock ensures that the mutex is released after a panic in groupEntityMetrics
func TestMutexUnlock(t *testing.T) {
	// IncLabel
	gem := groupEntityMetrics{groupMetrics: NewGroupMetrics()}
	doPanic(t, func() {
		gem.IncLabel(Query, "foo") // will panic because we never called EntityType first
	})
	assert.True(t, gem.TryLock(), "groupEntityMetrics lock was not released after the panic")

	// Trace
	gem = groupEntityMetrics{groupMetrics: NewGroupMetrics()}
	doPanic(t, func() {
		gem.Trace(map[string]string{"k": "v"}) // will panic because we never called EntityType first
	})
	assert.True(t, gem.TryLock(), "groupEntityMetrics lock was not released after the panic")

	// Inc
	gem = groupEntityMetrics{groupMetrics: NewGroupMetrics()}
	doPanic(t, func() {
		gem.Inc(Read, 1) // will panic because we never called EntityType first
	})
	assert.True(t, gem.TryLock(), "groupEntityMetrics lock was not released after the panic")

	// Val
	gem = groupEntityMetrics{groupMetrics: NewGroupMetrics()}
	doPanic(t, func() {
		gem.Val(Read, 1) // will panic because we never called EntityType first
	})
	assert.True(t, gem.TryLock(), "groupEntityMetrics lock was not released after the panic")

	// Report
	gem = groupEntityMetrics{groupMetrics: NewGroupMetrics()}
	doPanic(t, func() {
		gem.entity = map[string]*entityMetrics{"k": nil} // need this to trigger a panic in report
		gem.Report(false)
	})
	assert.True(t, gem.TryLock(), "groupEntityMetrics lock was not released after the panic")

	// Report
	gem = groupEntityMetrics{groupMetrics: NewGroupMetrics()}
	doPanic(t, func() {
		gem.entity = nil // need this to trigger a panic in report
		gem.EntityType("foo")
	})
	assert.True(t, gem.TryLock(), "groupEntityMetrics lock was not released after the panic")
}

func doPanic(t *testing.T, f func()) {
	defer func() {
		if r := recover(); r != nil {
			// This is expected
		}
	}()
	f()
	require.Fail(t, "Expected panic but did not get one")
}
