// Copyright 2017, Square, Inc.

package metrics_test

import (
	"testing"
	"time"

	"github.com/square/etre/metrics"
)

func TestCounterInc(t *testing.T) {
	m := metrics.NewMetrics()
	c := metrics.NewCounter()
	m.GetOrRegister("foo", c)
	c.Inc(10)

	expect := int64(10)
	actual := c.Count()

	if expect != actual {
		t.Fatalf("Expect count=%d, Actual count=%d", expect, actual)
	}
}

func TestCounterDec(t *testing.T) {
	m := metrics.NewMetrics()
	c := metrics.NewCounter()
	m.GetOrRegister("foo", c)
	c.Dec(1)

	expect := int64(-1)
	actual := c.Count()

	if expect != actual {
		t.Fatalf("Expect count=%d, Actual count=%d", expect, actual)
	}
}

func TestCounterCount(t *testing.T) {
	m := metrics.NewMetrics()
	c := metrics.NewCounter()
	m.GetOrRegister("foo", c)

	expect := int64(0)
	actual := c.Count()

	if expect != actual {
		t.Fatalf("Expect count=%d, Actual count=%d", expect, actual)
	}
}

func TestCounterClear(t *testing.T) {
	m := metrics.NewMetrics()
	c := metrics.NewCounter()
	m.GetOrRegister("foo", c)
	// Purposefully set the counter to something non-zero
	c.Inc(10)
	// Now clear the count
	c.Clear()

	expect := int64(0)
	actual := c.Count()

	if expect != actual {
		t.Fatalf("Expect count=%d, Actual count=%d", expect, actual)
	}
}

func testTimer(d time.Duration) {
	time.Sleep(d)
}

func TestTimerPercentile(t *testing.T) {
	m := metrics.NewMetrics()
	timer := metrics.NewTimer()
	m.GetOrRegister("foo", timer)

	d := 1 * time.Millisecond
	timer.Time(func() { testTimer(d) })

	// timer.Mean() returns nanosecond, so divide by 1e+06 to convert to millisecond.
	actual95 := timer.Percentile(95.0) / 1000000
	actual99 := timer.Percentile(99.0) / 1000000

	// Since there is only one datapoint, the 95th and 99th percentile should be the same.
	if actual95 != actual99 {
		t.Fatalf("Expect 95th percentile (%f) to equal 99th percentile (%f)", actual95, actual99)
	}
}

func TestTimerMean(t *testing.T) {
	m := metrics.NewMetrics()
	timer := metrics.NewTimer()
	m.GetOrRegister("foo", timer)

	durationValue := 1
	d := time.Duration(durationValue) * time.Millisecond
	timer.Time(func() { testTimer(d) })

	// timer.Mean() returns nanosecond, so divide by 1e+06 to convert to millisecond.
	actual := timer.Mean() / 1000000
	expectLowerBound := float64(durationValue - durationValue)
	expectUpperBound := float64(durationValue + durationValue)

	if actual < expectLowerBound || expectUpperBound < actual {
		t.Fatalf("Expect duration (d) range: %f < actual < %f, Actual duration value=%f", expectLowerBound, expectUpperBound, actual)
	}
}

func TestTimerCount(t *testing.T) {
	m := metrics.NewMetrics()
	timer := metrics.NewTimer()
	m.GetOrRegister("foo", timer)

	expect := int64(2)
	d := 1 * time.Millisecond

	for i := int64(0); i < expect; i++ {
		timer.Time(func() { testTimer(d) })
	}

	actual := timer.Count()

	if expect != actual {
		t.Fatalf("Expect count=%d, Actual count=%d", expect, actual)
	}
}
