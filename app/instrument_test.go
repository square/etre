// Copyright 2019, Square, Inc.

package app_test

import (
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/square/etre/app"
)

var now time.Time

func setup() {
	app.Now = func() time.Time { return now }
}

func TestInstrumentOneBlockOneCall(t *testing.T) {
	setup()

	inst := app.NewTimerInstrument()

	now = time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
	inst.Start("foo")

	now = time.Date(2009, time.November, 10, 23, 0, 2, 0, time.UTC)
	inst.Stop("foo")

	got := inst.Report()
	expect := []app.BlockTime{
		{
			Block: "foo",
			Level: 1,
			Calls: 1,
			Time:  time.Duration(2 * time.Second),
		},
	}
	if diff := deep.Equal(got, expect); diff != nil {
		t.Error(diff)
	}
}

func TestInstrumentManyBlocksOneCall(t *testing.T) {
	setup()

	inst := app.NewTimerInstrument()

	now = time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
	inst.Start("a")
	now = time.Date(2009, time.November, 10, 23, 0, 2, 0, time.UTC)
	inst.Stop("a")

	now = time.Date(2009, time.November, 10, 23, 0, 3, 0, time.UTC)
	inst.Start("b")
	now = time.Date(2009, time.November, 10, 23, 0, 4, 0, time.UTC)
	inst.Stop("b")

	now = time.Date(2009, time.November, 10, 23, 0, 4, 0, time.UTC)
	inst.Start("c")
	now = time.Date(2009, time.November, 10, 23, 0, 7, 0, time.UTC)
	inst.Stop("c")

	got := inst.Report()
	expect := []app.BlockTime{
		{Block: "a", Level: 1, Calls: 1, Time: time.Duration(2 * time.Second)},
		{Block: "b", Level: 1, Calls: 1, Time: time.Duration(1 * time.Second)},
		{Block: "c", Level: 1, Calls: 1, Time: time.Duration(3 * time.Second)},
		{Block: "?", Level: 1, Calls: 0, Time: time.Duration(1 * time.Second)},
	}
	if diff := deep.Equal(got, expect); diff != nil {
		t.Logf("%+v", got)
		t.Error(diff)
	}
}

func TestInstrumentNestedBlockCall(t *testing.T) {
	setup()

	inst := app.NewTimerInstrument()

	now = time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
	inst.Start("a")

	// b nested in a
	now = time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
	inst.Start("b")
	now = time.Date(2009, time.November, 10, 23, 0, 2, 0, time.UTC)
	inst.Stop("b")

	now = time.Date(2009, time.November, 10, 23, 0, 3, 0, time.UTC)
	inst.Stop("a")

	now = time.Date(2009, time.November, 10, 23, 0, 4, 0, time.UTC)
	got := inst.Report()
	expect := []app.BlockTime{
		{Block: "a", Level: 1, Calls: 1, Time: time.Duration(3 * time.Second)},
		{Block: "b", Level: 2, Calls: 1, Time: time.Duration(2 * time.Second)},
		{Block: "?", Level: 1, Calls: 0, Time: time.Duration(1 * time.Second)},
	}
	if diff := deep.Equal(got, expect); diff != nil {
		t.Error(diff)
	}
}
