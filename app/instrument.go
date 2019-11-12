// Copyright 2019, Square, Inc.

package app

import (
	"time"
)

var Now func() time.Time = time.Now

// Instrument records and reports query profile times. Use one instrument per query.
// Instruments are not safe for multiple goroutines.
type Instrument interface {
	// Start code block timer. Blocks can be nested (e.g. foo calls bar) and called
	// multiple times (e.g. calling foo in a for loop). Block names must be unique
	// within the query being instrumented. Be sure to call Stop for each block.
	Start(block string)

	// Stop code block timer previously started by calling Start.
	Stop(block string)

	// Report all unique code block calls.
	Report() []BlockTime
}

type nopInstrment struct{}

func (in nopInstrment) Start(block string)  {}
func (in nopInstrment) Stop(block string)   {}
func (in nopInstrment) Report() []BlockTime { return []BlockTime{} }

// NopInstrument is a no-op Instrument. When query profile sampling is less than
// 100% (< 1.0), NopInstrument is used in api.go to blackhole skipped measurements.
var NopInstrument nopInstrment

// TimerInstrument is the real Instrument. When a query meets the sampling rate
// in api.go, a TimerInstrument is created to record and report its times (profile).
type TimerInstrument struct {
	level  uint              // call stack
	callNo int               // number of calls
	calls  map[string][]call // keyed on block name, all calls per block
	seq    []BlockTime       // unique/flat call stack
	start  time.Time         // set on very first call to Start
}

// call is one code block call.
type call struct {
	start time.Time
	stop  time.Time
}

// BlockTime represents the time profile for one code block.
type BlockTime struct {
	Block string        // block name
	Level uint          // call stack
	Calls uint          // number of calls
	Time  time.Duration // total time spent in block (all calls)
}

func NewTimerInstrument() *TimerInstrument {
	return &TimerInstrument{
		calls: map[string][]call{},
		seq:   []BlockTime{},
	}
}

func (in *TimerInstrument) Start(block string) {
	now := Now() // now first so we're not instrumenting this func (save a few nanoseconds)

	in.level++
	in.callNo++

	if in.callNo == 1 { // very first call
		in.start = now
	}

	// Init calls map if we haven't seen this code block before
	if _, ok := in.calls[block]; !ok {
		in.calls[block] = []call{}
		in.seq = append(in.seq, BlockTime{
			Block: block,
			Level: in.level,
		})
	}

	// Record start time for code block
	in.calls[block] = append(in.calls[block], call{start: now})
}

func (in *TimerInstrument) Stop(block string) {
	now := Now()

	in.level--

	// Get last call for code block. The last one in the list has to be
	// the current one (which just ended) because Instrument does not support
	// concurrency, so there's can't be N-many block running at a time.
	calls, ok := in.calls[block]
	if !ok || len(calls) == 0 {
		panic("app.TimerInstrument: Start(" + block + ") not called")
	}
	call := calls[len(calls)-1]
	if !call.stop.IsZero() {
		panic("app.TimerInstrument: " + block + " stop time is not zero")
	}

	// Record stop time for code block (last/latest call)
	calls[len(calls)-1].stop = now
}

func (in *TimerInstrument) Report() []BlockTime {
	if in.callNo == 0 {
		return []BlockTime{}
	}

	now := Now()
	total := now.Sub(in.start) // total call time

	var recorded time.Duration
	for i, bt := range in.seq {
		calls, ok := in.calls[bt.Block]
		if !ok || len(calls) == 0 {
			panic("app.TimerInstrument: block " + bt.Block + " does not exit")
		}
		in.seq[i].Calls = uint(len(calls))
		for j, call := range calls {
			if call.stop.IsZero() { // error return before Stop() called
				calls[j].stop = now
			}
			d := call.stop.Sub(call.start)
			in.seq[i].Time += d
			if bt.Level == 1 {
				recorded += d
			}
		}
	}

	// Recorded time (i.e. call code block time) is usually < total time
	// because we don't instrument everything, just important function calls
	// and blocks. So total - record = unrecorded time (reported as Block="?")
	// which could be important: if recorded times are ok but the query misses
	// the SLA, it must be unrecorded time that took too much time. In this case,
	// we need to instrument more code to isolate the time sink.
	if recorded < total {
		in.seq = append(in.seq, BlockTime{
			Block: "?",
			Level: 1,
			Time:  time.Duration(total - recorded),
		})
	}

	return in.seq
}
