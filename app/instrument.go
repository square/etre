package app

import (
	"time"
)

var Now func() time.Time = time.Now

type Instrument interface {
	Start(block string)
	Stop(block string)
	Report() []BlockTime
}

type nopInstrment struct{}

func (in nopInstrment) Start(block string)  {}
func (in nopInstrment) Stop(block string)   {}
func (in nopInstrment) Report() []BlockTime { return []BlockTime{} }

var NopInstrument nopInstrment

type TimerInstrument struct {
	level  uint
	callNo int
	calls  map[string][]call
	seq    []BlockTime
	start  time.Time
}

type call struct {
	level uint
	// --
	callNo int
	start  time.Time
	stop   time.Time
}

type BlockTime struct {
	Block string
	Level uint
	Calls uint
	Time  time.Duration
}

func NewTimerInstrument() *TimerInstrument {
	return &TimerInstrument{
		calls: map[string][]call{},
		seq:   []BlockTime{},
	}
}

func (in *TimerInstrument) Start(block string) {
	now := Now()

	in.level++
	in.callNo++

	if in.callNo == 1 { // very first call
		in.start = now
	}

	if _, ok := in.calls[block]; !ok {
		in.calls[block] = []call{}
		in.seq = append(in.seq, BlockTime{
			Block: block,
			Level: in.level,
		})
	}
	in.calls[block] = append(in.calls[block], call{
		level:  in.level,
		callNo: in.callNo,
		start:  now,
	})
}

func (in *TimerInstrument) Stop(block string) {
	now := Now()

	in.level--
	calls, ok := in.calls[block]
	if !ok || len(calls) == 0 {
		panic("app.TimerInstrument: Start(" + block + ") not called")
	}
	call := calls[len(calls)-1]
	if !call.stop.IsZero() {
		panic("app.TimerInstrument: " + block + " stop time is not zero")
	}
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

	if recorded < total {
		in.seq = append(in.seq, BlockTime{
			Block: "?",
			Level: 1,
			Time:  time.Duration(total - recorded),
		})
	}

	return in.seq
}
