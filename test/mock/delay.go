// Copyright 2017, Square, Inc.

package mock

import ()

type Delayer struct {
	MaxTimestampFunc func() (int64, error)
	BeginChangeFunc  func(string) error
	EndChangeFunc    func(string) error
}

func (d *Delayer) MaxTimestamp() (int64, error) {
	if d.MaxTimestampFunc != nil {
		return d.MaxTimestampFunc()
	}
	return 0, nil
}

func (d *Delayer) BeginChange(changeId string) error {
	if d.BeginChangeFunc != nil {
		return d.BeginChangeFunc(changeId)
	}
	return nil
}

func (d *Delayer) EndChange(changeId string) error {
	if d.EndChangeFunc != nil {
		return d.EndChangeFunc(changeId)
	}
	return nil
}
