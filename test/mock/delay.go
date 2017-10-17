// Copyright 2017, Square, Inc.

package mock

import ()

type DelayManager struct {
	MaxTimestampFunc func() (int64, error)
	BeginChangeFunc  func(string) error
	EndChangeFunc    func(string) error
}

func (m *DelayManager) MaxTimestamp() (int64, error) {
	if m.MaxTimestampFunc != nil {
		return m.MaxTimestampFunc()
	}
	return 0, nil
}

func (m *DelayManager) BeginChange(changeId string) error {
	if m.BeginChangeFunc != nil {
		return m.BeginChangeFunc(changeId)
	}
	return nil
}

func (m *DelayManager) EndChange(changeId string) error {
	if m.EndChangeFunc != nil {
		return m.EndChangeFunc(changeId)
	}
	return nil
}
