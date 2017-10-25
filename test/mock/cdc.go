// Copyright 2017, Square, Inc.

package mock

import (
	"github.com/square/etre"
	"github.com/square/etre/cdc"
)

type CDCStore struct {
	WriteFunc func(etre.CDCEvent) error
	ReadFunc  func(cdc.Filter) ([]etre.CDCEvent, error)
}

func (s *CDCStore) Write(e etre.CDCEvent) error {
	if s.WriteFunc != nil {
		return s.WriteFunc(e)
	}
	return nil
}

func (s *CDCStore) Read(filter cdc.Filter) ([]etre.CDCEvent, error) {
	if s.ReadFunc != nil {
		return s.ReadFunc(filter)
	}
	return nil, nil
}

// Some test events that can be insterted into a db.
var CDCEvents = []etre.CDCEvent{
	etre.CDCEvent{EventId: "nru", EntityId: "e1", Rev: 0, Ts: 10},
	etre.CDCEvent{EventId: "vno", EntityId: "e2", Rev: 0, Ts: 13},
	etre.CDCEvent{EventId: "4pi", EntityId: "e3", Rev: 0, Ts: 13},
	etre.CDCEvent{EventId: "p34", EntityId: "e1", Rev: 1, Ts: 22},
	etre.CDCEvent{EventId: "vb0", EntityId: "e5", Rev: 0, Ts: 35},
	etre.CDCEvent{EventId: "bnu", EntityId: "e5", Rev: 1, Ts: 35},
	etre.CDCEvent{EventId: "qwp", EntityId: "e1", Rev: 3, Ts: 39}, // these two are out of
	etre.CDCEvent{EventId: "61p", EntityId: "e1", Rev: 2, Ts: 42}, // order in terms of rev/ts
	etre.CDCEvent{EventId: "2oi", EntityId: "e2", Rev: 1, Ts: 44},
}
