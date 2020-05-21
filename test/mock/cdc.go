// Copyright 2017-2020, Square, Inc.

package mock

import (
	"context"

	"github.com/square/etre"
	"github.com/square/etre/cdc"
)

var _ cdc.Store = CDCStore{}

type CDCStore struct {
	WriteFunc func(context.Context, etre.CDCEvent) error
	ReadFunc  func(cdc.Filter) ([]etre.CDCEvent, error)
}

func (s CDCStore) Write(ctx context.Context, e etre.CDCEvent) error {
	if s.WriteFunc != nil {
		return s.WriteFunc(ctx, e)
	}
	return nil
}

func (s CDCStore) Read(filter cdc.Filter) ([]etre.CDCEvent, error) {
	if s.ReadFunc != nil {
		return s.ReadFunc(filter)
	}
	return nil, nil
}

// Some test events that can be insterted into a db.
var CDCEvents = []etre.CDCEvent{
	etre.CDCEvent{Id: "nru", EntityId: "e1", EntityRev: 0, Ts: 10},
	etre.CDCEvent{Id: "vno", EntityId: "e2", EntityRev: 0, Ts: 13},
	etre.CDCEvent{Id: "4pi", EntityId: "e3", EntityRev: 0, Ts: 13},
	etre.CDCEvent{Id: "p34", EntityId: "e1", EntityRev: 1, Ts: 22},
	etre.CDCEvent{Id: "vb0", EntityId: "e5", EntityRev: 0, Ts: 35},
	etre.CDCEvent{Id: "bnu", EntityId: "e5", EntityRev: 1, Ts: 35},
	etre.CDCEvent{Id: "qwp", EntityId: "e1", EntityRev: 3, Ts: 39}, // these two are out of
	etre.CDCEvent{Id: "61p", EntityId: "e1", EntityRev: 2, Ts: 42}, // order in terms of rev/ts
	etre.CDCEvent{Id: "2oi", EntityId: "e2", EntityRev: 1, Ts: 44},
}
