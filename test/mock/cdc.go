// Copyright 2017-2019, Square, Inc.

package mock

import (
	"errors"

	"github.com/gorilla/websocket"
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

// --------------------------------------------------------------------------

type Feed struct {
	StartFunc func()
	WaitFunc  func()
	StopFunc  func()
}

func (f *Feed) Start() {
	if f.StartFunc != nil {
		f.StartFunc()
	}
}

func (f *Feed) Wait() {
	if f.WaitFunc != nil {
		f.WaitFunc()
	}
}

func (f *Feed) Stop() {
	if f.StopFunc != nil {
		f.StopFunc()
	}
}

type FeedFactory struct {
	MakeWebsocketFunc func(*websocket.Conn) *cdc.WebsocketFeed
	MakeInternalFunc  func(int) *cdc.InternalFeed
}

func (ff *FeedFactory) MakeWebsocket(wsConn *websocket.Conn) *cdc.WebsocketFeed {
	if ff.MakeWebsocketFunc != nil {
		return ff.MakeWebsocketFunc(wsConn)
	}
	return nil
}

func (ff *FeedFactory) MakeInternal(clientBufferSize int) *cdc.InternalFeed {
	if ff.MakeInternalFunc != nil {
		return ff.MakeInternalFunc(clientBufferSize)
	}
	return nil
}

// --------------------------------------------------------------------------

var (
	ErrPoller = errors.New("error in poller")
)

type Poller struct {
	RunFunc        func() error
	RegisterFunc   func(string) (<-chan etre.CDCEvent, int64, error)
	DeregisterFunc func(string)
	ErrorFunc      func() error
}

func (p *Poller) Run() error {
	if p.RunFunc != nil {
		p.RunFunc()
	}
	return nil
}

func (p *Poller) Register(uuid string) (<-chan etre.CDCEvent, int64, error) {
	if p.RegisterFunc != nil {
		return p.RegisterFunc(uuid)
	}
	return nil, 0, nil
}

func (p *Poller) Deregister(uuid string) {
	if p.DeregisterFunc != nil {
		p.DeregisterFunc(uuid)
	}
}

func (p *Poller) Error() error {
	if p.ErrorFunc != nil {
		return p.ErrorFunc()
	}
	return nil
}
