// Copyright 2020, Square, Inc.

package mock

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/square/etre"
	"github.com/square/etre/cdc/changestream"
)

type ChangeStreamServer struct {
	WatchFunc func(string) (<-chan etre.CDCEvent, error)
	CloseFunc func(string)
	RunFunc   func() error
	StopFunc  func()
}

func (s ChangeStreamServer) Watch(clientId string) (<-chan etre.CDCEvent, error) {
	if s.WatchFunc != nil {
		return s.WatchFunc(clientId)
	}
	return nil, nil
}

func (s ChangeStreamServer) Close(clientId string) {
	if s.CloseFunc != nil {
		s.CloseFunc(clientId)
	}
}

func (s ChangeStreamServer) Run() error {
	if s.RunFunc != nil {
		return s.RunFunc()
	}
	return nil
}

func (s ChangeStreamServer) Stop() {
	if s.StopFunc != nil {
		s.StopFunc()
	}
}

// --------------------------------------------------------------------------

var _ changestream.StreamerFactory = StreamerFactory{}

type StreamerFactory struct {
	MakeFunc func(clientId string) changestream.Streamer
}

func (f StreamerFactory) Make(clientId string) changestream.Streamer {
	if f.MakeFunc != nil {
		return f.MakeFunc(clientId)
	}
	return Stream{}
}

var _ changestream.Streamer = &Stream{}

type Stream struct {
	StartFunc  func(sinceTs int64) <-chan etre.CDCEvent
	InSyncFunc func() chan struct{}
	StatusFunc func() changestream.Status
	StopFunc   func()
	ErrorFunc  func() error
}

func (s Stream) Start(sinceTs int64) <-chan etre.CDCEvent {
	if s.StartFunc != nil {
		return s.StartFunc(sinceTs)
	}
	return nil

}

func (s Stream) InSync() chan struct{} {
	if s.InSyncFunc != nil {
		return s.InSyncFunc()
	}
	return nil
}

func (s Stream) Status() changestream.Status {
	if s.StatusFunc != nil {
		return s.StatusFunc()
	}
	return changestream.Status{}
}

func (s Stream) Stop() {
	if s.StopFunc != nil {
		s.StopFunc()
	}
}

func (s Stream) Error() error {
	if s.ErrorFunc != nil {
		return s.ErrorFunc()
	}
	return nil
}

// --------------------------------------------------------------------------

var RawInsertEvents = []bson.M{
	primitive.M{
		"_id":         primitive.M{"_data": primitive.Binary{Data: []uint8{0x82, 0x5e, 0x8f, 0x5c, 0xf7, 0x0, 0x0, 0x0, 0x26, 0x46, 0x3c, 0x5f, 0x69, 0x64, 0x0, 0x3c, 0x61, 0x62, 0x63, 0x0, 0x0, 0x5a, 0x10, 0x4, 0x4d, 0x5e, 0xa3, 0x2c, 0x5c, 0x27, 0x4c, 0xf8, 0xb1, 0xad, 0x80, 0x30, 0x3a, 0x44, 0xbc, 0x82, 0x4}}},
		"documentKey": primitive.M{"_id": "abc"},
		"fullDocument": primitive.M{ // etre.CDCEvent
			"_id":        "abc",
			"entityId":   "e13",
			"entityType": "node",
			"op":         "i",
			"rev":        int64(7),
			"ts":         54,
			"user":       "mike",
			"new":        primitive.M{"_id": "e13", "_rev": int64(7), "_type": "node", "foo": "bar"},
		},
		"ns":            primitive.M{"coll": "cdc", "db": "etre_test"},
		"operationType": "insert",
	},
}
