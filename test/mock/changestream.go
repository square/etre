package mock

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/square/etre"
)

type ChangeStreamServer struct {
	WatchFunc func() (<-chan etre.CDCEvent, error)
	CloseFunc func(<-chan etre.CDCEvent)
	RunFunc   func() error
	StopFunc  func()
}

func (s ChangeStreamServer) Watch() (<-chan etre.CDCEvent, error) {
	if s.WatchFunc != nil {
		return s.WatchFunc()
	}
	return nil, nil
}

func (s ChangeStreamServer) Close(c <-chan etre.CDCEvent) {
	if s.CloseFunc != nil {
		s.CloseFunc(c)
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
