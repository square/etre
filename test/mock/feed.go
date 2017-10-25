// Copyright 2017, Square, Inc.

package mock

import (
	"github.com/square/etre"
	"github.com/square/etre/cdc"

	"github.com/gorilla/websocket"
)

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
	MakeWSFunc   func(*websocket.Conn) cdc.Feed
	MakeChanFunc func(int64, int) (cdc.Feed, <-chan etre.CDCEvent)
}

func (ff *FeedFactory) MakeWS(wsConn *websocket.Conn) cdc.Feed {
	if ff.MakeWSFunc != nil {
		return ff.MakeWSFunc(wsConn)
	}
	return nil
}

func (ff *FeedFactory) MakeChan(startTs int64, clientBufferSize int) (cdc.Feed, <-chan etre.CDCEvent) {
	if ff.MakeChanFunc != nil {
		return ff.MakeChanFunc(startTs, clientBufferSize)
	}
	return nil, nil
}
