// Copyright 2017, Square, Inc.

package mock

import (
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
