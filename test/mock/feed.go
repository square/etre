// Copyright 2017, Square, Inc.

package mock

import (
	"github.com/square/etre/feed"

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
	MakeFunc func(*websocket.Conn) feed.Feed
}

func (ff *FeedFactory) Make(wsConn *websocket.Conn) feed.Feed {
	if ff.MakeFunc != nil {
		return ff.MakeFunc(wsConn)
	}
	return nil
}
