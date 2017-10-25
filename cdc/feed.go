// Copyright 2017, Square, Inc.

package cdc

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/square/etre"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/websocket"
	"github.com/rs/xid"
)

const (
	// Interval at which feed components emit log lines.
	DEFAULT_LOG_INTERVAL = 30 // seconds
)

var (
	ErrWebsocketConnect   = errors.New("cannot connect to websocket")
	ErrFeedAlreadyRunning = errors.New("feed already running")
	ErrChannelClosed      = errors.New("feed channel is closed")
	ErrStreamerStopped    = errors.New("feed event streamer was stopped")
)

// This allows tests to set the time.
var CurrentTimestamp func() int64 = func() int64 { return time.Now().UnixNano() / int64(time.Millisecond) }

// ////////////////////////////////////////////////////////////////////////////
// Feed Interface and Factory
// ////////////////////////////////////////////////////////////////////////////

// A Feed produces a change feed of Change Data Capture (CDC) events for all
// entity types. Feeds are meant to be used by clients to stream events as they
// happen within Etre. Each client will have its own feed.
type Feed interface {
	// Run runs a feed. It is a blocking operation that will only return
	// if it encounters an error. Run can only be called once on a feed.
	Run() error
}

type FeedFactory interface {
	// MakeWS makes a websocket feed.
	MakeWS(*websocket.Conn) Feed
	// MakeChan makes a channel feed.
	MakeChan(startTs int64, bufferSize int) (Feed, <-chan etre.CDCEvent)
}

type feedFactory struct {
	poller Poller
	cdcs   Store
}

func (ff *feedFactory) MakeWS(wsConn *websocket.Conn) Feed {
	return NewWSFeed(wsConn, ff.poller, ff.cdcs)
}

func (ff *feedFactory) MakeChan(startTs int64, bufferSize int) (Feed, <-chan etre.CDCEvent) {
	return NewChanFeed(startTs, bufferSize, ff.poller, ff.cdcs)
}

func NewFeedFactory(poller Poller, cdcs Store) FeedFactory {
	return &feedFactory{
		poller: poller,
		cdcs:   cdcs,
	}
}

// ////////////////////////////////////////////////////////////////////////////
// Websocket Feed Implementation
// ////////////////////////////////////////////////////////////////////////////

// wsFeed implements the Feed interface over a websocket.
type wsFeed struct {
	wsConn   *websocket.Conn // the websocket connection to the client
	streamer streamer        // streams events for the feed
	// --
	id              string      // id for this feed
	streamerStarted bool        // true once the streamer has been started
	running         bool        // true while the feed is running
	*sync.Mutex                 // guard function calls
	wsMutex         *sync.Mutex // guard ws send/write
	logger          *log.Entry  // used for logging
}

// NewWSFeed creates a feed that works on a given websocket. Call Run after the
// feed is created to make it listen to messages on the websocket. When the
// feed receives a "start" control message, it will begin sending events over
// the websocket. If the feed encounters an error it will attempt to send an
// "error" control message over the websocket before closing it.
func NewWSFeed(wsConn *websocket.Conn, poller Poller, cdcs Store) Feed {
	id := xid.New().String()
	return &wsFeed{
		wsConn:   wsConn,
		streamer: newStreamer(poller, cdcs, id), // make a streamer with the feed's id
		id:       id,
		wsMutex:  &sync.Mutex{},
		Mutex:    &sync.Mutex{},
		logger:   log.WithFields(log.Fields{"feedId": id}),
	}
}

func (f *wsFeed) Run() error {
	f.Lock()
	f.logger.Info("running feed")

	if f.wsConn == nil {
		return ErrWebsocketConnect
	}

	if f.running {
		return ErrFeedAlreadyRunning
	}
	f.running = true

	f.Unlock()
	return f.recv()
}

// ------------------------------------------------------------------------- //

// recv receives control messages until there's an error or the client calls Stop.
func (f *wsFeed) recv() error {
	var now time.Time
	for {
		_, bytes, err := f.wsConn.ReadMessage()
		now = time.Now()
		if err != nil {
			f.handleErr(err)
			return err
		}

		// Assume it's a control message since that's all we should ever receive.
		var msg map[string]interface{}
		if err := json.Unmarshal(bytes, &msg); err != nil {
			f.handleErr(err)
			return err
		}
		if _, ok := msg["control"]; !ok {
			// This shouldn't happen.
			f.handleErr(etre.ErrBadData)
			return err
		}
		if err := f.control(msg, now); err != nil {
			f.handleErr(err)
			return err
		}
	}
}

// send sends a message on the feed via the feed's websocket.
func (f *wsFeed) send(v interface{}) error {
	f.wsMutex.Lock()
	defer f.wsMutex.Unlock()
	f.wsConn.SetWriteDeadline(time.Now().Add(time.Duration(etre.CDC_WRITE_TIMEOUT) * time.Second))
	if err := f.wsConn.WriteJSON(v); err != nil {
		return fmt.Errorf("f.wsConn.WriteJSON: %s", err)
	}
	return nil
}

// control handles a control message from the client.
func (f *wsFeed) control(msg map[string]interface{}, now time.Time) error {
	switch msg["control"] {
	case "ping":
		// Ping from client
		v, ok := msg["srcTs"]
		if ok {
			// Go JSON makes all numbers float64, so convert to that first,
			// then int64 for UnixNano.
			t0 := int64(v.(float64)) // ts sent
			t1 := now.UnixNano()     // ts recv'ed
			latency := time.Duration(t1-t0) * time.Nanosecond
			f.logger.Info("API to client latency: %s", latency)
		}
		msg["control"] = "pong"
		msg["dstTs"] = now.UnixNano()
		if err := f.send(msg); err != nil {
			return err
		}
	case "pong":
		// Pong from call to Ping
		v1, ok1 := msg["srcTs"]
		v2, ok2 := msg["dstTs"]
		if !ok1 || !ok2 {
			return fmt.Errorf("srcTs or dstTs not set in ping-ping control message: %#v", msg)
		}

		// t0 -> t1 -> now
		t0 := int64(v1.(float64)) // sent by API
		t1 := int64(v2.(float64)) // recv'ed by client
		lag := etre.Latency{
			Send: (t1 - t0) / 1000000,
			Recv: (now.UnixNano() - t1) / 1000000,
			RTT:  (now.UnixNano() - t0) / 1000000,
		}

		// @todo: do more with this
		f.logger.Info(lag)
	case "start":
		v, ok := msg["startTs"]
		var startTs int64
		if ok {
			startTs = int64(v.(float64))
		} else {
			startTs = time.Now().Unix()
		}

		// Send back an ack that we're starting the feed.
		ack := map[string]string{
			"control": "start",
			"error":   "",
		}
		if err := f.send(ack); err != nil {
			return err
		}

		go func() {
			f.Lock()
			// Do nothing if the streamer was already started.
			if f.streamerStarted {
				f.Unlock()
				return
			} else {
				f.streamerStarted = true
				f.Unlock()
				f.runStreamer(startTs)
			}
		}()
	default:
		return fmt.Errorf("client sent unknown control message: %s: %#v", msg["control"], msg)
	}
	return nil
}

// runStreamer starts the websocket feed's streamer and sends all events that
// come out of it through the websocket to the client.
func (f *wsFeed) runStreamer(startTs int64) {
	// Start the streamer, and send all events it produces out through the
	// feed. The streamer will run forever until it is stopped or unitl it
	// encounters an error, at which point it will close the events channel.
	events := f.streamer.Start(startTs)
	for {
		// Get an event from the channel unless it has been closed.
		event, ok := <-events
		if !ok {
			break
		}

		err := f.send(event)
		if err != nil {
			f.streamer.Stop()
			f.handleErr(err)
			return
		}
	}

	// Check the error from the streamer. If there was none, it must have
	// been stopped. If there was an error, bail out.
	// @todo: if the error is ErrStreamerLag, restart the streamer.
	err := f.streamer.Error()
	if err != nil {
		f.handleErr(err)
	} else {
		// If the streamer was stopped, no new events will be sent on
		// feed. Therefore we must close the feed with an error letting
		// the client know.
		f.handleErr(ErrStreamerStopped)
		return
	}
}

// handleErr sends error control messages over the feed. If it fails to send
// a message, it stops the feed entirely.
func (f *wsFeed) handleErr(e error) {
	f.logger.Errorf("error in feed: %s", e)
	msg := map[string]interface{}{
		"control": "error",
		"error":   e.Error(),
	}
	err := f.send(msg)
	if err != nil {
		f.logger.Errorf("failed to send error message on feed: %s", err)
		f.stop()
	}
}

// stop shuts down the feed if it's running.
func (f *wsFeed) stop() {
	f.Lock()
	defer f.Unlock()
	f.logger.Info("stopping feed")

	f.running = false
	if f.wsConn != nil {
		f.wsConn.Close()
	}
}

// ////////////////////////////////////////////////////////////////////////////
// Channel Feed Implementation
// ////////////////////////////////////////////////////////////////////////////

// chanFeed implements the Feed interface over a channel.
type chanFeed struct {
	events     chan etre.CDCEvent // the channel that the feed sends events on
	startTs    int64              // the timestamp to start the feed from
	bufferSize int                // the buffer size of the channel that NewChanFeed returns
	streamer   streamer           // streams events for the feed
	// --
	id              string     // id for this feed
	streamerStarted bool       // true once the streamer has been started
	running         bool       // true while the feed is running
	*sync.Mutex                // guard function calls
	logger          *log.Entry // used for logging
}

// NewChanFeed creates a feed that sends events on the returned channel. Call
// Run after the feed is created to make it start sending events on the channel.
//
// bufferSize specifies the size of the channel that is returned. A value of 10
// is reasonable. If the channel blocks, it is closed and Run returns
// ErrCallerBlocked.
//
// startTs is the timestamp that the feed will start from.
func NewChanFeed(startTs int64, bufferSize int, poller Poller, cdcs Store) (Feed, <-chan etre.CDCEvent) {
	id := xid.New().String()
	events := make(chan etre.CDCEvent, bufferSize)
	return &chanFeed{
		events:     events,
		startTs:    startTs,
		bufferSize: bufferSize,
		streamer:   newStreamer(poller, cdcs, id), // make a streamer with the feed's id
		id:         id,
		Mutex:      &sync.Mutex{},
		logger:     log.WithFields(log.Fields{"feedId": id}),
	}, events
}

func (f *chanFeed) Run() error {
	f.Lock()
	f.logger.Info("running feed")

	if f.running {
		return ErrFeedAlreadyRunning
	}
	f.running = true

	f.Unlock()

	// Start the streamer, and send all events it produces out through the
	// feed. The streamer will run forever until it is stopped or unitl it
	// encounters an error, at which point it will close the events channel.
	streamedEvents := f.streamer.Start(f.startTs)
	for {
		// Get an event from the channel unless it has been closed.
		event, ok := <-streamedEvents
		if !ok {
			break
		}

		// Send the event out through the feed's channel. If the send
		// is blocked, return an error.
		select {
		case f.events <- event:
		default:
			f.streamer.Stop()
			return etre.ErrCallerBlocked
		}
	}

	// Check the error from the streamer. If there was none, it must have
	// been stopped. If there was an error, bail out.
	err := f.streamer.Error()
	if err != nil {
		return err
	} else {
		// If the streamer was stopped, no new events will be sent on
		// feed. Therefore we must close the feed with an error letting
		// the client know.
		return ErrStreamerStopped
	}
}
