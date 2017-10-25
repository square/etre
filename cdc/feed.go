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
// Feed Factory
// ////////////////////////////////////////////////////////////////////////////

// A FeedFactory creates feeds. A feed produces a change feed of Change Data
// Capture (CDC) events for all entity types. Feeds are used by clients to
// stream events as they happen within Etre. Each client has its own feed.
type FeedFactory interface {
	// MakeWebsocket makes a websocket feed.
	MakeWebsocket(*websocket.Conn) *WebsocketFeed
	// MakeInternal makes an internal feed.
	MakeInternal(bufferSize int) *InternalFeed
}

type feedFactory struct {
	poller Poller
	cdcs   Store
}

func (ff *feedFactory) MakeWebsocket(wsConn *websocket.Conn) *WebsocketFeed {
	return NewWebsocketFeed(wsConn, ff.poller, ff.cdcs)
}

func (ff *feedFactory) MakeInternal(bufferSize int) *InternalFeed {
	return NewInternalFeed(bufferSize, ff.poller, ff.cdcs)
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

// WebsocketFeed represents a feed that works over a websocket. Call Run on a
// feed to make it listen to control messages on the websocket. When the feed
// receives a "start" control message, it begins sending events over the
// websocket. If the feed encounters an error it attempts to send an "error"
// control message over the websocket before closing.
type WebsocketFeed struct {
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

// NewWebsocketSFeed creates a feed that works on a given websocket.
func NewWebsocketFeed(wsConn *websocket.Conn, poller Poller, cdcs Store) *WebsocketFeed {
	id := xid.New().String()
	return &WebsocketFeed{
		wsConn:   wsConn,
		streamer: newStreamer(poller, cdcs, id), // make a streamer with the feed's id
		id:       id,
		wsMutex:  &sync.Mutex{},
		Mutex:    &sync.Mutex{},
		logger:   log.WithFields(log.Fields{"feedId": id}),
	}
}

// Run makes a feed listen to messages on the websocket. The feed won't do
// anything until it receives a control message on the websocket. Run is a
// blocking operation that only returns if it encounters an error. Calling
// Run on a running feed retruns ErrFeedAlreadyRunning. Calling Run on a
// feed that has stopped returns ErrWebsocketConnect.
func (f *WebsocketFeed) Run() error {
	f.Lock()
	f.logger.Info("running feed")

	if f.wsConn == nil {
		f.Unlock()
		return ErrWebsocketConnect
	}

	if f.running {
		f.Unlock()
		return ErrFeedAlreadyRunning
	}
	f.running = true

	f.Unlock()
	return f.recv()
}

// ------------------------------------------------------------------------- //

// recv receives control messages until there's an error or the client calls Stop.
func (f *WebsocketFeed) recv() error {
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
func (f *WebsocketFeed) send(v interface{}) error {
	f.wsMutex.Lock()
	defer f.wsMutex.Unlock()
	f.wsConn.SetWriteDeadline(time.Now().Add(time.Duration(etre.CDC_WRITE_TIMEOUT) * time.Second))
	if err := f.wsConn.WriteJSON(v); err != nil {
		return fmt.Errorf("f.wsConn.WriteJSON: %s", err)
	}
	return nil
}

// control handles a control message from the client.
func (f *WebsocketFeed) control(msg map[string]interface{}, now time.Time) error {
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
func (f *WebsocketFeed) runStreamer(startTs int64) {
	// Start the streamer, and send all events it produces out through the
	// feed. The streamer runs forever until it is stopped or unitl it
	// encounters an error, at which point it closes the events channel.
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
		// If the streamer was stopped, no new events are sent on the
		// feed. Therefore we must close the feed with an error letting
		// the client know.
		f.handleErr(ErrStreamerStopped)
		return
	}
}

// handleErr sends error control messages over the feed. If it fails to send
// a message, it stops the feed entirely.
func (f *WebsocketFeed) handleErr(e error) {
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
func (f *WebsocketFeed) stop() {
	f.Lock()
	defer f.Unlock()
	f.logger.Info("stopping feed")

	f.running = false
	if f.wsConn != nil {
		f.wsConn.Close()
	}
}

// ////////////////////////////////////////////////////////////////////////////
// Internal Feed Implementation
// ////////////////////////////////////////////////////////////////////////////

// InternalFeed represents a feed that works on a channel. It is used by
// internal components in Etre that need access to a feed of CDC events
// (currently it is only used in tests). Call Start on a feed to start it and
// to get access to the channel that the feed sends events on.
type InternalFeed struct {
	events     chan etre.CDCEvent // the channel that the feed sends events on
	bufferSize int                // the buffer size of the channel that NewInternalFeed returns
	streamer   streamer           // streams events for the feed
	// --
	id              string        // id for this feed
	streamerStarted bool          // true once the streamer has been started
	started         bool          // true while the feed is running
	stopped         bool          // true after the feed is stopped
	stopChan        chan struct{} // channel that gets closed when Stop is called
	err             error         // last error in runStreamer()
	*sync.Mutex                   // guard function calls
	logger          *log.Entry    // used for logging
}

// NewInternalFeed creates a feed that works on a channel.
//
// bufferSize causes Start to create and return a buffered feed channel. A value
// of 10 is reasonable. If the channel blocks, it is closed and Error returns
// ErrCallerBlocked.
func NewInternalFeed(bufferSize int, poller Poller, cdcs Store) *InternalFeed {
	id := xid.New().String()
	return &InternalFeed{
		bufferSize: bufferSize,
		streamer:   newStreamer(poller, cdcs, id), // make a streamer with the feed's id
		id:         id,
		Mutex:      &sync.Mutex{},
		logger:     log.WithFields(log.Fields{"feedId": id}),
	}
}

// Start starts a feed from a given timestamp. It returns a feed channel on
// which the caller can receive CDC events for as long as the feed is running.
// Calling Start again returns the same feed channel if already started. On
// error or when Stop is called, the feed channel is closed and Error returns
// the error. A feed cannot be restarted once it has stopped.
func (f *InternalFeed) Start(startTs int64) <-chan etre.CDCEvent {
	f.Lock()
	defer f.Unlock()
	f.logger.Info("starting feed")

	if f.stopped {
		f.logger.Info("feed already stoped")
		return f.events
	}
	if f.started {
		f.logger.Info("feed already started")
		return f.events
	}

	f.started = true
	f.events = make(chan etre.CDCEvent, f.bufferSize)
	f.stopChan = make(chan struct{})

	go f.runStreamer(startTs)

	return f.events
}

// Stop stops the feed and closes the feed channel returned by Start. It is
// safe to call multiple times.
func (f *InternalFeed) Stop() {
	f.Lock()
	defer f.Unlock()
	f.logger.Info("stopping feed")

	if !f.started {
		f.logger.Info("feed not running")
		return
	}
	if f.stopped {
		f.logger.Info("feed already stopped")
		return
	}

	f.stopped = true

	// Close the stop channel, but do NOT close the events channel. The
	// events channel must be closed by runStreamer() to avoid sending
	// on a closed channel.
	close(f.stopChan)
}

// Error returns the error that caused the feed channel to be closed.
func (f *InternalFeed) Error() error {
	f.Lock()
	defer f.Unlock()
	return f.err
}

// ------------------------------------------------------------------------- //

func (f *InternalFeed) runStreamer(startTs int64) {
	// Start the streamer, and send all events it produces out through the
	// feed. The streamer runs forever until it is stopped or unitl it
	// encounters an error, at which point it closes the events channel.
	streamedEvents := f.streamer.Start(startTs)
	for {
		// Check to see if stop was called.
		select {
		case <-f.stopChan:
			close(f.events)
			return
		default:
		}

		// Get an event from the channel unless it has been closed.
		event, ok := <-streamedEvents
		if !ok {
			break
		}

		// Send the event out through the feed's channel. If the send
		// is blocked, save the error.
		select {
		case f.events <- event:
		default:
			f.Lock()
			f.streamer.Stop()
			f.err = etre.ErrCallerBlocked
			close(f.events)
			f.Unlock()
			return
		}
	}

	// Check the error from the streamer. If there was none, it must have
	// been stopped. Regardless, bail out.
	f.Lock()
	defer f.Unlock()
	err := f.streamer.Error()
	if err != nil {
		f.err = err
	} else {
		// If the streamer was stopped, no new events are sent on the
		// feed. Therefore we must close the feed with an error letting
		// the client know.
		f.err = ErrStreamerStopped
	}
	close(f.events)
	return
}
