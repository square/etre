// Copyright 2017, Square, Inc.

// Package feed provides interfaces for managing long-running event feeds
// between an Etre instance and a client.
package feed

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/square/etre"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/websocket"
	"github.com/satori/go.uuid"
)

const (
	DEFAULT_CHUNK_WINDOW = 3600 // seconds
)

// This allows tests to set the time.
var CurrentTimestamp func() int64 = func() int64 { return time.Now().UnixNano() / int64(time.Millisecond) }

// A Feed produces a change feed of Change Data Capture (CDC) events for all
// entity types. Feeds are meant to be used by clients to stream events as they
// happen within Etre. Each client will have its own feed.
type Feed interface {
	// Start makes the feed start listening for control messages. Start
	// doesn't actually cause the Feed to begin producing events...that
	// will only happen if the feed's client sends a control message
	// telling the Feed to do that.
	Start()

	// Wait blocks while the feed is running.
	Wait()

	// Stop stops the Feed from producing events and closes the Feed's
	// connection to its client. Once a Feed has been stopped it cannot
	// be started again.
	Stop()
}

// feed implements the Feed interface over a websocket.
type feed struct {
	wsConn   *websocket.Conn // the websocket connection to the client
	producer Producer        // the producer that will produce events for the feed
	// --
	id              string        // UUID for this feed
	producerStarted bool          // true once the producer has been started
	running         bool          // true while the feed is running
	stopChan        chan struct{} // channel that gets closed when Stop is called
	*sync.Mutex                   // guard function calls
	wsMutex         *sync.Mutex   // guard ws send/write
	logger          *log.Entry    // used for logging
}

// NewFeed creates a feed with the given websocket. It also takes a producer
// factory as an argument, which is used to create a Producer for the feed.
func NewFeed(wsConn *websocket.Conn, pf ProducerFactory) Feed {
	id := uuid.NewV4().String()
	return &feed{
		wsConn:   wsConn,
		producer: pf.Make(id), // make a producer with the feed's id
		id:       id,
		wsMutex:  &sync.Mutex{},
		Mutex:    &sync.Mutex{},
		logger:   log.WithFields(log.Fields{"feedId": id}),
	}
}

type FeedFactory interface {
	Make(*websocket.Conn) Feed
}

type feedFactory struct {
	pf ProducerFactory
}

func (ff *feedFactory) Make(wsConn *websocket.Conn) Feed {
	return NewFeed(wsConn, ff.pf)
}

func NewFeedFactory(pf ProducerFactory) FeedFactory {
	return &feedFactory{
		pf: pf,
	}
}

func (f *feed) Start() {
	f.Lock()
	defer f.Unlock()
	f.logger.Info("starting feed")

	if f.wsConn == nil {
		f.logger.Info("cannot start a previously stopped feed")
		return
	}

	if f.running {
		f.logger.Info("feed already running")
		return
	}

	f.running = true
	f.stopChan = make(chan struct{})

	go f.recv()
}

func (f *feed) Wait() {
	f.Lock()
	if !f.running {
		f.Unlock()
		return
	}
	f.Unlock()

	<-f.stopChan
}

func (f *feed) Stop() {
	f.stop()
}

// ------------------------------------------------------------------------- //

// recv receives control messages until there's an error or the client calls Stop.
func (f *feed) recv() {
	var now time.Time
	for {
		_, bytes, err := f.wsConn.ReadMessage()
		now = time.Now()
		if err != nil {
			f.handleErr(err)
			return
		}

		// Assume it's a control message since that's all we should ever receive.
		var msg map[string]interface{}
		if err := json.Unmarshal(bytes, &msg); err != nil {
			f.handleErr(err)
			return
		}
		if _, ok := msg["control"]; !ok {
			// This shouldn't happen.
			f.handleErr(etre.ErrBadData)
			return
		}
		if err := f.control(msg, now); err != nil {
			f.handleErr(err)
			return
		}
	}
}

// send sends a message on the feed via the feed's websocket.
func (f *feed) send(v interface{}) error {
	f.wsMutex.Lock()
	defer f.wsMutex.Unlock()
	f.wsConn.SetWriteDeadline(time.Now().Add(time.Duration(etre.CDC_WRITE_TIMEOUT) * time.Second))
	if err := f.wsConn.WriteJSON(v); err != nil {
		return fmt.Errorf("f.wsConn.WriteJSON: %s", err)
	}
	return nil
}

// control handles a control message from the client.
func (f *feed) control(msg map[string]interface{}, now time.Time) error {
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

		v, ok = msg["chunkWindow"]
		var chunkWindow int
		if ok {
			chunkWindow = int(v.(float64))
		} else {
			chunkWindow = DEFAULT_CHUNK_WINDOW
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
			defer f.Unlock()
			// Do nothing if the producer was already started.
			if f.producerStarted {
				return
			} else {
				f.producerStarted = true
				f.startProducer(startTs, chunkWindow)
			}
		}()
	default:
		return fmt.Errorf("client sent unknown control message: %s: %#v", msg["control"], msg)
	}
	return nil
}

// startProducer starts the feed's producer and sends all events that come out of it
// through the websocket to the client.
func (f *feed) startProducer(startTs int64, chunkWindow int) {
	// Start the producer, and send all events it produces out through the
	// feed. The producer will run forever until it is stopped or unitl it
	// encounters an error, at which point it will close the events channel.
	events := f.producer.Start(startTs, chunkWindow)
	for {
		// If the feed was stopped, stop the producer and return.
		select {
		case <-f.stopChan:
			f.producer.Stop()
			return
		default:
		}

		// Get an event from the channel unless it has been closed.
		event, ok := <-events
		if !ok {
			break
		}

		err := f.send(event)
		if err != nil {
			f.producer.Stop()
			f.handleErr(err)
			return
		}
	}

	// Check the error from the producer. If there was none, it must have
	// been stopped. If there was an error, bail out.
	// @todo: if the error is ErrProducerLag, restart the producer.
	err := f.producer.Error()
	if err != nil {
		f.handleErr(err)
	} else {
		// If the producer was stopped, no new events will be sent on
		// feed. Therefore we must close the feed with an error letting
		// the client know.
		f.handleErr(fmt.Errorf("CDC event producer was stopped"))
		return
	}
}

// stop shuts down the feed if it's running.
func (f *feed) stop() {
	f.Lock()
	defer f.Unlock()
	f.logger.Info("stopping feed")

	if !f.running {
		f.logger.Info("feed not running")
		return
	}

	close(f.stopChan)
	f.running = false
	if f.wsConn != nil {
		f.wsConn.Close()
	}
}

// handleErr sends error control messages over the feed. If it fails to send
// a message, it stops the feed entirely.
func (f *feed) handleErr(e error) {
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
