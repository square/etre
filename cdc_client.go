// Copyright 2017, Square, Inc.

package etre

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// A CDCClient consumes a change feed of Change Data Capture (CDC) events for
// all entity types. It handles all control messages. The caller should call
// Stop when done to shutdown the feed. On error or abnormal shutdown, Error
// returns the last error.
type CDCClient interface {
	// Start starts the CDC feed from the given time. On success, a feed channel
	// is returned on which the caller can receive CDC events for as long as
	// the feed remains connected. Calling Start again returns the same feed
	// channel if already started. To restart the feed, call Stop then Start
	// again. On error or abnormal shutdown, the feed channel is closed, and
	// Error returns the error.
	Start(time.Time) (<-chan CDCEvent, error)

	// Stop stops the feed and closes the feed channel returned by Start. It is
	// safe to call multiple times.
	Stop()

	// Ping pings the API and reports latency. Latency values are all zero on
	// timeout or error. On error, the feed is most likely closed.
	Ping(timeout time.Duration) Latency

	// Error returns the error that caused the feed channel to be closed. Start
	// resets the error.
	Error() error
}

// Internal implementation of CDCClient over a websocket.
type cdcClient struct {
	addr       string
	tlsConfig  *tls.Config
	bufferSize int
	dbg        bool
	// --
	*sync.Mutex             // guard function calls
	wsMutex     *sync.Mutex // guard ws send/write
	wsConn      *websocket.Conn
	events      chan CDCEvent
	err         error        // last error in recv()
	started     bool         // Start called and successful
	stopped     bool         // Stop called
	pingChan    chan Latency // for Ping
}

// NewCDCClient creates a CDC feed consumer on the given websocket address.
// addr must be ws://host:port or wss://host:port.
//
// bufferSize causes Start to create and return a buffered feed channel. A value
// of 10 is reasonable. If the channel blocks, it is closed and Error returns
// ErrCallerBlocked.
//
// Enable debug prints a lot of low-level feed/websocket logging to STDERR.
//
// The client does not automatically ping the server. The caller should run a
// separate goroutine to periodically call Ping. Every 10-60s is reasonable.
func NewCDCClient(addr string, tlsConfig *tls.Config, bufferSize int, debug bool) CDCClient {
	addr += API_ROOT + "/changes"
	c := &cdcClient{
		addr:       addr,
		tlsConfig:  tlsConfig,
		bufferSize: bufferSize,
		dbg:        debug,
		// --
		Mutex:    &sync.Mutex{},
		wsMutex:  &sync.Mutex{},
		pingChan: make(chan Latency, 1),
	}
	c.debug("addr: %s", addr)
	return c
}

func (c *cdcClient) Start(startTs time.Time) (<-chan CDCEvent, error) {
	c.debug("Start: call")
	defer c.debug("Start: return")
	c.Lock()
	defer c.Unlock()

	// If already started, return the existing event chan
	if c.started {
		c.debug("already started")
		return c.events, nil
	}

	// Connect
	u, err := url.Parse(c.addr)
	if err != nil {
		return nil, err
	}
	c.debug("connecting to %s", c.addr)
	dialer := &websocket.Dialer{
		TLSClientConfig: c.tlsConfig,
	}
	conn, resp, err := dialer.Dial(u.String(), nil)
	if err != nil {
		if resp != nil {
			defer resp.Body.Close()
			body, _ := ioutil.ReadAll(resp.Body)
			return nil, apiError(resp, body)
		}
		return nil, fmt.Errorf("websocket.DefaultDialer.Dial(%s): %s", u.String(), err)
	}
	c.wsConn = conn

	// Send start control message
	start := map[string]interface{}{
		"control": "start",
		"startTs": (startTs.UnixNano() / int64(time.Millisecond)),
		// @todo: checkpoint
	}
	c.debug("sending start")
	if err := c.send(start); err != nil {
		c.wsConn.Close()
		return nil, err
	}

	// Receive start control ack
	var ack map[string]string
	c.debug("waiting for start ack")
	if err := c.wsConn.ReadJSON(&ack); err != nil {
		c.wsConn.Close()
		return nil, fmt.Errorf("wsConn.ReadJSON: %s", err)
	}
	c.debug("start ack received: %#v", ack)
	errMsg, ok := ack["error"]
	if ok && errMsg != "" {
		return nil, fmt.Errorf("API error: %s", errMsg)
	}

	// Start consuming CDC feed
	c.debug("OK, cdc feed started")
	c.started = true
	c.stopped = false
	c.err = nil
	c.events = make(chan CDCEvent, c.bufferSize)
	go c.recv()

	return c.events, nil
}

func (c *cdcClient) Stop() {
	c.debug("Stop: call")
	defer c.debug("Stop: return")
	c.Lock()
	defer c.Unlock()
	if c.stopped {
		c.debug("already stopped")
		return
	}
	if c.wsConn != nil {
		c.wsConn.Close()
	}
	c.stopped = true
}

func (c *cdcClient) Ping(timeout time.Duration) Latency {
	c.debug("Ping: call")
	defer c.debug("Ping: return")

	// DO NOT guard this function with c.Lock(). We only need to guard ws writes,
	// and send() will do that for us.

	var lag Latency
	ping := map[string]interface{}{
		"control": "ping",
		"srcTs":   time.Now().UnixNano(),
	}
	if err := c.send(ping); err != nil {
		// A half-dead/open/close connection is detected by trying to send,
		// so an error here probably means the API went away without closing
		// the TCP connection. Receive doesn't detect this, but send does.
		c.shutdown(err)
		return lag
	}
	select {
	case lag = <-c.pingChan:
	case <-time.After(timeout):
		c.debug("ping timeout")
	}

	return lag
}

func (c *cdcClient) Error() error {
	// Need to guard this because we never know when shutdown() will write c.err
	c.Lock()
	defer c.Unlock()
	return c.err
}

// --------------------------------------------------------------------------

// Receive CDC events and control messages until there's an error or caller
// calls Stop. Control messages should be infrequent.
func (c *cdcClient) recv() {
	c.debug("recv: call")
	defer c.debug("recv: return")
	defer close(c.events)
	var now time.Time
	for {
		_, bytes, err := c.wsConn.ReadMessage()
		now = time.Now()
		if err != nil {
			c.shutdown(err)
			return
		}

		// CDC events should be the bulk of data we recv, so presume it's that.
		var e CDCEvent
		if err := json.Unmarshal(bytes, &e); err != nil {
			c.shutdown(err)
			return
		}

		// If EventId is set (not empty), then it's a CDC event as expected
		if e.EventId != "" {
			c.debug("cdc event: %#v", e)
			select {
			case c.events <- e: // send CDC event to caller
			default:
				c.debug("caller blocked")
				c.shutdown(ErrCallerBlocked)
				return
			}
		} else {
			// It's not a CDC event, so it should be a control message
			var msg map[string]interface{}
			if err := json.Unmarshal(bytes, &msg); err != nil {
				c.shutdown(err)
				return
			}
			if _, ok := msg["control"]; !ok {
				// This shouldn't happen: data is not a CDC event or a control message
				c.shutdown(ErrBadData)
				return
			}
			if err := c.control(msg, now); err != nil {
				c.shutdown(err)
				return
			}
		}
	}
}

// Handle a control message from the API. Returning an error causes the recv loop
// to shutdown.
func (c *cdcClient) control(msg map[string]interface{}, now time.Time) error {
	c.debug("control: call: %#v", msg)
	defer c.debug("control: return")

	switch msg["control"] {
	case "error":
		// API is letting us know that something on its end broke it's closing
		// the connection. This is the last data it sends.
		return fmt.Errorf("API error: %s", msg["error"].(string))
	case "checkpoint":
		// @todo
	case "ping":
		// Ping from API
		v, ok := msg["srcTs"]
		if ok {
			// Go JSON makes all numbers float64, so convert to that first,
			// then int64 for UnixNano.
			t0 := int64(v.(float64)) // ts sent
			t1 := now.UnixNano()     // ts recv'ed
			latency := time.Duration(t1-t0) * time.Nanosecond
			c.debug("API to client latency: %s", latency)
		}
		msg["control"] = "pong"
		msg["dstTs"] = now.UnixNano()
		if err := c.send(msg); err != nil {
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
		t0 := int64(v1.(float64)) // sent by client
		t1 := int64(v2.(float64)) // recv'ed by API
		lag := Latency{
			Send: (t1 - t0) / 1000000,
			Recv: (now.UnixNano() - t1) / 1000000,
			RTT:  (now.UnixNano() - t0) / 1000000,
		}
		c.debug("lag: %#v", lag)
		select {
		case c.pingChan <- lag:
		default:
			c.debug("pingChan blocked")
		}
	default:
		return fmt.Errorf("API sent unknown control message: %s: %#v", msg["control"], msg)
	}
	return nil
}

func (c *cdcClient) send(v interface{}) error {
	c.debug("send: call")
	defer c.debug("send: return")
	c.wsMutex.Lock()
	defer c.wsMutex.Unlock()
	c.wsConn.SetWriteDeadline(time.Now().Add(time.Duration(CDC_WRITE_TIMEOUT) * time.Second))
	if err := c.wsConn.WriteJSON(v); err != nil {
		return fmt.Errorf("c.wsConn.WriteJSON: %s", err)
	}
	return nil
}

// Close websocket and save error, if not already stopped gracefully.
func (c *cdcClient) shutdown(err error) {
	c.debug("shutdown: call: %v", err)
	defer c.debug("shutdown: return")
	c.Lock()
	defer c.Unlock()
	if c.stopped {
		c.debug("already stopped")
		return
	}
	if c.wsConn != nil {
		c.wsConn.Close()
	}
	c.err = err
}

func (c *cdcClient) debug(fmt string, v ...interface{}) {
	if !c.dbg {
		return
	}
	log.Printf(fmt, v...)
}

// //////////////////////////////////////////////////////////////////////////
// Mock client
// //////////////////////////////////////////////////////////////////////////

type MockCDCClient struct {
	StartFunc func(time.Time) (<-chan CDCEvent, error)
	StopFunc  func()
	PingFunc  func(time.Duration) Latency
	ErrorFunc func() error
}

func (c MockCDCClient) Start(startTs time.Time) (<-chan CDCEvent, error) {
	if c.StartFunc != nil {
		return c.StartFunc(startTs)
	}
	return nil, nil
}

func (c MockCDCClient) Stop() {
	if c.StopFunc != nil {
		c.StopFunc()
	}
	return
}

func (c MockCDCClient) Ping(timeout time.Duration) Latency {
	if c.PingFunc != nil {
		return c.PingFunc(timeout)
	}
	return Latency{}
}

func (c MockCDCClient) Error() error {
	if c.ErrorFunc != nil {
		return c.ErrorFunc()
	}
	return nil
}
