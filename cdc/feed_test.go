// Copyright 2017, Square, Inc.

package cdc_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/square/etre"
	"github.com/square/etre/cdc"
	"github.com/square/etre/test/mock"

	"github.com/go-test/deep"
	"github.com/gorilla/websocket"
)

func TestWSFeed(t *testing.T) {
	// Setup a websocket handler to handle the initial low-level ws connection
	// and start a feed. After the feed is started we can test it by sending
	// it control messages and validating what it sends back.
	pollerEvents := make(chan etre.CDCEvent)
	wsHandler := func(w http.ResponseWriter, r *http.Request) {
		// Upgrade to a WebSocket connection.
		var upgrader = websocket.Upgrader{}
		wsConn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatal(err)
			return
		}

		// Create a mock Poller.
		p := &mock.Poller{
			RegisterFunc: func(string) (<-chan etre.CDCEvent, int64, error) {
				return pollerEvents, 0, nil
			},
		}

		// Create a feed.
		f := cdc.NewWebsocketFeed(wsConn, p, &mock.CDCStore{})

		// Run the feed.
		f.Run()
	}
	ts := httptest.NewServer(http.HandlerFunc(wsHandler))
	defer ts.Close()

	// Create a client that we can use to send messages to the feed. We will also
	// use this client to validate the messages that the feed sends back.
	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Error(err)
	}
	wsUrl := fmt.Sprintf("ws://%s", u.Host)
	clientConn, _, err := websocket.DefaultDialer.Dial(wsUrl, nil)
	if err != nil {
		t.Errorf("%s, %s", ts.URL, err)
	}

	srcTs := time.Now()

	//
	// Send feed server a ping control message (client -> server ping)
	//

	ping := map[string]interface{}{
		"control": "ping",
		"srcTs":   srcTs.UnixNano(),
	}
	if err := clientConn.WriteJSON(ping); err != nil {
		t.Fatal(err)
		return
	}
	var pong map[string]interface{}
	if err := clientConn.ReadJSON(&pong); err != nil {
		t.Fatal(err)
		return
	}
	if pong["control"] != "pong" {
		t.Errorf("wrong control reply '%s', expected 'pong'", pong["control"])
	}
	dstTs, ok := pong["dstTs"]
	if !ok {
		t.Errorf("dstTs not set in ping reply, expected a UnixNano value")
	} else {
		// Go JSON makes all numbers float64, so convert to that first,
		// then int64 for UnixNano.
		n := int64(dstTs.(float64))
		if n <= srcTs.UnixNano() {
			t.Errorf("got ts %s <= sent ts %d, expected it to be greater", n, srcTs.UnixNano())
		}
	}

	//
	// Send feed server a start control message (clinet -> server start)
	//

	startTs := 1
	start := map[string]interface{}{
		"control": "start",
		"startTs": startTs,
	}
	if err := clientConn.WriteJSON(start); err != nil {
		t.Fatal(err)
		return
	}
	var ack map[string]interface{}
	if err := clientConn.ReadJSON(&ack); err != nil {
		t.Fatal(err)
		return
	}
	if ack["control"] != "start" {
		t.Errorf("wrong control reply '%s', expected 'start'", ack["control"])
	}
	if e, ok := ack["error"]; ok {
		if e != "" {
			t.Errorf("got an error in the ack respons of '%s', expected no error", e)
		}
	}

	//
	// Make the poller produce events, which will cause the streamer to send them to the feed.
	// Then verify the events get sent through the ws to the client.
	//

	events := []etre.CDCEvent{
		etre.CDCEvent{EventId: "abc", Ts: 2}, // these timestamps have to be greater than startTs
		etre.CDCEvent{EventId: "def", Ts: 3},
		etre.CDCEvent{EventId: "ghi", Ts: 4},
	}
	for _, event := range events {
		pollerEvents <- event
	}

	var recvdEvents []etre.CDCEvent
	for i := 0; i < len(events); i++ {
		var recvdEvent etre.CDCEvent
		if err := clientConn.ReadJSON(&recvdEvent); err != nil {
			t.Fatal(err)
			return
		}
		recvdEvents = append(recvdEvents, recvdEvent)
	}
	if diff := deep.Equal(recvdEvents, events); diff != nil {
		t.Error(diff)
	}

	//
	// Close the pollers's event channel, which will cause the streamer to stop which will then
	// cause the feed to error out and stop.
	//

	close(pollerEvents)
	var errControl map[string]interface{}
	if err := clientConn.ReadJSON(&errControl); err != nil {
		t.Fatal(err)
		return
	}

	// Close the client wsConn (this is what the real client will do in the case of an error).
	clientConn.Close()

	if errControl["control"] != "error" {
		t.Errorf("wrong control reply '%s', expected 'error'", errControl["control"])
	}
	if errControl["error"] != cdc.ErrStreamerLag.Error() {
		t.Errorf("err = %s, expected %s", errControl["error"], cdc.ErrStreamerLag)
	}
}

func TestChanFeed(t *testing.T) {
	pollerEvents := make(chan etre.CDCEvent)
	// Create a mock Poller.
	p := &mock.Poller{
		RegisterFunc: func(string) (<-chan etre.CDCEvent, int64, error) {
			return pollerEvents, 0, nil
		},
	}

	// Create a feed.
	f := cdc.NewInternalFeed(10, p, &mock.CDCStore{})

	startTs := int64(1)
	feedChan := f.Start(startTs)

	//
	// Make the poller produce events, which will cause the streamer to send them to the feed.
	// Then verify the events get sent through the channel to the client.
	//

	events := []etre.CDCEvent{
		etre.CDCEvent{EventId: "abc", Ts: 2}, // these timestamps have to be greater than startTs
		etre.CDCEvent{EventId: "def", Ts: 3},
		etre.CDCEvent{EventId: "ghi", Ts: 4},
	}
	for _, event := range events {
		pollerEvents <- event
	}

	var recvdEvents []etre.CDCEvent
	for i := 0; i < len(events); i++ {
		recvdEvent := <-feedChan
		recvdEvents = append(recvdEvents, recvdEvent)
	}
	if diff := deep.Equal(recvdEvents, events); diff != nil {
		t.Error(diff)
	}

	//
	// Close the pollers's event channel, which will cause the streamer to stop which will then
	// cause the feed to error out and stop.
	//

	close(pollerEvents)
	// Give client a few milliseconds to shutdown
	time.Sleep(500 * time.Millisecond)

	if f.Error() != cdc.ErrStreamerLag {
		t.Errorf("err = %s, expected %s", f.Error(), cdc.ErrStreamerLag)
	}
}

func TestChanFeedCallerBlocked(t *testing.T) {
	pollerEvents := make(chan etre.CDCEvent, 10)
	maxPolledTs := int64(1)
	// Create a mock Poller.
	p := &mock.Poller{
		RegisterFunc: func(string) (<-chan etre.CDCEvent, int64, error) {
			return pollerEvents, maxPolledTs, nil
		},
	}

	// Create a feed.
	f := cdc.NewInternalFeed(1, p, &mock.CDCStore{}) // IMPORTANT: channel buffer size of 1

	startTs := int64(5) // must be greater than maxPolledTimestamp
	f.Start(startTs)

	//
	// Make the poller produce events, which will cause the streamer to send them to the feed.
	// This will cause the feed's channel to fill up and close with an error.
	//

	events := []etre.CDCEvent{
		etre.CDCEvent{EventId: "abc", Ts: 9}, // these timestamps have to be greater than startTs
		etre.CDCEvent{EventId: "def", Ts: 10},
		etre.CDCEvent{EventId: "ghi", Ts: 11},
	}
	for _, event := range events {
		select {
		case pollerEvents <- event:
		default:
		}
	}

	// Give client a few milliseconds to shutdown
	time.Sleep(500 * time.Millisecond)

	if f.Error() != etre.ErrCallerBlocked {
		t.Errorf("err = %s, expected %s", f.Error(), etre.ErrCallerBlocked)
	}
}
