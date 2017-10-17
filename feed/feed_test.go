// Copyright 2017, Square, Inc.

package feed_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/square/etre"
	"github.com/square/etre/feed"
	"github.com/square/etre/test/mock"

	"github.com/go-test/deep"
	"github.com/gorilla/websocket"
)

func TestFeed(t *testing.T) {
	// Setup a websocket handler to handle the initial low-level ws connection
	// and start a feed. After the feed is started we can test it by sending
	// it control messages and validating what it sends back.
	producerEvents := make(chan etre.CDCEvent)
	wsHandler := func(w http.ResponseWriter, r *http.Request) {
		// Upgrade to a WebSocket connection.
		var upgrader = websocket.Upgrader{}
		wsConn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatal(err)
			return
		}

		// Create a mock producer and factory.
		p := &mock.Producer{
			StartFunc: func(startTs int64, chunkWindow int) <-chan etre.CDCEvent {
				return producerEvents
			},
			ErrorFunc: func() error {
				return mock.ErrProducer
			},
		}
		pf := &mock.ProducerFactory{
			MakeFunc: func(id string) feed.Producer {
				return p
			},
		}

		// Create a feed.
		f := feed.NewFeed(wsConn, pf)
		// Stopping a feed that hasn't started shouldn't do anything.
		f.Stop()
		// Wait does nothing if the feed hasn't started.
		f.Wait()
		// Start the feed.
		f.Start()
		// Starting a feed that was already started shouldn't do anything.
		f.Start()

		// Wait for the feed to finish.
		f.Wait()
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

	startTs := time.Now()

	//
	// Send feed server a ping control message (client -> server ping)
	//

	ping := map[string]interface{}{
		"control": "ping",
		"srcTs":   startTs.UnixNano(),
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
		if n <= startTs.UnixNano() {
			t.Errorf("got ts %s <= sent ts %d, expected it to be greater", n, startTs.UnixNano())
		}
	}

	//
	// Send feed server a start control message (clinet -> server start)
	//

	start := map[string]interface{}{
		"control": "start",
		"startTs": startTs.UnixNano(), // @todo: FIX THIS
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
	// Make the feed producer produce events, and verify they get sent through the ws to the client.
	//

	events := []etre.CDCEvent{
		etre.CDCEvent{EventId: "abc"},
		etre.CDCEvent{EventId: "def"},
		etre.CDCEvent{EventId: "ghi"},
	}
	for _, event := range events {
		producerEvents <- event
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
	// Close the producer's event channel, which will cause the feed to error out and stop.
	//

	close(producerEvents)
	var errControl map[string]interface{}
	if err := clientConn.ReadJSON(&errControl); err != nil {
		t.Fatal(err)
		return
	}
	if errControl["control"] != "error" {
		t.Errorf("wrong control reply '%s', expected 'error'", errControl["control"])
	}
	if errControl["error"] != mock.ErrProducer.Error() {
		t.Errorf("err = %s, expected %s", errControl["error"], mock.ErrProducer)
	}
}
