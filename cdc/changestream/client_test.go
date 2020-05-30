// Copyright 2017-2020, Square, Inc.

package changestream_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/gorilla/websocket"

	"github.com/square/etre"
	"github.com/square/etre/cdc/changestream"
	"github.com/square/etre/test/mock"
)

/*
	To test Client, we need to mock what's on both ends of it:

	  HTTP client -(ws)-> Client <- Streamer

	An HTTP client is attached to a Client via a websocket (ws) in api.changesHandler.
	The API handler also passes Client a Streamer which is its input of CDC events
	that Client sends back to the HTTP client.

	To test Client, we need a mock ws and Streamer, and each test plays the role
	of the HTTP cilent. (Normally, etre.CDCClient is the real HTTP client.)
*/

type server struct {
	*sync.Mutex
	ts            *httptest.Server
	url           string
	Client        *changestream.WebsocketClient
	clientRunning chan struct{}
	doneChan      chan struct{}
	err           error
}

var clientNo int

func setupClient(t *testing.T, streamer changestream.Streamer) *server {
	//etre.DebugEnabled = true
	server := &server{
		Mutex:         &sync.Mutex{},
		doneChan:      make(chan struct{}),
		clientRunning: make(chan struct{}),
	}
	wsHandler := func(w http.ResponseWriter, r *http.Request) {
		defer close(server.doneChan)
		var upgrader = websocket.Upgrader{}
		wsConn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatal(err)
		}
		clientNo++
		clientId := fmt.Sprintf("client%d", clientNo)
		server.Lock()
		server.Client = changestream.NewWebsocketClient(clientId, wsConn, streamer)
		server.Unlock()
		runChan := make(chan struct{})
		go func() {
			defer close(runChan)
			err = server.Client.Run()
			server.Lock()
			server.err = err
			server.Unlock()
		}()
		time.Sleep(100 * time.Millisecond)
		close(server.clientRunning)
		<-runChan
	}
	server.ts = httptest.NewServer(http.HandlerFunc(wsHandler))
	u, err := url.Parse(server.ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	server.url = fmt.Sprintf("ws://%s", u.Host)
	return server
}

// --------------------------------------------------------------------------

func TestClientWebsocketPingFromClient(t *testing.T) {
	// Test client -> server ping control message. HTTP clients should ping
	// the server (Etre) periodically.
	server := setupClient(t, mock.Stream{})
	defer server.ts.Close()

	// Connect to server.ts/wsHandler
	clientConn, _, err := websocket.DefaultDialer.Dial(server.url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()

	srcTs := time.Now()
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
		t.Fatal("dstTs not set in ping reply, expected a UnixNano value")
	}
	n := int64(dstTs.(float64)) // JSON numbers are floats, so convert to that first, then to int64
	if n <= srcTs.UnixNano() {
		t.Errorf("got ts %d <= sent ts %d, expected it to be greater", n, srcTs.UnixNano())
	}
}

func TestClientWebsocketPingToClient(t *testing.T) {
	// Test server -> client ping. Client (this test) should respond with "pong",
	// and Client (code under test) should log the latency.
	// the server (Etre) periodically.
	server := setupClient(t, mock.Stream{})
	defer server.ts.Close()

	// Connect to server.ts/wsHandler
	clientConn, _, err := websocket.DefaultDialer.Dial(server.url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()

	<-server.clientRunning

	startTs := time.Now().UnixNano()

	lagChan := make(chan etre.Latency, 1)
	go func() {
		lagChan <- server.Client.Ping(1 * time.Second)
	}()

	// This causes Send latency of ~100s because we wait this long until
	// receiving the ping below
	time.Sleep(100 * time.Millisecond)

	// Receive ping from Client
	var msg map[string]interface{}
	if err := clientConn.ReadJSON(&msg); err != nil {
		t.Fatal(err)
		return
	}
	if msg["control"] != "ping" {
		t.Errorf("wrong control '%s', expected 'ping'", msg["control"])
	}
	srcTs, ok := msg["srcTs"]
	if !ok {
		t.Fatal("srcTs not set in ping message, expected a UnixNano value")
	}
	n := int64(srcTs.(float64)) // JSON numbers are floats, so convert to that first, then to int64
	if n <= startTs {
		t.Errorf("srcTs %d <= startTs %d, expected it to be greater", n, startTs)
	}

	// Reply with pong, setting dstTs (we're the dst, Client was the src)
	dstTs := time.Now()
	msg["control"] = "pong"
	msg["dstTs"] = dstTs.UnixNano()
	if err := clientConn.WriteJSON(msg); err != nil {
		t.Fatal(err)
	}

	var gotLag etre.Latency
	select {
	case gotLag = <-lagChan:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for Client.Latency() to return")
	}
	if gotLag.RTT < 80 || gotLag.RTT > 120 {
		t.Errorf("got RTT %d, expected between 80 and 120ms", gotLag.RTT)
	}
}

func TestClientStreamer(t *testing.T) {
	// Test that client starts getting CDC events after sending "start" control message,
	// which makes the server-side Client start its Streamer
	eventsChan := make(chan etre.CDCEvent, 1)
	streamer := mock.Stream{
		StartFunc: func(sinceTs int64) <-chan etre.CDCEvent {
			return eventsChan
		},
	}
	server := setupClient(t, streamer)
	defer server.ts.Close()

	// Connect to server.ts/wsHandler
	clientConn, _, err := websocket.DefaultDialer.Dial(server.url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()

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

	// Send some events via mock.Stream which should flow through the Client
	// and to the HTTP client
	events := []etre.CDCEvent{
		etre.CDCEvent{Id: "abc", Ts: 2}, // these timestamps have to be greater than startTs
		etre.CDCEvent{Id: "def", Ts: 3},
		etre.CDCEvent{Id: "ghi", Ts: 4},
	}
	for _, event := range events {
		eventsChan <- event // when Client called Streamer.Start(), it got this chan
	}

	var gotEvents []etre.CDCEvent
	for i := 0; i < len(events); i++ {
		var recvdEvent etre.CDCEvent
		if err := clientConn.ReadJSON(&recvdEvent); err != nil {
			t.Fatal(err)
			return
		}
		gotEvents = append(gotEvents, recvdEvent)
	}
	if diff := deep.Equal(gotEvents, events); diff != nil {
		t.Error(diff)
	}

	// Client <- Streamer is one way, so when something happens in Streamer,
	// it closes the chan it returned to Client (eventsChan) to signal to the
	// Client to shut down.
	close(eventsChan)
	var errControl map[string]interface{}
	if err := clientConn.ReadJSON(&errControl); err != nil {
		t.Fatal(err)
	}
	if errControl["control"] != "error" {
		t.Errorf("wrong control reply '%s', expected 'error'", errControl["control"])
	}

	// Wait for server/wsHandler to return, then check err from Client.Run().
	// It should be ErrWebsocketClosed because closing the stream chan (eventsChan)
	// causes a clean shutdown which closes the websocket.
	<-server.doneChan
	server.Lock()
	gotErr := server.err
	server.Unlock()
	if gotErr != changestream.ErrWebsocketClosed {
		t.Errorf("got error %T, expected ErrWebsocketClosed", gotErr)
	}
}

func TestClientInvalidMessageType(t *testing.T) {
	// Test that client returns an error control message if given an invalid message
	eventsChan := make(chan etre.CDCEvent, 1)
	streamer := mock.Stream{
		StartFunc: func(sinceTs int64) <-chan etre.CDCEvent {
			return eventsChan
		},
	}
	server := setupClient(t, streamer)
	defer server.ts.Close()

	// Connect to server.ts/wsHandler
	clientConn, _, err := websocket.DefaultDialer.Dial(server.url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()

	// Messages are supposed to be map[string]interface{}, so []int is invalid
	invalidMessage := []int{1, 2, 3}
	if err := clientConn.WriteJSON(invalidMessage); err != nil {
		t.Fatal(err)
		return
	}

	var errControl map[string]interface{}
	if err := clientConn.ReadJSON(&errControl); err != nil {
		t.Fatal(err)
	}
	if errControl["control"] != "error" {
		t.Errorf("wrong control reply '%s', expected 'error'", errControl["control"])
	}
}

func TestClientInvalidMessageContent(t *testing.T) {
	// Test that client returns error control message if given a valid message
	// struct that doesn't have "control"
	eventsChan := make(chan etre.CDCEvent, 1)
	streamer := mock.Stream{
		StartFunc: func(sinceTs int64) <-chan etre.CDCEvent {
			return eventsChan
		},
	}
	server := setupClient(t, streamer)
	defer server.ts.Close()

	// Connect to server.ts/wsHandler
	clientConn, _, err := websocket.DefaultDialer.Dial(server.url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()

	// This would be valid except "control": "ping" is missing
	ping := map[string]interface{}{
		"srcTs": time.Now().UnixNano(),
	}
	if err := clientConn.WriteJSON(ping); err != nil {
		t.Fatal(err)
		return
	}

	var errControl map[string]interface{}
	if err := clientConn.ReadJSON(&errControl); err != nil {
		t.Fatal(err)
	}
	if errControl["control"] != "error" {
		t.Errorf("wrong control reply '%s', expected 'error'", errControl["control"])
	}
}

func TestClientClose(t *testing.T) {
	// Test that Client stops nicely if the websocket client goes away unexpectedly.
	eventsChan := make(chan etre.CDCEvent, 1)
	streamer := mock.Stream{
		StartFunc: func(sinceTs int64) <-chan etre.CDCEvent {
			return eventsChan
		},
	}
	server := setupClient(t, streamer)
	defer server.ts.Close()

	// Connect to server.ts/wsHandler
	clientConn, _, err := websocket.DefaultDialer.Dial(server.url, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Start streamer so that goroutine is running
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
		t.Fatalf("wrong control reply '%s', expected 'start'", ack["control"])
	}

	time.Sleep(200 * time.Millisecond)

	clientConn.Close()

	<-server.doneChan
	server.Lock()
	gotErr := server.err
	server.Unlock()
	if gotErr == nil || gotErr == changestream.ErrWebsocketClosed {
		t.Errorf("error nil or changestream.ErrWebsocketClosed, expected websocket.CloseError or other")
	}
}
