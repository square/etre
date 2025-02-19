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

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
		require.NoError(t, err)

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
	require.NoError(t, err)

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
	require.NoError(t, err)
	defer clientConn.Close()

	srcTs := time.Now()
	ping := map[string]interface{}{
		"control": "ping",
		"srcTs":   srcTs.UnixNano(),
	}
	err = clientConn.WriteJSON(ping)
	require.NoError(t, err)

	var pong map[string]interface{}
	err = clientConn.ReadJSON(&pong)
	require.NoError(t, err)

	assert.Equal(t, "pong", pong["control"])
	dstTs, ok := pong["dstTs"]
	require.True(t, ok, "dstTs not set in ping reply, expected a UnixNano value")
	n := int64(dstTs.(float64)) // JSON numbers are floats, so convert to that first, then to int64
	assert.True(t, n > srcTs.UnixNano(), "dstTs %d <= srcTs %d, expected it to be greater", n, srcTs.UnixNano())
}

func TestClientWebsocketPingToClient(t *testing.T) {
	// Test server -> client ping. Client (this test) should respond with "pong",
	// and Client (code under test) should log the latency.
	// the server (Etre) periodically.
	server := setupClient(t, mock.Stream{})
	defer server.ts.Close()

	// Connect to server.ts/wsHandler
	clientConn, _, err := websocket.DefaultDialer.Dial(server.url, nil)
	require.NoError(t, err)
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
	err = clientConn.ReadJSON(&msg)
	require.NoError(t, err)

	assert.Equal(t, "ping", msg["control"])
	srcTs, ok := msg["srcTs"]
	require.True(t, ok, "srcTs not set in ping message, expected a UnixNano value")
	n := int64(srcTs.(float64)) // JSON numbers are floats, so convert to that first, then to int64
	assert.True(t, n > startTs, "srcTs %d <= startTs %d, expected it to be greater", n, startTs)

	// Reply with pong, setting dstTs (we're the dst, Client was the src)
	dstTs := time.Now()
	msg["control"] = "pong"
	msg["dstTs"] = dstTs.UnixNano()
	err = clientConn.WriteJSON(msg)
	require.NoError(t, err)

	var gotLag etre.Latency
	select {
	case gotLag = <-lagChan:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for Client.Latency() to return")
	}
	assert.False(t, gotLag.RTT < 80 || gotLag.RTT > 120, "got RTT %d, expected between 80 and 120ms", gotLag.RTT)
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
	require.NoError(t, err)
	defer clientConn.Close()

	startTs := 1
	start := map[string]interface{}{
		"control": "start",
		"startTs": startTs,
	}
	err = clientConn.WriteJSON(start)
	require.NoError(t, err)

	var ack map[string]interface{}
	err = clientConn.ReadJSON(&ack)
	require.NoError(t, err)
	assert.Equal(t, "start", ack["control"])
	assert.Empty(t, ack["error"], "got an error in the ack response. Expected no error")

	// Send some events via mock.Stream which should flow through the Client
	// and to the HTTP client
	events := []etre.CDCEvent{
		{Id: "abc", Ts: 2}, // these timestamps have to be greater than startTs
		{Id: "def", Ts: 3},
		{Id: "ghi", Ts: 4},
	}
	for _, event := range events {
		eventsChan <- event // when Client called Streamer.Start(), it got this chan
	}

	var gotEvents []etre.CDCEvent
	for i := 0; i < len(events); i++ {
		var recvdEvent etre.CDCEvent
		err = clientConn.ReadJSON(&recvdEvent)
		require.NoError(t, err)
		gotEvents = append(gotEvents, recvdEvent)
	}
	assert.Equal(t, events, gotEvents)

	// Client <- Streamer is one way, so when something happens in Streamer,
	// it closes the chan it returned to Client (eventsChan) to signal to the
	// Client to shut down.
	close(eventsChan)
	var errControl map[string]interface{}
	err = clientConn.ReadJSON(&errControl)
	require.NoError(t, err)
	assert.Equal(t, "error", errControl["control"])

	// Wait for server/wsHandler to return, then check err from Client.Run().
	// It should be ErrWebsocketClosed because closing the stream chan (eventsChan)
	// causes a clean shutdown which closes the websocket.
	<-server.doneChan
	server.Lock()
	gotErr := server.err
	server.Unlock()
	assert.Equal(t, changestream.ErrWebsocketClosed, gotErr)
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
	require.NoError(t, err)
	defer clientConn.Close()

	// Messages are supposed to be map[string]interface{}, so []int is invalid
	invalidMessage := []int{1, 2, 3}
	err = clientConn.WriteJSON(invalidMessage)
	require.NoError(t, err)

	var errControl map[string]interface{}
	err = clientConn.ReadJSON(&errControl)
	require.NoError(t, err)
	assert.Equal(t, "error", errControl["control"])
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
	require.NoError(t, err)
	defer clientConn.Close()

	// This would be valid except "control": "ping" is missing
	ping := map[string]interface{}{
		"srcTs": time.Now().UnixNano(),
	}
	err = clientConn.WriteJSON(ping)
	require.NoError(t, err)

	var errControl map[string]interface{}
	err = clientConn.ReadJSON(&errControl)
	require.NoError(t, err)
	assert.Equal(t, "error", errControl["control"])
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
	require.NoError(t, err)

	// Start streamer so that goroutine is running
	startTs := 1
	start := map[string]interface{}{
		"control": "start",
		"startTs": startTs,
	}
	err = clientConn.WriteJSON(start)
	require.NoError(t, err)

	var ack map[string]interface{}
	err = clientConn.ReadJSON(&ack)
	require.NoError(t, err)
	assert.Equal(t, "start", ack["control"])

	time.Sleep(200 * time.Millisecond)

	clientConn.Close()

	<-server.doneChan
	server.Lock()
	gotErr := server.err
	server.Unlock()
	require.Error(t, gotErr)
	assert.NotEqual(t, changestream.ErrWebsocketClosed.Error(), gotErr.Error())
}
