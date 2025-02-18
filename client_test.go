// Copyright 2017-2020, Square, Inc.

package etre_test

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/square/etre"
)

// The httptest.Server uses these globals. setup() will reset them to defaults.
// Tests should define them as needed immediately after calling setup().
var (
	ts *httptest.Server

	// From test (client)
	gotMethod string
	gotPath   string
	gotQuery  string
	gotBody   []byte

	// Response to test
	respData       interface{}
	respError      *etre.Error // if respData is nil
	respStatusCode int
)
var httpClient = &http.Client{}

func init() {
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotQuery, _ = url.QueryUnescape(r.URL.RawQuery)

		if r.Method == "POST" || r.Method == "PUT" {
			var err error
			gotBody, err = ioutil.ReadAll(r.Body)
			if err != nil {
				panic(err.Error())
			}
		}

		w.WriteHeader(respStatusCode)

		// Write response data, if any
		var bytes []byte
		var err error
		if respError != nil {
			bytes, err = json.Marshal(respError)
			if err != nil {
				panic(err.Error())
			}
		} else if respData != nil {
			bytes, err = json.Marshal(respData)
			if err != nil {
				panic(err.Error())
			}
		}
		if bytes != nil {
			w.Write(bytes)
		}
	}))
}

func setup(t *testing.T) {
	// Reset global vars to defaults
	gotMethod = ""
	gotPath = ""
	gotQuery = ""
	gotBody = nil
	respError = nil
	respData = nil
	respStatusCode = http.StatusOK
}

// //////////////////////////////////////////////////////////////////////////
// Misc
// //////////////////////////////////////////////////////////////////////////

func TestEntityType(t *testing.T) {
	setup(t)
	ec := etre.NewEntityClient("node", ts.URL, httpClient)
	if ec.EntityType() != "node" {
		t.Errorf("got entity type %s, expected node", ec.EntityType())
	}
}

func TestQueryAndIdRequired(t *testing.T) {
	setup(t)

	entities := []etre.Entity{
		{
			"foo": "bar",
		},
	}

	ec := etre.NewEntityClient("node", ts.URL, httpClient)

	// All methods that take query string should return ErrNoQuery if not given one
	if _, err := ec.Query("", etre.QueryFilter{}); err != etre.ErrNoQuery {
		t.Errorf("got error %v, expected etre.ErrNoQuery", err)
	}
	if _, err := ec.Update("", entities[0]); err != etre.ErrNoQuery {
		t.Errorf("got error %v, expected etre.ErrNoQuery", err)
	}
	if _, err := ec.Delete(""); err != etre.ErrNoQuery {
		t.Errorf("got error %v, expected etre.ErrNoQuery", err)
	}

	// All methods that take id string should return ErrIdNotSet if not given one
	if _, err := ec.UpdateOne("", entities[0]); err != etre.ErrIdNotSet {
		t.Errorf("got error %v, expected etre.ErrIdNotSet", err)
	}
	if _, err := ec.DeleteOne(""); err != etre.ErrIdNotSet {
		t.Errorf("got error %v, expected etre.ErrIdNotSet", err)
	}
	if _, err := ec.Labels(""); err != etre.ErrIdNotSet {
		t.Errorf("got error %v, expected etre.ErrIdNotSet", err)
	}
	if _, err := ec.DeleteLabel("", "foo"); err != etre.ErrIdNotSet {
		t.Errorf("got error %v, expected etre.ErrIdNotSet", err)
	}
}

// //////////////////////////////////////////////////////////////////////////
// Query
// //////////////////////////////////////////////////////////////////////////

func TestQueryOK(t *testing.T) {
	setup(t)

	// Set global vars used by httptest.Server
	respData = []etre.Entity{
		{
			"_id":      "abc",
			"hostname": "localhost",
		},
	}

	ec := etre.NewEntityClient("node", ts.URL, httpClient)

	// Normal query that returns status code 200 and respData
	query := "x=y"
	expectQuery := "query=" + query
	got, err := ec.Query(query, etre.QueryFilter{})
	require.NoError(t, err)

	// Verify call and response
	expectPath := etre.API_ROOT + "/entities/node"
	if gotPath != expectPath {
		t.Errorf("got path %s, expected %s", gotPath, expectPath)
	}
	if gotQuery != expectQuery {
		t.Errorf("got query %s, expected %s", gotQuery, expectQuery)
	}
	if diff := deep.Equal(got, respData); diff != nil {
		t.Error(diff)
	}
}

func TestQueryNoResults(t *testing.T) {
	// Same test as TestQueryOK but no results to make sure client handles
	// status code 200 but an empty list.
	setup(t)

	// Set global vars used by httptest.Server
	respData = []etre.Entity{}

	ec := etre.NewEntityClient("node", ts.URL, httpClient)

	got, err := ec.Query("any=thing", etre.QueryFilter{})
	require.NoError(t, err)

	if diff := deep.Equal(got, respData); diff != nil {
		t.Error(diff)
	}
}

func TestQueryHandledError(t *testing.T) {
	// Test that client returns error on API error and no entities
	setup(t)

	// Set global vars used by httptest.Server
	respStatusCode = http.StatusInternalServerError
	respError = &etre.Error{
		Type:    "fake_error",
		Message: "this is a fake error",
	}

	ec := etre.NewEntityClient("node", ts.URL, httpClient)
	got, err := ec.Query("any=thing", etre.QueryFilter{})
	require.Error(t, err)

	if !strings.Contains(err.Error(), respError.Message) {
		t.Errorf("error message does not contain '%s': '%s'", respError.Message, err)
	}
	assert.Nil(t, got)
}

func TestQueryUnhandledError(t *testing.T) {
	// Like TestQueryHandledError above, but simulating a more severe error,
	// like a panic, that makes the API _not_ return an etre.Error. The client
	// should handle this and still return an error.
	setup(t)

	// Set global vars used by httptest.Server
	respStatusCode = http.StatusInternalServerError

	ec := etre.NewEntityClient("node", ts.URL, httpClient)
	got, err := ec.Query("any=thing", etre.QueryFilter{})
	require.Error(t, err)
	if !strings.Contains(err.Error(), "no response") {
		t.Errorf("error does not contain 'no response': '%s'", err)
	}
	if got != nil {
		t.Errorf("got entities, expected nil: %v", got)
	}
}

// //////////////////////////////////////////////////////////////////////////
// Insert
// //////////////////////////////////////////////////////////////////////////

func TestInsertOK(t *testing.T) {
	setup(t)

	// Set global vars used by httptest.Server
	respData = etre.WriteResult{
		Writes: []etre.Write{
			{
				EntityId: "abc",
				URI:      "http://localhost/entity/abc",
			},
		},
	}
	respStatusCode = http.StatusCreated

	// New etre.Client
	ec := etre.NewEntityClient("node", ts.URL, httpClient)

	// Normal insert that returns status code 201 and a write result
	entities := []etre.Entity{
		{
			"foo": "bar",
		},
	}
	got, err := ec.Insert(entities)
	require.NoError(t, err)

	// Verify call and response
	if gotMethod != "POST" {
		t.Errorf("got method %s, expected POST", gotMethod)
	}
	expectPath := etre.API_ROOT + "/entities/node"
	if gotPath != expectPath {
		t.Errorf("got path %s, expected %s", gotPath, expectPath)
	}
	if diff := deep.Equal(got, respData); diff != nil {
		t.Error(diff)
	}
}

func TestInsertAPIError(t *testing.T) {
	// API should return error in WriteResult.Error
	setup(t)

	// Set global vars used by httptest.Server
	respStatusCode = http.StatusInternalServerError
	respData = etre.WriteResult{
		Error: &etre.Error{
			Type:    "fake_error",
			Message: "this is a fake error",
		},
	}

	// Get error on insert
	ec := etre.NewEntityClient("node", ts.URL, httpClient)
	entities := []etre.Entity{
		{
			"foo": "bar",
		},
	}
	got, err := ec.Insert(entities)
	require.NoError(t, err)

	if diff := deep.Equal(got, respData.(etre.WriteResult)); diff != nil {
		t.Error(diff)
	}
}

func TestInsertUnhandledError(t *testing.T) {
	// If API crashes or some unhandled error occurs, there's no WriteResult,
	// but client should handle this and still return an error
	setup(t)

	// Set global vars used by httptest.Server
	respStatusCode = http.StatusInternalServerError
	respData = nil // error ^, but no resp data

	ec := etre.NewEntityClient("node", ts.URL, httpClient)
	entities := []etre.Entity{{"foo": "bar"}}
	wr, err := ec.Insert(entities)
	require.Error(t, err)

	if !strings.Contains(err.Error(), "no response") {
		t.Errorf("error does not contain 'no response': '%s'", err)
	}
	if !wr.IsZero() {
		t.Errorf("non-zero WriteResult, expected no WriteResult: %+v", wr)
	}
}

func TestInsertNoEntityError(t *testing.T) {
	setup(t)

	ec := etre.NewEntityClient("node", ts.URL, httpClient)

	// A zero length slice of entities should return ErrNoEntity
	entities := []etre.Entity{}
	got, err := ec.Insert(entities)
	if err != etre.ErrNoEntity {
		t.Fatalf("err is '%s', expected ErrNoEtity", err)
	}
	if got.Writes != nil {
		t.Errorf("got []etre.WriteResult, expected nil: %#v", got)
	}
}

// //////////////////////////////////////////////////////////////////////////
// Update
// //////////////////////////////////////////////////////////////////////////

func TestUpdateOK(t *testing.T) {
	setup(t)

	// Set global vars used by httptest.Server
	respData = etre.WriteResult{
		Writes: []etre.Write{
			{
				URI: "http://localhost/entity/abc",
				Diff: map[string]interface{}{
					"foo": "foo",
				},
			},
		},
	}

	// New etre.Client
	ec := etre.NewEntityClient("node", ts.URL, httpClient)

	// Normal update that returns status code 200 and a write result
	entity := etre.Entity{
		"foo": "bar", // patch foo:foo -> for:bar
	}
	got, err := ec.Update("foo=bar", entity)
	require.NoError(t, err)

	// Verify call and response
	if gotMethod != "PUT" {
		t.Errorf("got method %s, expected PUT", gotMethod)
	}
	expectPath := etre.API_ROOT + "/entities/node"
	if gotPath != expectPath {
		t.Errorf("got path %s, expected %s", gotPath, expectPath)
	}
	if diff := deep.Equal(got, respData); diff != nil {
		t.Error(diff)
	}
}

func TestUpdateAPIError(t *testing.T) {
	setup(t)

	// Set global vars used by httptest.Server
	respError = &etre.Error{
		Type:    "fake_error",
		Message: "this is a fake error",
	}
	respStatusCode = http.StatusInternalServerError

	// Get error on update
	ec := etre.NewEntityClient("node", ts.URL, httpClient)
	entity := etre.Entity{
		"foo": "bar",
	}
	got, err := ec.Update("foo=bar", entity)
	require.Error(t, err)

	// The etre.Error.Message should bubble up
	if !strings.Contains(err.Error(), respError.Message) {
		t.Errorf("error does not contain '%s': %s", respError.Message, err)
	}

	// There should not be any entities returned
	if got.Writes != nil {
		t.Errorf("got []etre.WriteResult, expected nil: %#v", got)
	}
}

func TestUpdateNoEntityError(t *testing.T) {
	setup(t)

	ec := etre.NewEntityClient("node", ts.URL, httpClient)

	// A zero length slice of entities should return ErrNoEntity
	entity := etre.Entity{}
	got, err := ec.Update("foo=bar", entity)
	if err != etre.ErrNoEntity {
		t.Fatalf("err is '%s', expected ErrNoEtity", err)
	}
	if got.Writes != nil {
		t.Errorf("got []etre.WriteResult, expected nil: %#v", got)
	}
}

// //////////////////////////////////////////////////////////////////////////
// UpdateOne
// //////////////////////////////////////////////////////////////////////////

func TestUpdateOneOK(t *testing.T) {
	// Same at TestUpdateOK, just calling UpdateOne instead which is just
	// a convenience func for Update.
	setup(t)

	respData = etre.WriteResult{
		Writes: []etre.Write{
			{
				EntityId: "abc",
				URI:      "http://localhost/entity/abc",
				Diff: map[string]interface{}{
					"foo": "foo",
				},
			},
		},
	}

	ec := etre.NewEntityClient("node", ts.URL, httpClient)

	entity := etre.Entity{
		"foo": "bar", // patch foo:foo -> for:bar
	}
	got, err := ec.UpdateOne("abc", entity)
	require.NoError(t, err)

	if gotMethod != "PUT" {
		t.Errorf("got method %s, expected PUT", gotMethod)
	}
	expectPath := etre.API_ROOT + "/entity/node/abc"
	if gotPath != expectPath {
		t.Errorf("got path %s, expected %s", gotPath, expectPath)
	}
	if diff := deep.Equal(got, respData); diff != nil {
		t.Error(diff)
	}
}

// //////////////////////////////////////////////////////////////////////////
// Delete
// //////////////////////////////////////////////////////////////////////////

func TestDeleteOK(t *testing.T) {
	setup(t)

	// Set global vars used by httptest.Server
	respData = etre.WriteResult{
		Writes: []etre.Write{
			{
				EntityId: "abc",
				URI:      "http://localhost/entity/abc",
				Diff: map[string]interface{}{
					"foo": "foo",
				},
			},
		},
	}

	// New etre.Client
	ec := etre.NewEntityClient("node", ts.URL, httpClient)

	// Normal delete that returns status code 200 and a write result
	query := "foo=bar"
	got, err := ec.Delete(query)
	require.NoError(t, err)

	// Verify call and response
	if gotMethod != "DELETE" {
		t.Errorf("got method %s, expected DELETE", gotMethod)
	}
	expectPath := etre.API_ROOT + "/entities/node"
	expectQuery := "query=" + query
	if gotPath != expectPath {
		t.Errorf("got path %s, expected %s", gotPath, expectPath)
	}
	if gotQuery != expectQuery {
		t.Errorf("got query %s, expected %s", gotQuery, expectQuery)
	}
	if diff := deep.Equal(got, respData); diff != nil {
		t.Error(diff)
	}
}

func TestDeleteWithSet(t *testing.T) {
	setup(t)

	// Set global vars used by httptest.Server
	respData = etre.WriteResult{
		Writes: []etre.Write{
			{
				EntityId: "abc",
				URI:      "http://localhost/entity/abc",
				Diff: map[string]interface{}{
					"foo": "foo",
				},
			},
		},
	}

	// New etre.Client
	ec := etre.NewEntityClient("node", ts.URL, httpClient)

	set := etre.Set{
		Id:   "setid",
		Op:   "setop",
		Size: 3,
	}
	ec = ec.WithSet(set)

	// Normal delete that returns status code 200 and a write result
	query := "foo=bar"
	got, err := ec.Delete(query)
	require.NoError(t, err)

	// Verify call and response
	if gotMethod != "DELETE" {
		t.Errorf("got method %s, expected DELETE", gotMethod)
	}
	expectPath := etre.API_ROOT + "/entities/node"
	expectQuery := "query=" + query + "&setId=setid&setOp=setop&setSize=3"
	if gotPath != expectPath {
		t.Errorf("got path %s, expected %s", gotPath, expectPath)
	}
	if gotQuery != expectQuery {
		t.Errorf("got query %s, expected %s", gotQuery, expectQuery)
	}
	if diff := deep.Equal(got, respData); diff != nil {
		t.Error(diff)
	}
}

// //////////////////////////////////////////////////////////////////////////
// DeleteOne
// //////////////////////////////////////////////////////////////////////////

func TestDeleteOneOK(t *testing.T) {
	// Same test as DeleteOK, just using the DeleteOne convenience function instead
	setup(t)

	respData = etre.WriteResult{
		Writes: []etre.Write{
			{
				EntityId: "abc",
				URI:      "http://localhost/entity/abc",
				Diff: map[string]interface{}{
					"foo": "foo",
				},
			},
		},
	}

	ec := etre.NewEntityClient("node", ts.URL, httpClient)

	got, err := ec.DeleteOne("abc")
	require.NoError(t, err)

	if gotMethod != "DELETE" {
		t.Errorf("got method %s, expected DELETE", gotMethod)
	}
	expectPath := etre.API_ROOT + "/entity/node/abc"
	if gotPath != expectPath {
		t.Errorf("got path %s, expected %s", gotPath, expectPath)
	}
	if diff := deep.Equal(got, respData); diff != nil {
		t.Error(diff)
	}
}

func TestDeleteOneWithSet(t *testing.T) {
	// With a set up, the query should contain the set op params
	setup(t)

	respData = etre.WriteResult{
		Writes: []etre.Write{
			{
				EntityId: "abc",
				URI:      "http://localhost/entity/abc",
				Diff: map[string]interface{}{
					"foo": "foo",
				},
			},
		},
	}

	ec := etre.NewEntityClient("node", ts.URL, httpClient)

	set := etre.Set{
		Id:   "setid",
		Op:   "setop",
		Size: 2,
	}
	ec = ec.WithSet(set)

	got, err := ec.DeleteOne("abc")
	require.NoError(t, err)
	if gotMethod != "DELETE" {
		t.Errorf("got method %s, expected DELETE", gotMethod)
	}
	expectPath := etre.API_ROOT + "/entity/node/abc"
	expectQuery := "setId=setid&setOp=setop&setSize=2" // testing this
	if gotPath != expectPath {
		t.Errorf("got path %s, expected %s", gotPath, expectPath)
	}
	if gotQuery != expectQuery {
		t.Errorf("got query %s, expected %s", gotQuery, expectQuery)
	}
	if diff := deep.Equal(got, respData); diff != nil {
		t.Error(diff)
	}
}

// //////////////////////////////////////////////////////////////////////////
// Labels and DeleteLabel
// //////////////////////////////////////////////////////////////////////////

func TestLabelsOK(t *testing.T) {
	setup(t)

	// Set global vars used by httptest.Server
	respData = []string{"foo", "bar"}

	ec := etre.NewEntityClient("node", ts.URL, httpClient)

	got, err := ec.Labels("abc")
	require.NoError(t, err)
	if gotMethod != "GET" {
		t.Errorf("got method %s, expected GET", gotMethod)
	}
	expectPath := etre.API_ROOT + "/entity/node/abc/labels"
	if gotPath != expectPath {
		t.Errorf("got path %s, expected %s", gotPath, expectPath)
	}
	if diff := deep.Equal(got, respData); diff != nil {
		t.Error(diff)
	}
}

func TestDeleteLabelOK(t *testing.T) {
	setup(t)

	respData = etre.WriteResult{
		Writes: []etre.Write{
			{
				EntityId: "abc",
				URI:      "http://localhost/entity/abc",
				Diff: map[string]interface{}{
					"foo": "foo",
				},
			},
		},
	}

	ec := etre.NewEntityClient("node", ts.URL, httpClient)

	got, err := ec.DeleteLabel("abc", "foo")
	require.NoError(t, err)

	if gotMethod != "DELETE" {
		t.Errorf("got method %s, expected DELETE", gotMethod)
	}
	expectPath := etre.API_ROOT + "/entity/node/abc/labels/foo"
	if gotPath != expectPath {
		t.Errorf("got path %s, expected %s", gotPath, expectPath)
	}
	if diff := deep.Equal(got, respData); diff != nil {
		t.Error(diff)
	}
}

// //////////////////////////////////////////////////////////////////////////
// CDC
// //////////////////////////////////////////////////////////////////////////

func TestCDCClient(t *testing.T) {
	debug := false // Ryan's Rule #9

	// Setup a websocket handler to handle the initial low-level ws connection
	// and do the Etre CDC feed start sequence: client send start control and
	// waits to receive start control ack. After started, flow is synchronous
	// so test can send/receive (mostly send) on wsConn which will be read by
	// client and, if it's a CDC event, sent to the events chan returned by Start.
	connChan := make(chan bool)
	var wsConn *websocket.Conn
	var gotStart map[string]interface{}
	startAck := map[string]interface{}{
		"control": "start",
	}
	wsHandler := func(w http.ResponseWriter, r *http.Request) {
		var upgrader = websocket.Upgrader{}
		var err error
		wsConn, err = upgrader.Upgrade(w, r, nil)
		require.NoError(t, err)
		defer wsConn.Close()
		err = wsConn.ReadJSON(&gotStart)
		require.NoError(t, err)
		err = wsConn.WriteJSON(startAck)
		require.NoError(t, err)
		connChan <- true
		<-connChan
	}
	ts = httptest.NewServer(http.HandlerFunc(wsHandler))
	defer ts.Close()
	defer close(connChan)

	// Start client
	url, _ := url.Parse(ts.URL)
	ec := etre.NewCDCClient("ws://"+url.Host, nil, 10, debug)
	defer ec.Stop()

	startTs := time.Now()
	events, err := ec.Start(startTs)
	require.NoError(t, err)

	// Wait for wsHandler ^ to do start sequence
	select {
	case <-connChan:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for wsHandler to ack start sequence")
	}

	//
	// Client is connected to wsConn (via wsHandler), waiting to receive from us.
	// At this point, communicate is synchronous: cdc feed (us) -> client. Client
	// only sends in response to control:ping.
	//

	// Start should be idempotent
	events2, err2 := ec.Start(startTs)
	if err2 != nil {
		t.Errorf("got error %s, expected none", err2)
	}
	if events2 != events {
		t.Errorf("Start did not return same events chan, expected same one")
	}

	// Verify client sent correct start control message

	// Need to marshal and unmarshal this because startTs: startTs will be
	// a time.Time type but startTs recv'ed is a string and startTs.String()
	// is slightly different than the JSON-mashaled time string. So only
	// way to be consistent is to cmp json marshaled to json marshaled.
	v := map[string]interface{}{
		"control": "start",
		"startTs": startTs.UnixNano() / int64(time.Millisecond),
	}
	bytes, _ := json.Marshal(v)
	var expectStart map[string]interface{}
	json.Unmarshal(bytes, &expectStart)
	if diff := deep.Equal(gotStart, expectStart); diff != nil {
		t.Logf("gotStart: %#v", gotStart)
		t.Logf("expectStart: %s", string(bytes))
		t.Error(diff)
	}

	// First, let's send the client a CDC event and make sure it sends via the
	// events chan it returned from Start()
	sentEvent := etre.CDCEvent{
		Id:         "xyz",
		Ts:         1001,
		Op:         "i", // insert
		Caller:     "ryan",
		EntityId:   "abc",
		EntityType: "node",
		Old:        nil,
		New: &etre.Entity{
			"_id": "abc",
			"foo": "bar",
		},
	}
	if err := wsConn.WriteJSON(sentEvent); err != nil {
		t.Fatal(err)
		return
	}
	var gotEvent etre.CDCEvent
	select {
	case gotEvent = <-events:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout receiving event from client chan")
	}

	// The event we got should be the event we sent--that's the whole point!
	if diff := deep.Equal(gotEvent, sentEvent); diff != nil {
		t.Logf("%#v", gotStart)
		t.Error(diff)
	}

	//
	// Send client a ping control message (server -> client ping)
	//

	ping := map[string]interface{}{
		"control": "ping",
		"srcTs":   startTs.UnixNano(),
	}
	if err := wsConn.WriteJSON(ping); err != nil {
		t.Fatal(err)
		return
	}
	var pong map[string]interface{}
	if err := wsConn.ReadJSON(&pong); err != nil {
		t.Fatal(err)
		return
	}
	if pong["control"] != "pong" {
		t.Errorf("wrong control reply '%s', expected 'ping'", pong["control"])
	}
	ts, ok := pong["dstTs"]
	if !ok {
		t.Errorf("dstTs not set in ping reply, expected a UnixNano value")
	} else {
		// Go JSON makes all numbers float64, so convert to that first,
		// then int64 for UnixNano.
		n := int64(ts.(float64))
		if n <= startTs.UnixNano() {
			t.Errorf("got ts %d <= sent ts %d, expected it to be greater", n, startTs.UnixNano())
		}
	}

	//
	// Ping server (client -> server ping)
	//

	// Recv ping, wait 101ms, send pong
	waitForPing := make(chan struct{})
	go func() {
		var ping map[string]interface{}
		var err error

		err = wsConn.ReadJSON(&ping)
		require.NoError(t, err)

		time.Sleep(101 * time.Millisecond)
		ping["control"] = "pong"
		ping["dstTs"] = time.Now().UnixNano()
		err = wsConn.WriteJSON(ping)
		require.NoError(t, err)

		close(waitForPing)
	}()

	lag := ec.Ping(time.Duration(1 * time.Second))

	// lag.Recv is almost always <1ms because it's the time from calling
	// Ping to wsConn.ReadJSON in the gorountine above. Since that's local
	// it's microseconds. But the time.Sleep in the goroutine creates an
	// artificial Send and RTT lag.
	if lag.Send < 101 || lag.RTT < 101 {
		t.Errorf("got zero lag, exected > 100ms values: %#v", lag)
	}

	<-waitForPing

	//
	// Send client an error
	//

	// This should cause the client to close the connection, which we can detect
	// by trying to read, which should return an error
	errorMsg := map[string]interface{}{
		"control": "error",
		"error":   "fake error",
	}
	err = wsConn.WriteJSON(errorMsg)
	require.NoError(t, err)

	// Give client a few milliseconds to shutdown
	time.Sleep(500 * time.Millisecond)
	var rand map[string]interface{} // shouldn't read random data
	err = wsConn.ReadJSON(&rand)
	require.Error(t, err)

	// The client should save the error ^ and return it
	gotError := ec.Error().Error()
	if !strings.Contains(gotError, "fake error") {
		t.Errorf("got error '%s', expected 'fake error'", gotError)
	}
}
