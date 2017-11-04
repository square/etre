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
	"github.com/square/etre"
)

// @todo: the tests here are racey (run with --race to see)

var httpClient *http.Client

// The httptest.Server uses these globals. setup() will reset them to defaults.
// Tests should define them as needed immediately after calling setup().
var ts *httptest.Server
var (
	gotMethod string
	gotPath   string
	gotQuery  string
	gotBody   []byte
)
var ( // response data in order of precedence
	respError *etre.Error
	respData  interface{}
)
var respStatusCode int

func setup(t *testing.T) {
	if ts == nil {
		ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotMethod = r.Method
			gotPath = r.URL.Path
			gotQuery, _ = url.QueryUnescape(r.URL.RawQuery)

			if r.Method == "POST" || r.Method == "PUT" {
				var err error
				gotBody, err = ioutil.ReadAll(r.Body)
				if err != nil {
					t.Fatal(err)
				}
			}

			w.WriteHeader(respStatusCode)

			// Write response data, if any
			var bytes []byte
			var err error
			if respError != nil {
				bytes, err = json.Marshal(respError)
				if err != nil {
					t.Fatal(err)
				}
			} else if respData != nil {
				bytes, err = json.Marshal(respData)
				if err != nil {
					t.Fatal(err)
				}
			}
			if bytes != nil {
				w.Write(bytes)
			}
		}))
	}
	if httpClient == nil {
		httpClient = &http.Client{}
	}

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

func TestWriteError(t *testing.T) {
	// No error when len(en) = len(wr) and ErrorCode=0 for all wr
	en := []etre.Entity{
		{"_id": "abc", "foo": "bar"},
	}
	wr := []etre.WriteResult{
		{Id: "abc"},
	}
	if err := etre.WriteError(wr, en); err != nil {
		t.Errorf("got error '%s', expected none", err)
	}

	// Add an error
	wr[0].Error = "duplicate entity"
	if err := etre.WriteError(wr, en); err == nil {
		t.Errorf("no error, expected \"duplicate entry\"")
	}
}

func TestEntityType(t *testing.T) {
	setup(t)
	ec := etre.NewEntityClient("node", ts.URL, httpClient)
	if ec.EntityType() != "node" {
		t.Errorf("got entity type %s, expected node", ec.EntityType)
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

	// All methods that take query string should return ErrNoQuery if not
	// given one
	if _, err := ec.Query("", etre.QueryFilter{}); err != etre.ErrNoQuery {
		t.Errorf("got error %v, expected etre.ErrNoQuery", err)
	}
	if _, err := ec.Update("", entities[0]); err != etre.ErrNoQuery {
		t.Errorf("got error %v, expected etre.ErrNoQuery", err)
	}
	if _, err := ec.Delete(""); err != etre.ErrNoQuery {
		t.Errorf("got error %v, expected etre.ErrNoQuery", err)
	}

	// All methods that take id string should return ErrIdNotSet if not
	// given one
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

	// New etre.Client
	ec := etre.NewEntityClient("node", ts.URL, httpClient)

	// Normal query that returns status code 200 and respData
	query := "x=y"
	expectQuery := "query=" + query
	got, err := ec.Query(query, etre.QueryFilter{})
	if err != nil {
		t.Fatal(err)
	}

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

func TestQueryLongOK(t *testing.T) {
	setup(t)

	// Set global vars used by httptest.Server
	respData = []etre.Entity{
		{
			"_id":      "abc",
			"hostname": "localhost",
		},
	}

	// New etre.Client
	ec := etre.NewEntityClient("node", ts.URL, httpClient)

	// A very long query that returns status code 200 and respData
	query := "this query is fake and just needs to be greater than 2,000 characters long" +
		"in order to trigger the client automatically switching from endpoint GET /entities," +
		"which we think is canonical and logical because that endpoint intuitively feels" +
		"like it would fetch entities and so we made it do just that, to endpoint POST /query" +
		"which, taking the query--this very long and wordy query--by way of POST data instead" +
		"of URL query parameter bypasses the roughly 2,000 character limit for a URL." +
		"Despite how long this fake query already is, as of here we are only just over 500" +
		"characters. So... yeah. Let's just pretend a cat crawled up on the keyboard and" +
		"lied down to take a nap, thereby randomly pressing keys, resulting in djkdlf;a " +
		"jkfd;a jkfdl;afdsa jklfdakljdfa  fdjkal;fd ajk;fd39pura; jf0-2ir;lsdj fjdk fda093 " +
		"u9025;lkj fds03292jkl;sa 903[2fjklseaui9032jakl;fd ;309rjoiajfkle;jro3palk;f3u09 " +
		"................................................................................ " +
		"................................................................................ " +
		"................................................................................ " +
		"................................................................................ " +
		"................................................................................ " +
		"................................................................................ " +
		"................................................................................ " +
		"................................................................................ " +
		"................................................................................ " +
		"................................................................................ " +
		"................................................................................ " +
		"................................................................................ " +
		"................................................................................ " +
		"................................................................................ " +
		"(that's where its nose rested on the '.' key). OK, that's long enough. Your dog " +
		"is scratching at the door and wants to be let back in the house."
	got, err := ec.Query(query, etre.QueryFilter{})
	if err != nil {
		t.Fatal(err)
	}

	// Verify call and response
	if gotMethod != "POST" {
		t.Errorf("got method %s, expected POST", gotMethod)
	}
	expectPath := etre.API_ROOT + "/query/node"
	if gotPath != expectPath {
		t.Errorf("got path %s, expected %s", gotPath, expectPath)
	}
	if gotQuery != "" {
		t.Errorf("got query %s, expected none", gotQuery)
	}
	if gotBody == nil {
		t.Errorf("no body, expected the query as POST data")
	} else {
		if string(gotBody) != query {
			t.Errorf("got query in body '%s', expected the long query", string(gotBody))
		}
	}
	if diff := deep.Equal(got, respData); diff != nil {
		t.Error(diff)
	}
}

func TestQueryNoResults(t *testing.T) {
	setup(t)

	// Set global vars used by httptest.Server
	respData = []etre.Entity{}

	// Same test as TestQueryOK but no results to make sure client handles
	// status code 200 but an empty list.
	ec := etre.NewEntityClient("node", ts.URL, httpClient)

	got, err := ec.Query("any=thing", etre.QueryFilter{})
	if err != nil {
		t.Fatal(err)
	}

	if diff := deep.Equal(got, respData); diff != nil {
		t.Error(diff)
	}
}

func TestQueryHandledError(t *testing.T) {
	setup(t)

	// Set global vars used by httptest.Server
	respError = &etre.Error{
		Type:    "fake_error",
		Message: "this is a fake error",
	}
	respStatusCode = http.StatusInternalServerError

	// Test that client returns error on API error
	ec := etre.NewEntityClient("node", ts.URL, httpClient)
	got, err := ec.Query("any=thing", etre.QueryFilter{})
	if err == nil {
		t.Fatal("err is nil, expected an error")
	}

	// The etre.Error.Message should bubble up
	if !strings.Contains(err.Error(), respError.Message) {
		t.Errorf("error does not contain '%s': %s", respError.Message, err)
	}

	// There should not be any entities returned
	if got != nil {
		t.Errorf("got []etre.Entity, expected nil: %#v", got)
	}
}

func TestQueryUnhandledError(t *testing.T) {
	setup(t)

	// Set global vars used by httptest.Server
	respStatusCode = http.StatusInternalServerError

	// Like TestQueryHandledError above, but simulating a more severe error,
	// like a panic, that makes the API not return an etre.etre.Error
	ec := etre.NewEntityClient("node", ts.URL, httpClient)
	got, err := ec.Query("any=thing", etre.QueryFilter{})
	if err == nil {
		t.Fatal("err is nil, expected an error")
	}

	// There should not be any entities returned
	if got != nil {
		t.Errorf("got []etre.Entity, expected nil: %#v", got)
	}
}

// //////////////////////////////////////////////////////////////////////////
// Insert
// //////////////////////////////////////////////////////////////////////////

func TestInsertOK(t *testing.T) {
	setup(t)

	// Set global vars used by httptest.Server
	respData = []etre.WriteResult{
		{
			Id:  "abc",
			URI: "http://localhost/entity/abc",
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
	if err != nil {
		t.Fatal(err)
	}

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
	setup(t)

	// Set global vars used by httptest.Server
	respError = &etre.Error{
		Type:    "fake_error",
		Message: "this is a fake error",
	}
	respStatusCode = http.StatusInternalServerError

	// Get error on insert
	ec := etre.NewEntityClient("node", ts.URL, httpClient)
	entities := []etre.Entity{
		{
			"foo": "bar",
		},
	}
	got, err := ec.Insert(entities)
	if err == nil {
		t.Fatal("err is nil, expected an error")
	}

	// The etre.Error.Message should bubble up
	if !strings.Contains(err.Error(), respError.Message) {
		t.Errorf("error does not contain '%s': %s", respError.Message, err)
	}

	// There should not be any entities returned
	if got != nil {
		t.Errorf("got []etre.WriteResult, expected nil: %#v", got)
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
	if got != nil {
		t.Errorf("got []etre.WriteResult, expected nil: %#v", got)
	}
}

// //////////////////////////////////////////////////////////////////////////
// Update
// //////////////////////////////////////////////////////////////////////////

func TestUpdateOK(t *testing.T) {
	setup(t)

	// Set global vars used by httptest.Server
	respData = []etre.WriteResult{
		{
			URI: "http://localhost/entity/abc",
			Diff: map[string]interface{}{
				"foo": "foo",
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
	if err != nil {
		t.Fatal(err)
	}

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
	if err == nil {
		t.Fatal("err is nil, expected an error")
	}

	// The etre.Error.Message should bubble up
	if !strings.Contains(err.Error(), respError.Message) {
		t.Errorf("error does not contain '%s': %s", respError.Message, err)
	}

	// There should not be any entities returned
	if got != nil {
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
	if got != nil {
		t.Errorf("got []etre.WriteResult, expected nil: %#v", got)
	}
}

// //////////////////////////////////////////////////////////////////////////
// UpdateOne
// //////////////////////////////////////////////////////////////////////////

func TestUpdateOneOK(t *testing.T) {
	setup(t)

	// Set global vars used by httptest.Server
	respData = etre.WriteResult{
		Id:  "abc",
		URI: "http://localhost/entity/abc",
		Diff: map[string]interface{}{
			"foo": "foo",
		},
	}

	// Same at TestUpdateOK, just calling UpdateOne instead which is just
	// a convenience func for Update.
	ec := etre.NewEntityClient("node", ts.URL, httpClient)

	entity := etre.Entity{
		"foo": "bar", // patch foo:foo -> for:bar
	}
	got, err := ec.UpdateOne("abc", entity)
	if err != nil {
		t.Fatal(err)
	}

	if gotMethod != "PUT" {
		t.Errorf("got method %s, expected PUT", gotMethod)
	}
	expectPath := etre.API_ROOT + "/entity/node/abc"
	if gotPath != expectPath {
		t.Errorf("got path %s, expected %s", gotPath, expectPath)
	}
	if diff := deep.Equal(got, respData.(etre.WriteResult)); diff != nil {
		t.Error(diff)
	}
}

// //////////////////////////////////////////////////////////////////////////
// Delete
// //////////////////////////////////////////////////////////////////////////

func TestDeleteOK(t *testing.T) {
	setup(t)

	// Set global vars used by httptest.Server
	respData = []etre.WriteResult{
		{
			Id:  "abc",
			URI: "http://localhost/entity/abc",
			Diff: map[string]interface{}{
				"foo": "foo",
			},
		},
	}

	// New etre.Client
	ec := etre.NewEntityClient("node", ts.URL, httpClient)

	// Normal delete that returns status code 200 and a write result
	query := "foo=bar"
	got, err := ec.Delete(query)
	if err != nil {
		t.Fatal(err)
	}

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
	respData = []etre.WriteResult{
		{
			Id:  "abc",
			URI: "http://localhost/entity/abc",
			Diff: map[string]interface{}{
				"foo": "foo",
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
	if err != nil {
		t.Fatal(err)
	}

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
	setup(t)

	// Set global vars used by httptest.Server
	respData = etre.WriteResult{
		Id:  "abc",
		URI: "http://localhost/entity/abc",
		Diff: map[string]interface{}{
			"foo": "foo",
		},
	}

	// Same test as DeleteOK, just using the DeleteOne convenience function instead
	ec := etre.NewEntityClient("node", ts.URL, httpClient)

	// Normal delete that returns status code 200 and a write result
	got, err := ec.DeleteOne("abc")
	if err != nil {
		t.Fatal(err)
	}
	if gotMethod != "DELETE" {
		t.Errorf("got method %s, expected DELETE", gotMethod)
	}
	expectPath := etre.API_ROOT + "/entity/node/abc"
	if gotPath != expectPath {
		t.Errorf("got path %s, expected %s", gotPath, expectPath)
	}
	if diff := deep.Equal(got, respData.(etre.WriteResult)); diff != nil {
		t.Error(diff)
	}
}

func TestDeleteOneWithSet(t *testing.T) {
	setup(t)

	respData = etre.WriteResult{
		Id:  "abc",
		URI: "http://localhost/entity/abc",
		Diff: map[string]interface{}{
			"foo": "foo",
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
	if err != nil {
		t.Fatal(err)
	}
	if gotMethod != "DELETE" {
		t.Errorf("got method %s, expected DELETE", gotMethod)
	}
	expectPath := etre.API_ROOT + "/entity/node/abc"
	expectQuery := "setId=setid&setOp=setop&setSize=2"
	if gotPath != expectPath {
		t.Errorf("got path %s, expected %s", gotPath, expectPath)
	}
	if gotQuery != expectQuery {
		t.Errorf("got query %s, expected %s", gotQuery, expectQuery)
	}
	if diff := deep.Equal(got, respData.(etre.WriteResult)); diff != nil {
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

	// New etre.Client
	ec := etre.NewEntityClient("node", ts.URL, httpClient)

	got, err := ec.Labels("abc")
	if err != nil {
		t.Fatal(err)
	}

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

	// Set global vars used by httptest.Server
	respData = etre.WriteResult{
		Id:  "abc",
		URI: "http://localhost/entity/abc",
		Diff: map[string]interface{}{
			"foo": "foo",
		},
	}

	// New etre.Client
	ec := etre.NewEntityClient("node", ts.URL, httpClient)

	got, err := ec.DeleteLabel("abc", "foo")
	if err != nil {
		t.Fatal(err)
	}

	if gotMethod != "DELETE" {
		t.Errorf("got method %s, expected DELETE", gotMethod)
	}
	expectPath := etre.API_ROOT + "/entity/node/abc/labels/foo"
	if gotPath != expectPath {
		t.Errorf("got path %s, expected %s", gotPath, expectPath)
	}
	if diff := deep.Equal(got, respData.(etre.WriteResult)); diff != nil {
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
		if err != nil {
			t.Fatal(err)
			return
		}
		defer wsConn.Close()
		if err := wsConn.ReadJSON(&gotStart); err != nil {
			t.Fatal(err)
			return
		}
		if err := wsConn.WriteJSON(startAck); err != nil {
			t.Fatal(err)
			return
		}
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
	if err != nil {
		t.Fatal(err)
	}

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
		EventId:    "xyz",
		EntityId:   "abc",
		EntityType: "node",
		Ts:         1001,
		User:       "ryan",
		Op:         "i", // insert
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
			t.Errorf("got ts %s <= sent ts %d, expected it to be greater", n, startTs.UnixNano())
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
		if err != nil {
			t.Error(err)
			return
		}
		time.Sleep(101 * time.Millisecond)
		ping["control"] = "pong"
		ping["dstTs"] = time.Now().UnixNano()
		t.Logf("%#v", ping)
		err = wsConn.WriteJSON(ping)
		if err != nil {
			t.Error(err)
			return
		}

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
	if err != nil {
		t.Fatal(err)
		return
	}

	// Give client a few milliseconds to shutdown
	time.Sleep(500 * time.Millisecond)
	var rand map[string]interface{} // shouldn't read random data
	err = wsConn.ReadJSON(&rand)
	if err == nil {
		t.Errorf("no error read, expected an error; read %#v", rand)
	}

	// The client should save the error ^ and return it
	gotError := ec.Error().Error()
	if !strings.Contains(gotError, "fake error") {
		t.Errorf("got error '%s', expected 'fake error'", gotError)
	}
}
