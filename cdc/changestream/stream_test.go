// Copyright 2017-2020, Square, Inc.

package changestream_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-test/deep"

	"github.com/square/etre"
	"github.com/square/etre/cdc"
	"github.com/square/etre/cdc/changestream"
	"github.com/square/etre/test/mock"
)

// var events1 in changestream_test.go

func waitUntilClosed(c <-chan etre.CDCEvent) error {
	for {
		select {
		case _, ok := <-c:
			if !ok {
				return nil
			}
		case <-time.After(500 * time.Millisecond):
			return fmt.Errorf("timeout waiting for streamChan to be closed")
		}
	}
}

// --------------------------------------------------------------------------

func TestStreamNow(t *testing.T) {
	// Test streaming from Now()/sinceTs=0, i.e. no backlog. This is the
	// simplest case that basically wires server directly to client via
	// the steamer which is doing translation from etre.CDCEvent events from Mongo
	// to etre.CDCEvent to clients.
	serverChan := make(chan etre.CDCEvent, 1)
	srv := mock.ChangeStreamServer{
		WatchFunc: func() (<-chan etre.CDCEvent, error) {
			return serverChan, nil
		},
	}
	stream := changestream.NewServerStreamer(srv, &mock.CDCStore{})
	streamChan := stream.Start(0) // startTs = 0 = no backlock

	// Almost immediately after staring without a backlog (startTs=0), the streamer
	// should signal that it's in sync by closing it's sync chan
	select {
	case <-stream.InSync():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for InSync()")
	}

	// We haven't sent any events to serverChan, so the streamer shouldn't
	// have received any, either
	var gotEvent etre.CDCEvent
	select {
	case gotEvent = <-streamChan:
		t.Errorf("got event before one expected: %#v", gotEvent)
	default:
	}

	// Status is only Running=true and InSync=true, all other field zero values,
	// because the backlog code didn't run, i.e. we starting Now()/sinceTs=0
	// presumes client is in sync
	gotStatus := stream.Status()
	expectStatus := changestream.Status{
		Running: true,
		InSync:  true,
	}
	if diff := deep.Equal(gotStatus, expectStatus); diff != nil {
		t.Error(diff)
	}

	// Server sends 1 event (real MongoDBServer would do this when MongoDB change stream
	// pushes a raw event). It should be sent immediately to client on streamChan.
	serverChan <- events1[0]
	select {
	case gotEvent = <-streamChan:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for event on streamChan")
	}

	if diff := deep.Equal(gotEvent, events1[0]); diff != nil {
		t.Error(diff)
	}

	// Status should be unchanged
	gotStatus = stream.Status()
	if diff := deep.Equal(gotStatus, expectStatus); diff != nil {
		t.Error(diff)
	}

	// Stop the streamer and check the status
	stream.Stop()
	gotStatus = stream.Status()
	expectStatus = changestream.Status{
		Running: false, // this changes to false after calling Stop
		InSync:  true,
	}
	if diff := deep.Equal(gotStatus, expectStatus); diff != nil {
		t.Error(diff)
	}

	// Stop is idempotent
	stream.Stop()
	gotStatus = stream.Status()
	expectStatus = changestream.Status{
		Running: false, // this changes to false after calling Stop
		InSync:  true,
	}
	if diff := deep.Equal(gotStatus, expectStatus); diff != nil {
		t.Error(diff)
	}
}

func TestStreamBacklogNoNewEvents(t *testing.T) {
	// Test backlog streaming with startTs > 0, i.e. a start time in the past.
	// This is the simple case where there are no current (new) events while
	// streaming the backlog which makes shifting into sync very easy: we're
	// in sync as soon as the backlog is done.
	//
	// Simulating this is easy: just don't send any new events on serverChan.
	serverChan := make(chan etre.CDCEvent, 1)
	srv := mock.ChangeStreamServer{
		WatchFunc: func() (<-chan etre.CDCEvent, error) {
			return serverChan, nil
		},
	}

	// In our mock CDC store, we need to signal to test after streamBacklog()
	// calls store.Read() else there'll be a race condition
	var gotFilter cdc.Filter
	readChan := make(chan struct{})
	store := mock.CDCStore{
		ReadFunc: func(f cdc.Filter) ([]etre.CDCEvent, error) {
			defer close(readChan)
			gotFilter = f
			return events1, nil // in changestream_test.go
		},
	}
	stream := changestream.NewServerStreamer(srv, store)
	nowTs := time.Now().UnixNano() / int64(time.Millisecond)
	stream.Start(100) // []events1 (in changestream_test.go) starts at 100

	// Wait for streamBacklog() to call store.Read()
	select {
	case <-readChan:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for readChan (mock.CDCStore.Read return)")
	}

	// Our startTs should be plumbed all the way through to the store.Read() call.
	// UntilTs is also set to now + changestream.BacklogWait. See streamBacklog() for why.
	// That value is nondeterministic, but it must be < when the test start (nowTs).
	// Also, for the baclog we order by Ts ascending because, internally, the steamer
	// uses an etre.RevOrder to handle out of order events.
	if gotFilter.UntilTs < nowTs {
		t.Errorf("got cdc.Filter.UntilTs = %d, expected >= %d", gotFilter.UntilTs, nowTs)
	}
	gotFilter.UntilTs = 0
	expectFilter := cdc.Filter{
		SinceTs: 100,
		Order:   cdc.ByTsAsc{},
	}
	if diff := deep.Equal(gotFilter, expectFilter); diff != nil {
		t.Error(diff)
	}

	// Since we didn't send any current events on serverChan, the internal buff
	// is zero-length which makes shiftToCurrent() return immediately and backlog()
	// is done. Execution continues to "In Sync Stream" in stream().
	select {
	case <-stream.InSync():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for InSync()")
	}

	// We can tell that the sync was instant because the buffer usage is zero
	gotStatus := stream.Status()
	expectStatus := changestream.Status{
		Running:     true,
		InSync:      true,
		BufferUsage: []int{changestream.ServerBufferSize, 0, 0},
	}
	if diff := deep.Equal(gotStatus, expectStatus); diff != nil {
		t.Error(diff)
	}
}

func TestStreamBacklogNewEvents(t *testing.T) {
	// Test backlog streaming and shift to current when new events after the
	// last backlog event. This is the second easiest case because we know
	// we're in sync if the new events ocurred after everything in the backlog,
	// else they would have been in the backlog.
	//
	// To simulate, we do the backlog as before (previous test ^) but also send
	// the new event on serverChan so it get buffered and the logic in shiftToCurrent()
	// gets triggered. We have to send the new event before or while streamBacklog()
	// is running else it will process the backlog from our mock store and close
	// backlogDoneChan which will close bufferCurrentEvents() before it has a chance
	// to recv and buffer the new event. This is easy to do: when mock store.Read()
	// is called, we know streamBacklog() is running (because it's the caller), so at
	// that point we went the new event which will be recieved by bufferCurrentEvents(),
	// then check the status to ensure it was saved, then unblock store.Read().
	serverChan := make(chan etre.CDCEvent)
	srv := mock.ChangeStreamServer{
		WatchFunc: func() (<-chan etre.CDCEvent, error) {
			return serverChan, nil
		},
	}

	newEvent := etre.CDCEvent{
		EventId:    "4",
		EntityId:   "e2",
		EntityType: "node",
		Rev:        9,
		Ts:         401,
		Op:         "i",
		New: &etre.Entity{
			"_id":   "e1",
			"_type": "node",
			"_rev":  int64(0),
			"foo":   "bar",
		},
	}

	// In our mock CDC store, we need to signal to test after streamBacklog()
	// calls store.Read() else there'll be a race condition
	readChan := make(chan struct{})
	store := mock.CDCStore{
		ReadFunc: func(f cdc.Filter) ([]etre.CDCEvent, error) {
			defer close(readChan)
			serverChan <- newEvent
			return events1, nil // in changestream_test.go
		},
	}
	stream := changestream.NewServerStreamer(srv, store)
	streamChan := stream.Start(100) // []events1 (in changestream_test.go) starts at 100

	// Wait for streamBacklog() to call store.Read()
	timeout := time.After(1 * time.Second)
	select {
	case <-readChan:
	case <-timeout:
		t.Fatal("timeout waiting for readChan (mock.CDCStore.Read return)")
	}
	for {
		select {
		case <-timeout:
			t.Fatal("timeout waiting for new event to be buffered")
		default:
		}
		s := stream.Status()
		if s.BufferUsage[1] == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Since we didn't send any current events on serverChan, the internal buff
	// is zero-length which makes shiftToCurrent() return immediately and backlog()
	// is done. Execution continues to "In Sync Stream" in stream().
	select {
	case <-stream.InSync():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for InSync()")
	}

	// We can tell that the sync was instant because the buffer usage is zero
	gotStatus := stream.Status()
	expectStatus := changestream.Status{
		Running:     true,
		InSync:      true,
		BufferUsage: []int{changestream.ServerBufferSize, 1, 1},
	}
	if diff := deep.Equal(gotStatus, expectStatus); diff != nil {
		t.Logf("%+v", gotStatus)
		t.Error(diff)
	}

	// Get all events sent to client which should be backlog + newEvent
	gotEvents := []etre.CDCEvent{}
	for len(streamChan) > 0 {
		e := <-streamChan
		gotEvents = append(gotEvents, e)
	}
	expectEvents := make([]etre.CDCEvent, len(events1)+1)
	copy(expectEvents, events1)
	expectEvents[len(expectEvents)-1] = newEvent
	if diff := deep.Equal(gotEvents, expectEvents); diff != nil {
		t.Error(diff)
	}

	// Stop the streamer and check the status. This is important becuase the backlog
	// starts and coordinates several goroutines, so Stop() shouldn't hang waiting for
	// goroutines to return (they should have already returned when backlog() returned).
	stream.Stop()
	gotStatus = stream.Status()
	expectStatus = changestream.Status{
		Running:     false,
		InSync:      true,
		BufferUsage: []int{changestream.ServerBufferSize, 1, 1},
	}
	if diff := deep.Equal(gotStatus, expectStatus); diff != nil {
		t.Error(diff)
	}
}

func TestStreamBacklogNewOverlappingEvents(t *testing.T) {
	// Like previous test, but now the new events overlap with backlog events
	// which can happen if the MongoDB change stream starts in the middle of a big
	// batch of writes. See code comments about BacklogWait in streamBacklog().
	serverChan := make(chan etre.CDCEvent)
	srv := mock.ChangeStreamServer{
		WatchFunc: func() (<-chan etre.CDCEvent, error) {
			return serverChan, nil
		},
	}

	// The "new" events are really the last 2 events from the backlog.
	// The revorder will filter out these dupes.
	newEvents := events1[2:]

	// In our mock CDC store, we need to signal to test after streamBacklog()
	// calls store.Read() else there'll be a race condition
	readChan := make(chan struct{})
	store := mock.CDCStore{
		ReadFunc: func(f cdc.Filter) ([]etre.CDCEvent, error) {
			defer close(readChan)
			for _, e := range newEvents {
				serverChan <- e
			}
			return events1, nil
		},
	}
	stream := changestream.NewServerStreamer(srv, store)
	streamChan := stream.Start(100) // []events1 (in changestream_test.go) starts at 100

	// Wait for streamBacklog() to call store.Read()
	timeout := time.After(1 * time.Second)
	select {
	case <-readChan:
	case <-timeout:
		t.Fatal("timeout waiting for readChan (mock.CDCStore.Read return)")
	}
	for {
		select {
		case <-timeout:
			t.Fatal("timeout waiting for new event to be buffered")
		default:
		}
		s := stream.Status()
		if s.BufferUsage[1] == 2 { // we send events1[2:] which is 2 events
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Since we didn't send any current events on serverChan, the internal buff
	// is zero-length which makes shiftToCurrent() return immediately and backlog()
	// is done. Execution continues to "In Sync Stream" in stream().
	select {
	case <-stream.InSync():
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timeout waiting for InSync(); ServerStreamer.Err: %v", stream.Error())
	}

	// Get all events sent to client which should be backlog without the dupe "new" events
	gotEvents := []etre.CDCEvent{}
	for len(streamChan) > 0 {
		e := <-streamChan
		gotEvents = append(gotEvents, e)
	}
	if diff := deep.Equal(gotEvents, events1); diff != nil {
		t.Error(diff)
	}
}

func TestStreamNewEventsOutOfOrder(t *testing.T) {
	// Test that revorder is keeping events in sync. Both backlog and stream events
	// pass through sendToClient() which uses the etre.RevOrder, so here we simulate
	// the stream sending out of order revs.
	//
	// Unit tests for etre.RevOrder ensure it works, but this test is ensuring it's
	// also working in this code.
	serverChan := make(chan etre.CDCEvent)
	srv := mock.ChangeStreamServer{
		WatchFunc: func() (<-chan etre.CDCEvent, error) {
			return serverChan, nil
		},
	}
	stream := changestream.NewServerStreamer(srv, &mock.CDCStore{})
	streamChan := stream.Start(0) // no backlog

	// The events are in order in events1, so we scramble them and send to client
	// via serverChan (as if MongoDB streamed the events in this order)
	newEvents := []etre.CDCEvent{
		events1[0],
		events1[3],
		events1[1],
		events1[2],
	}
	for _, e := range newEvents {
		serverChan <- e
	}

	// Get all events sent to client which should events1 in order
	gotEvents := []etre.CDCEvent{}
	timeout := time.After(1 * time.Second)
	for {
		select {
		case e := <-streamChan:
			gotEvents = append(gotEvents, e)
		case <-timeout:
			t.Fatal("timeout waiting for events")
		default:
		}
		if len(gotEvents) == 4 {
			break
		}
	}
	if diff := deep.Equal(gotEvents, events1); diff != nil {
		t.Error(diff)
	}
}

func TestStreamServerClosedStreamDuringBacklog(t *testing.T) {
	// Test that everything stops when the server closes its stream. This could happen
	// if MongoDB closes the change stream. Test with backlog so all the goroutines
	// are started.
	clientBuffSize := changestream.ClientBufferSize
	defer func() {
		changestream.ClientBufferSize = clientBuffSize
	}()
	changestream.ClientBufferSize = 0
	serverChan := make(chan etre.CDCEvent, 1) // we'll close this
	srv := mock.ChangeStreamServer{
		WatchFunc: func() (<-chan etre.CDCEvent, error) {
			return serverChan, nil
		},
	}
	readCalledChan := make(chan struct{}) // closed by Read when it's called
	readReturnChan := make(chan struct{}) // close to let Read return
	store := mock.CDCStore{
		ReadFunc: func(f cdc.Filter) ([]etre.CDCEvent, error) {
			close(readCalledChan)
			<-readReturnChan
			// Sending 1 backlog event
			return []etre.CDCEvent{events1[0]}, nil // in changestream_test.go
		},
	}
	stream := changestream.NewServerStreamer(srv, store)
	streamChan := stream.Start(100)

	// Wait for streamBacklog() to call store.Read()
	select {
	case <-readCalledChan:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for readCalledChan")
	}

	// Simulate MongoDB closing the change stream
	close(serverChan)

	// Let Read in streamBacklog() return which will try to send that 1 backlog
	// event but block because we set changestream.ClientBufferSize = 0
	close(readReturnChan)

	// Get that 1 backlog event, then wait for ServerStreamer to close client chan
	// (last thing it does on shutdown)
	if err := waitUntilClosed(streamChan); err != nil {
		t.Fatal(err)
	}

	// Status should reflect that we're not running, were not in sync, did have the
	// backlog buffer, and the server closed the stream
	gotStatus := stream.Status()
	expectStatus := changestream.Status{
		Running:            false,
		InSync:             false,
		BufferUsage:        []int{changestream.ServerBufferSize, 0, 0},
		ServerClosedStream: true,
	}
	if diff := deep.Equal(gotStatus, expectStatus); diff != nil {
		t.Error(diff)
	}

	// Error() should return changestream.ErrServerClosedStream
	err := stream.Error()
	if err != changestream.ErrServerClosedStream {
		t.Errorf("got error '%s', expected '%s' (changestream.ErrServerClosedStream)",
			err, changestream.ErrServerClosedStream)
	}
}

func TestStreamServerClosedStreamDuringSync(t *testing.T) {
	// Test same as previous but don't backlog so we go right to the main sync loop
	serverChan := make(chan etre.CDCEvent, 1) // we'll close this
	srv := mock.ChangeStreamServer{
		WatchFunc: func() (<-chan etre.CDCEvent, error) {
			return serverChan, nil
		},
	}
	stream := changestream.NewServerStreamer(srv, &mock.CDCStore{})
	streamChan := stream.Start(0)

	// Wait until code gets to the in sync loop that we're testing
	select {
	case <-stream.InSync():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for InSync()")
	}

	// Simulate MongoDB closing the change stream
	close(serverChan)

	// Wait for ServerStreamer to close client chan (last thing it does on shutdown)
	if err := waitUntilClosed(streamChan); err != nil {
		t.Fatal(err)
	}

	// Status should reflect that we were in sync and the server closed the stream.
	// No BufferUsage becuse we didn't have a backlog.
	gotStatus := stream.Status()
	expectStatus := changestream.Status{
		Running:            false,
		InSync:             true,
		ServerClosedStream: true,
	}
	if diff := deep.Equal(gotStatus, expectStatus); diff != nil {
		t.Error(diff)
	}

	// changestream.ErrServerClosedStream should be reported
	err := stream.Error()
	if err != changestream.ErrServerClosedStream {
		t.Errorf("got error '%s', expected '%s' (changestream.ErrServerClosedStream)",
			err, changestream.ErrServerClosedStream)
	}
}
