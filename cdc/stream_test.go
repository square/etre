// Copyright 2017, Square, Inc.

package cdc_test

import (
	"testing"
	"time"

	"github.com/square/etre"
	"github.com/square/etre/cdc"
	"github.com/square/etre/test/mock"

	"github.com/go-test/deep"
)

func TestStreamer(t *testing.T) {
	pollerEvents := make(chan etre.CDCEvent)
	maxPolledTs := make(chan int64)
	var deregisterCount int
	// Create a mock Poller.
	p := &mock.Poller{
		RegisterFunc: func(string) (<-chan etre.CDCEvent, int64, error) {
			return pollerEvents, <-maxPolledTs, nil
		},
		DeregisterFunc: func(string) {
			deregisterCount++
		},
	}

	// Create a mock CDC Store.
	storeEvents := make(chan []etre.CDCEvent)
	var lastFilterRecvd cdc.Filter
	cdcs := &mock.CDCStore{
		ReadFunc: func(filter cdc.Filter) ([]etre.CDCEvent, error) {
			lastFilterRecvd = filter
			return <-storeEvents, nil
		},
	}

	// Create an internal feed which is basically a light wrapper around a streamer.
	f := cdc.NewInternalFeed(10, p, cdcs)
	startTs := int64(1)
	feedChan := f.Start(startTs)

	//
	// Make the streamer loop through 3 chunks of past events before catching up
	// to the poller.
	//

	// The poller starts off 9999999 milliseconds ahead of the streamer.
	maxPolledTs <- 10000000

	// Make the Store return some events.
	events1 := []etre.CDCEvent{
		etre.CDCEvent{EventId: "abc", Ts: 2000}, // these timestamps have to be greater than startTs
		etre.CDCEvent{EventId: "def", Ts: 3000},
		etre.CDCEvent{EventId: "ghi", Ts: 4000},
	}
	storeEvents <- events1

	// Make the Store return some more events.
	events2 := []etre.CDCEvent{
		etre.CDCEvent{EventId: "jkl", Ts: 4200000},
	}
	storeEvents <- events2

	maxPolledTs <- 10020000

	// This is the last iteration of producing old events.
	events3 := []etre.CDCEvent{
		etre.CDCEvent{EventId: "mno", Ts: 8000000},
	}
	storeEvents <- events3

	//
	// Make the poller produce events, which will cause the streamer to send them
	// to the feed.
	//

	events4 := []etre.CDCEvent{
		etre.CDCEvent{EventId: "ppp", Ts: 12001000},
		etre.CDCEvent{EventId: "qqq", Ts: 12002000},
		etre.CDCEvent{EventId: "rrr", Ts: 12003000},
	}
	for _, event := range events4 {
		pollerEvents <- event
	}

	//
	// Verify the streamer has sent all of the events to the feed that we expect.
	//

	expectedEvents := events1
	expectedEvents = append(expectedEvents, events2...)
	expectedEvents = append(expectedEvents, events3...)

	var recvdEvents []etre.CDCEvent
	for i := 0; i < len(expectedEvents); i++ {
		recvdEvent := <-feedChan
		recvdEvents = append(recvdEvents, recvdEvent)
	}
	if diff := deep.Equal(recvdEvents, expectedEvents); diff != nil {
		t.Error(diff)
	}

	//
	// Close the pollers's event channel, which will cause the streamer to stop
	// which will then cause the feed to error out and stop.
	//

	close(pollerEvents)
	// Give client a few milliseconds to shutdown
	time.Sleep(500 * time.Millisecond)

	if f.Error() != cdc.ErrStreamerLag {
		t.Errorf("err = %s, expected %s", f.Error(), cdc.ErrStreamerLag)
	}

	//
	// Verify the last filter provided to Store.Read looks coorect.
	//

	expectedFilter := cdc.Filter{
		SinceTs: 7200001,
		UntilTs: 10020000,
	}
	if diff := deep.Equal(lastFilterRecvd, expectedFilter); diff != nil {
		t.Error(diff)
	}
}

// Test when the streamer skips immediately to the poller.
func TestStreamerImmediatelyPoll(t *testing.T) {
	pollerEvents := make(chan etre.CDCEvent)
	maxPolledTs := make(chan int64)
	var deregisterCount int
	// Create a mock Poller.
	p := &mock.Poller{
		RegisterFunc: func(string) (<-chan etre.CDCEvent, int64, error) {
			return pollerEvents, <-maxPolledTs, nil
		},
		DeregisterFunc: func(string) {
			deregisterCount++
		},
	}

	// Create an internal feed which is basically a light wrapper around a streamer.
	f := cdc.NewInternalFeed(100, p, &mock.CDCStore{})
	startTs := int64(100)
	feedChan := f.Start(startTs)

	//
	// Make the streamer jump straight to the poller by returning a maxPooledTs
	// that is less than startTs
	//
	//

	maxPolledTs <- 1

	//
	// Make the poller produce events, which will cause the streamer to send them
	// to the feed.
	//

	events := []etre.CDCEvent{
		etre.CDCEvent{EventId: "ppp", Ts: 12001000},
		etre.CDCEvent{EventId: "qqq", Ts: 12002000},
		etre.CDCEvent{EventId: "rrr", Ts: 12003000},
	}
	for _, event := range events {
		pollerEvents <- event
	}

	//
	// Verify the streamer has sent all of the events to the feed that we expect.
	//

	var recvdEvents []etre.CDCEvent
	for i := 0; i < len(events); i++ {
		recvdEvent := <-feedChan
		recvdEvents = append(recvdEvents, recvdEvent)
	}
	if diff := deep.Equal(recvdEvents, events); diff != nil {
		t.Error(diff)
	}

	//
	// Close the pollers's event channel, which will cause the streamer to stop
	// which will then cause the feed to error out and stop.
	//

	close(pollerEvents)
	// Give client a few milliseconds to shutdown
	time.Sleep(500 * time.Millisecond)

	if f.Error() != cdc.ErrStreamerLag {
		t.Errorf("err = %s, expected %s", f.Error(), cdc.ErrStreamerLag)
	}
}
