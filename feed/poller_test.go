// Copyright 2017, Square, Inc.

package feed_test

import (
	"testing"
	"time"

	"github.com/square/etre"
	"github.com/square/etre/cdc"
	"github.com/square/etre/feed"
	"github.com/square/etre/test/mock"

	"github.com/go-test/deep"
)

// @todo: figure out a more elegant way to test this wrt controlling when
// clients get registered with the poller.
func TestPoller(t *testing.T) {
	maxTimestampChan := make(chan int64)
	// Create a mock delay manager that will return whatever timestamp we send
	// to it over the maxTimestampChan. It will block until it receives something.
	dm := &mock.DelayManager{
		MaxTimestampFunc: func() (int64, error) {
			return <-maxTimestampChan, nil
		},
	}
	defer close(maxTimestampChan)

	listEventsChan := make(chan []etre.CDCEvent)
	actualFilters := []cdc.Filter{}
	// Create a mock CDC manager that will return whatever events we send
	// to it over the listEventsChan. It will block until it receives something.
	// Also record the filters we receive.
	cdcm := &mock.CDCManager{
		ListEventsFunc: func(filter cdc.Filter) ([]etre.CDCEvent, error) {
			actualFilters = append(actualFilters, filter)
			return <-listEventsChan, nil
		},
	}
	defer close(listEventsChan)

	// Create a poller.
	p := feed.NewPoller(cdcm, dm, 0, 100, 100)
	feed.CurrentTimestamp = func() int64 { return 1 } // current timestamp is 1

	// Stopping before the poller has been started shouldn't do anything.
	p.Stop()

	// Registering with the poller before it has started returns an error.
	_, _, err := p.Register("a")
	if err != feed.ErrPollerNotRunning {
		t.Errorf("error = nil, expected %s", feed.ErrPollerNotRunning)
	}

	// Start the poller.
	p.Start()
	// Starting it again shouldn't do anything.
	p.Start()

	// Cause the dm to return a specific timestamp, and the cdcm to return
	// specific events. Since there aren't any clients registered with the
	// poller right now, these events won't get sent anywhere.
	maxTimestampChan <- 5
	listEventsChan <- []etre.CDCEvent{
		etre.CDCEvent{EventId: "abc"},
		etre.CDCEvent{EventId: "def"},
	}

	// On the next loop, register with the poller before it sends the events
	// for that loop.
	maxTimestampChan <- 12
	c1Events, lastPolledTs, err := p.Register("client1")
	if err != nil {
		t.Error(err)
	}
	listEventsChan <- []etre.CDCEvent{
		etre.CDCEvent{EventId: "ghi"},
		etre.CDCEvent{EventId: "jkl"},
	}

	if lastPolledTs != 5 {
		t.Errorf("lastPolledTs = %d, expectd 5", lastPolledTs)
	}

	// On the next loop, register a second client with the poller before i
	// sends the events for that loop.
	maxTimestampChan <- 23
	c2Events, lastPolledTs, err := p.Register("client2")
	if err != nil {
		t.Error(err)
	}
	listEventsChan <- []etre.CDCEvent{
		etre.CDCEvent{EventId: "mno"},
		etre.CDCEvent{EventId: "pqr"},
	}

	if lastPolledTs != 12 {
		t.Errorf("lastPolledTs = %d, expectd 5", lastPolledTs)
	}

	// On the next loop, stop the poller before it sends events for
	// that loop. This will cause it to stop after the loop finishes.
	maxTimestampChan <- 23
	p.Stop()
	listEventsChan <- []etre.CDCEvent{
		etre.CDCEvent{EventId: "stu"},
		etre.CDCEvent{EventId: "vwx"},
	}

	// Get all the events for the clients. The channel for each client should
	// have a buffer size of 100 (set in NewPoller()) and we only created a
	// handfull of events, so we should be able to see everything the poller
	// sent them. The channels should also be closed now.
	var c1Actual []etre.CDCEvent
POLLED_EVENTS1:
	for {
		select {
		case event, ok := <-c1Events:
			if !ok {
				break POLLED_EVENTS1
			}
			c1Actual = append(c1Actual, event)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for event on client1's channel")
		}
	}
	var c2Actual []etre.CDCEvent
POLLED_EVENTS2:
	for {
		select {
		case event, ok := <-c2Events:
			if !ok {
				break POLLED_EVENTS2
			}
			c2Actual = append(c2Actual, event)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for event on client1's channel")
		}
	}

	c1Expected := []etre.CDCEvent{
		etre.CDCEvent{EventId: "ghi"},
		etre.CDCEvent{EventId: "jkl"},
		etre.CDCEvent{EventId: "mno"},
		etre.CDCEvent{EventId: "pqr"},
		etre.CDCEvent{EventId: "stu"},
		etre.CDCEvent{EventId: "vwx"},
	}

	c2Expected := []etre.CDCEvent{
		etre.CDCEvent{EventId: "mno"},
		etre.CDCEvent{EventId: "pqr"},
		etre.CDCEvent{EventId: "stu"},
		etre.CDCEvent{EventId: "vwx"},
	}

	if diff := deep.Equal(c1Expected, c1Actual); diff != nil {
		t.Error(diff)
	}
	if diff := deep.Equal(c2Expected, c2Actual); diff != nil {
		t.Error(diff)
	}

	// @todo: also test to make sure we send the correct cdc.Filters
}
