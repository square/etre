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

func TestPoller(t *testing.T) {
	maxTimestampChan := make(chan int64)
	// Create a mock delayer that will return whatever timestamp we send
	// to it over the maxTimestampChan. It will block until it receives something.
	d := &mock.Delayer{
		MaxTimestampFunc: func() (int64, error) {
			return <-maxTimestampChan, nil
		},
	}
	defer close(maxTimestampChan)

	readEventsChan := make(chan []etre.CDCEvent)
	actualFilters := []cdc.Filter{}
	// Create a mock CDC store that will return whatever events we send
	// to it over the readEventsChan. It will block until it receives something.
	// Also record the filters we receive.
	cdcs := &mock.CDCStore{
		ReadFunc: func(filter cdc.Filter) ([]etre.CDCEvent, error) {
			actualFilters = append(actualFilters, filter)
			return <-readEventsChan, nil
		},
	}
	defer close(readEventsChan)

	// Create a mock pollInterval ticker that we will use to control the
	// poller from this test.
	pollIntervalChan := make(chan time.Time)
	pollInterval := &time.Ticker{
		C: pollIntervalChan,
	}

	// Create a poller.
	p := cdc.NewPoller(cdcs, d, 100, pollInterval)
	cdc.CurrentTimestamp = func() int64 { return 1 } // current timestamp is 1

	// Registering with the poller before it has started returns an error.
	_, _, err := p.Register("a")
	if err != cdc.ErrPollerNotRunning {
		t.Errorf("error = nil, expected %s", cdc.ErrPollerNotRunning)
	}

	// Run the poller.
	go p.Run()

	// Cause the delayer to return a specific timestamp, and the cdcs to return
	// specific events. Since there aren't any clients registered with the
	// poller right now, these events won't get sent anywhere.
	maxTimestampChan <- 5
	readEventsChan <- []etre.CDCEvent{
		etre.CDCEvent{EventId: "abc"},
		etre.CDCEvent{EventId: "def"},
	}

	// Allow the poller to progress through the next loop.
	pollIntervalChan <- time.Now()

	// Register a client with the poller.
	c1Events, lastPolledTs, err := p.Register("client1")
	if err != nil {
		t.Error(err)
	}

	if lastPolledTs != 5 {
		t.Errorf("lastPolledTs = %d, expectd 5", lastPolledTs)
	}

	maxTimestampChan <- 12
	readEventsChan <- []etre.CDCEvent{
		etre.CDCEvent{EventId: "ghi"},
		etre.CDCEvent{EventId: "jkl"},
	}

	// Allow the poller to progress through the next loop.
	pollIntervalChan <- time.Now()

	// Register another client with the poller.
	c2Events, lastPolledTs, err := p.Register("client2")
	if err != nil {
		t.Error(err)
	}

	if lastPolledTs != 12 {
		t.Errorf("lastPolledTs = %d, expectd 12", lastPolledTs)
	}

	maxTimestampChan <- 23
	readEventsChan <- []etre.CDCEvent{
		etre.CDCEvent{EventId: "mno"},
		etre.CDCEvent{EventId: "pqr"},
	}

	// Allow the poller to progress through the next loop.
	pollIntervalChan <- time.Now()

	// Deregister the first client from the poller.
	p.Deregister("client1")

	maxTimestampChan <- 28
	readEventsChan <- []etre.CDCEvent{
		etre.CDCEvent{EventId: "stu"},
		etre.CDCEvent{EventId: "vwx"},
	}

	// Allow the poller to progress through the next loop.
	pollIntervalChan <- time.Now()

	// Deregister the second client from the poller.
	p.Deregister("client2")

	// Since both clients have been deregistered, they should not see the
	// events from this loop.
	maxTimestampChan <- 31
	readEventsChan <- []etre.CDCEvent{
		etre.CDCEvent{EventId: "yz1"},
		etre.CDCEvent{EventId: "234"},
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
