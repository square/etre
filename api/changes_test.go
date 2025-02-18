// Copyright 2017-2020, Square, Inc.

package api_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/square/etre"
	"github.com/square/etre/cdc/changestream"
	"github.com/square/etre/test/mock"
)

func TestChanges(t *testing.T) {
	server := setup(t, defaultConfig, mock.EntityStore{})
	defer server.ts.Close()

	streamChan := make(chan etre.CDCEvent, 1)
	var gotSinceTs int64
	stream := mock.Stream{
		StartFunc: func(sinceTs int64) <-chan etre.CDCEvent {
			gotSinceTs = sinceTs
			return streamChan
		},
	}

	server.streamerFactory.MakeFunc = func(clientId string) changestream.Streamer {
		return stream
	}

	wsURL := strings.Replace(server.url, "http", "ws", 1)
	client := etre.NewCDCClient(wsURL, nil, 10, true)
	eventsChan, err := client.Start(time.Time{})
	require.NoError(t, err)

	latency := client.Ping(time.Duration(300 * time.Millisecond))
	assert.False(t, latency.Send > 100 || latency.Recv > 100 && latency.RTT > 100, "Ping latency > 100ms, expected near-zero: %+v", latency)

	mux := &sync.Mutex{}
	events := []etre.CDCEvent{}
	doneChan := make(chan struct{})
	go func() {
		for e := range eventsChan {
			mux.Lock()
			events = append(events, e)
			mux.Unlock()
		}
		close(doneChan)
	}()

	timeout := time.After(1 * time.Second)

	select {
	case streamChan <- mock.CDCEvents[0]:
	case <-timeout:
		t.Fatal("timeout sending to stremaChan")
	}

	haveEvents := false
	for !haveEvents {
		mux.Lock()
		haveEvents = len(events) > 0
		mux.Unlock()
		time.Sleep(20 * time.Millisecond)
		select {
		case <-timeout:
			t.Fatal("timeout waiting for events")
		default:
		}
	}

	client.Stop()

	select {
	case <-doneChan:
	case <-timeout:
		t.Fatal("timeout waiting for recv goroutine to finish")
	}

	assert.Equal(t, mock.CDCEvents[0:1], events)
	assert.Equal(t, int64(0), gotSinceTs)

	/*
		@todo Fix data race
		// -- Metrics -----------------------------------------------------------
		expectMetrics := []mock.MetricMethodArgs{
			{Method: "Inc", Metric: metrics.CDCClients, IntVal: 1},  // client connect
			{Method: "Inc", Metric: metrics.CDCClients, IntVal: -1}, // client disconnect
		}
		if diffs := deep.Equal(server.metricsrec.Called, expectMetrics); diffs != nil {
			t.Logf("   got: %+v", server.metricsrec.Called)
			t.Logf("expect: %+v", expectMetrics)
			t.Error(diffs)
		}
	*/
}
