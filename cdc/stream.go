// Copyright 2017, Square, Inc.

package cdc

import (
	"errors"
	"sync"
	"time"

	"github.com/square/etre"

	log "github.com/Sirupsen/logrus"
)

const (
	// @todo: make this dynamically generated
	DEFAULT_CHUNK_WINDOW = 3600000 // milliseconds
)

var (
	ErrStreamerLag = errors.New("streamer fell too far behind the CDC event poller")
)

// A streamer produces a stream of CDC events on a channel. It can start
// streaming events from any point in the past, continually making its way closer
// and closer to present events. Once it catches up to new events that are being
// created in real-time, it seamlessly transitions to streaming those.
//
// streamers are used by feeds to produce the events that a feed sends to its
// client. A feed can only have one streamer.
type streamer interface {
	// Start starts streaming events from a given timestamp. It returns a
	// channel on which the caller can receive all of the events it streams
	// for as long as the streamer is running. A streamer runs until Stop
	// is called or until it encounters an error.
	Start(startTs int64) <-chan etre.CDCEvent

	// Stop stops the streamer and closes the event channel returned by Start.
	Stop()

	// Error returns the error that caused the streamer to stop. Start resets
	// the error.
	Error() error
}

// realStreamer implements the streamer interface by producing a stream of
// events from the Store and Poller.
type realStreamer struct {
	poller Poller
	cdcs   Store
	// --
	feedId      string             // id of the feed that created this streamer
	events      chan etre.CDCEvent // channel on which events are streamd
	err         error              // the last error the streamer encountered
	running     bool               // true while the streamer is running
	stopChan    chan struct{}      // channel that gets closed when Stop is called
	*sync.Mutex                    // guard function calls
	logger      *log.Entry         // used for logging
}

// newStreamer creates a streamer that uses the provided Poller and Store
// to stream events. feedId is the id of the feed that created it.
func newStreamer(poller Poller, cdcs Store, feedId string) streamer {
	return &realStreamer{
		poller: poller,
		cdcs:   cdcs,
		feedId: feedId,
		logger: log.WithFields(log.Fields{"feedId": feedId}),
		Mutex:  &sync.Mutex{},
	}
}

func (p *realStreamer) Start(startTs int64) <-chan etre.CDCEvent {
	p.Lock()
	defer p.Unlock()
	p.logger.Info("starting streamer")

	if p.running {
		p.logger.Info("streamer already running")
		return p.events
	}

	p.running = true
	p.err = nil
	p.events = make(chan etre.CDCEvent)
	p.stopChan = make(chan struct{})

	go p.stream(startTs)
	return p.events
}

func (p *realStreamer) Stop() {
	p.Lock()
	defer p.Unlock()
	p.logger.Info("stopping streamer")

	if !p.running {
		p.logger.Info("streamer not running")
		return
	}
	p.running = false

	// Close the stop channel, but DO NOT close the events channel (that
	// must happen within the private stream method below to avoid sending
	// on a closed channel).
	close(p.stopChan)
}

func (p *realStreamer) Error() error {
	p.Lock()
	defer p.Unlock()
	return p.err
}

// -------------------------------------------------------------------------- //

func (p *realStreamer) stream(startTs int64) {
	var newEvents <-chan etre.CDCEvent
	var lastChunk bool
	var maxPolledTs int64
	var err error
	var registeredWithPoller bool
	logInterval := time.NewTicker(time.Duration(DEFAULT_LOG_INTERVAL) * time.Second)

	// sinceTs is the lower-bound timestamp that the streamer selects events
	// on as it chugs along. It gets updated after each chunk of events.
	sinceTs := startTs

	// Deregsiter the streamer from the poller when we're done.
	defer p.poller.Deregister(p.feedId)

	// Retrieve CDC events from the past (i.e., not a live stream of new events).
	p.logger.Infof("starting to stream past CDC events (startTs=%d, chunkWindow=%d)", startTs, DEFAULT_CHUNK_WINDOW)
PAST_EVENTS:
	// This loop works by streaming past events between incremental chunks of
	// time. The goal is to eventually catch up to where the poller is and then
	// switch over to streaming events from the poller instead of chunking
	// through past events. At the beginning of each loop we register with the
	// poller to see if we are close to catching up to it. If we're not, we
	// deregister and continue chunking through past events. If we are, then
	// we either switch to the poller immediately or stream one last chunk
	// before switching over.
	for {
		// If the streamer was stopped, close the events channel and return.
		select {
		case <-p.stopChan:
			close(p.events)
			return
		default:
		}

		// The default upper-bound timestamp for this chunk is the lower-
		// bound timestamp plus the chunkWindow size.
		untilTs := sinceTs + DEFAULT_CHUNK_WINDOW

		// Register with the poller. Only do this when we think we might
		// be close to catching up to it (or if this is the first loop
		// in this function). There's no reason to go through the extra
		// work of registering/deregistering with the poller if we know
		// that the upper- bound of the last chunk is still less than the
		// max timestamp the poller was at when we last registered with it.
		if untilTs >= maxPolledTs {
			newEvents, maxPolledTs, err = p.poller.Register(p.feedId)
			if err != nil {
				p.Lock()
				p.err = err
				p.Unlock()
				return
			}
			registeredWithPoller = true
		}

		// If the poller is behind the lower-bound timestamp for this
		// chunk, just jump straight to streaming new events from the
		// poller and skip streaming past events. This can only ever
		// happen on the very first iteration of this loop.
		if maxPolledTs <= sinceTs {
			log.Info("skip straight to poller")
			break PAST_EVENTS
		}

		// If the upper-bound timestamp is greater than the timestamp that
		// the poller last polled before we registered with it, use the
		// poller's timetamp as the upper-bound instead. Also, if that's
		// the case, this is our last chunk of streaming past events before
		// we switch over to the poller.
		if untilTs >= maxPolledTs {
			untilTs = maxPolledTs
			lastChunk = true
		} else {
			// We are still at least one chunk of past events behind
			// the poller. Deregister from the poller for now while we
			// try to catch up to it.
			if registeredWithPoller {
				p.poller.Deregister(p.feedId)
				registeredWithPoller = false
			}
		}

		// Log current status every so often.
		select {
		case <-logInterval.C:
			p.logger.Infof("feed: still streaming past CDC events (current chunk is from %d until %d)", sinceTs, untilTs)
		default:
		}

		// Get the CDC events between our lower-bound and upper-bound
		// timestamps.
		filter := Filter{
			SinceTs: sinceTs,
			UntilTs: untilTs,
		}
		events, err := p.cdcs.Read(filter)
		if err != nil {
			p.Lock()
			p.err = err
			p.Unlock()
			return
		}

		// Send the CDC events to the client.
		for _, event := range events {
			p.events <- event
		}

		if lastChunk {
			p.logger.Info("streamer has caught up with live events, switching to poller")
			break PAST_EVENTS
		}

		// Set the lower-bound timestamp for the next chunk to be
		// equal to the upper-bound timestamp of the current chunk.
		sinceTs = untilTs
	}

	// Retrieve new, live CDC events from the poller.
	p.logger.Info("starting to stream new events")
NEW_EVENTS:
	// This loop works by streaming new events that come from the poller.
	for {
		// If the streamer was stopped, close the events channel and return.
		select {
		case <-p.stopChan:
			close(p.events)
			break NEW_EVENTS
		default:
		}

		// Publish new events to the client as they are received from the poller. If
		// the poller closes the events channel, that means that either the poller
		// stopped or this feed fell too far behind the poller.
		event, more := <-newEvents
		if more {
			// Filter out any events that have a timestamp earlier than the
			// timestamp the streamer was started with. This can happen when
			// startTs is greater than the maxPolledTs of the poller when
			// this streamer was started.
			if event.Ts >= startTs {
				p.events <- event
			}
		} else {
			err := p.poller.Error()
			p.Lock()
			defer p.Unlock()
			if err != nil {
				p.err = err
			} else {
				// If the poller is fine, that means this feed fell too far
				// behind it to catch up to it (that is the only other way,
				// outside of an error in the poller, that the newEvents
				// channel would be closed).
				p.err = ErrStreamerLag
			}

			// Close the events channel and return.
			close(p.events)
			return
		}
	}
	p.logger.Info("finished streaming events")
}
