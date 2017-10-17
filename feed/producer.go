// Copyright 2017, Square, Inc.

package feed

import (
	"errors"
	"sync"
	"time"

	"github.com/square/etre"
	"github.com/square/etre/cdc"

	log "github.com/Sirupsen/logrus"
)

var (
	ErrProducerLag = errors.New("producer fell too far behind the CDC event poller")
)

// A Producer produces a stream of CDC events on a channel. It can start
// producing events from any point in the past, slowly making its way closer and
// closer to present events. Once it catches up to live events that are being
// created in real-time, it will seamlessly transition to producing those.
//
// Producers are intended to be used by Feeds to produce the events that a feed
// needs to send to its client. A Feed should only ever have one Producer.
type Producer interface {
	// Start starts producing events from a given timestamp, chunking by the
	// given chunkWindow (ex: if startTs is 1 and chunkWindow is 60, it will
	// first produce events with 1 <= eventTs < 61, followed by events with
	// 61 <= eventTs < 121, etc.). It returns a channel on which the caller
	// can receive all of the events the Producer produces for as long as
	// the producer is running. A Producer will run until Stop is called or
	// until it encounters an error.
	Start(startTs int64, chunkWindow int) <-chan etre.CDCEvent

	// Stop stops the producer and closes the event channel returned by Start.
	Stop()

	// Error returns the error that caused the Producer to stop. Start resets
	// the error.
	Error() error
}

type producer struct {
	poller   Poller
	cdcm     cdc.Manager
	logEvery int // log every N seconds
	// --
	feedId      string             // id of the feed that created this producer
	events      chan etre.CDCEvent // channel on which events are produced
	err         error              // the last error the producer encountered
	running     bool               // true while the producer is running
	stopChan    chan struct{}      // channel that gets closed when Stop is called
	*sync.Mutex                    // guard function calls
	logger      *log.Entry         // used for logging
}

func NewProducer(poller Poller, cdcm cdc.Manager, logEvery int, feedId string) Producer {
	return &producer{
		poller:   poller,
		cdcm:     cdcm,
		logEvery: logEvery,
		feedId:   feedId,
		logger:   log.WithFields(log.Fields{"feedId": feedId}),
		Mutex:    &sync.Mutex{},
	}
}

type ProducerFactory interface {
	Make(feedId string) Producer
}

type producerFactory struct {
	poller   Poller
	cdcm     cdc.Manager
	logEvery int // log every N seconds
}

func NewProducerFactory(poller Poller, cdcm cdc.Manager, logEvery int) ProducerFactory {
	return &producerFactory{
		poller:   poller,
		cdcm:     cdcm,
		logEvery: logEvery,
	}
}

func (pf *producerFactory) Make(feedId string) Producer {
	return NewProducer(pf.poller, pf.cdcm, pf.logEvery, feedId)
}

func (p *producer) Start(startTs int64, chunkWindow int) <-chan etre.CDCEvent {
	p.Lock()
	defer p.Unlock()
	p.logger.Info("starting producer")

	if p.running {
		p.logger.Info("producer already running")
		return p.events
	}

	p.running = true
	p.err = nil
	p.events = make(chan etre.CDCEvent)
	p.stopChan = make(chan struct{})

	go p.produce(startTs, chunkWindow)
	return p.events
}

func (p *producer) Stop() {
	p.Lock()
	defer p.Unlock()
	p.logger.Info("stopping producer")

	if !p.running {
		p.logger.Info("producer not running")
		return
	}
	p.running = false

	// Close the stop channel, but DO NOT close the events channel (that
	// must happen within the private produce method below to avoid sending
	// on a closed channel).
	close(p.stopChan)
}

func (p *producer) Error() error {
	p.Lock()
	defer p.Unlock()
	return p.err
}

// -------------------------------------------------------------------------- //

func (p *producer) produce(startTs int64, chunkWindow int) {
	var newEvents <-chan etre.CDCEvent
	var lastChunk bool
	var maxPolledTs int64
	var err error
	var registeredWithPoller bool
	logTicker := time.NewTicker(time.Duration(p.logEvery) * time.Second)

	// sinceTs is the lower-bound timestamp that the producer will select events
	// on as it chugs along. It will get updated after each chunk of events.
	sinceTs := startTs

	// Deregsiter the producer from the poller when we're done.
	defer p.poller.Deregister(p.feedId)

	// Retrieve CDC events from the past (i.e., not a live stream of new events).
	p.logger.Infof("starting to produce past CDC events (startTs=%d, chunkWindow=%d)", startTs, chunkWindow)
PAST_EVENTS:
	// This loop works by producing past events between incremental chunks of
	// time. The goal is to eventually catch up to where the poller is and then
	// switch over to producing events from the poller instead of chunking
	// through past events. At the beginning of each loop we register with the
	// poller to see if we are close to catching up to it. If we're not, we
	// deregister and continue chunking through past events. If we are, then
	// we either switch to the poller immediately or produce one last chunk
	// before switching over.
	for {
		// If the producer was stopped, close the events channel and return.
		select {
		case <-p.stopChan:
			close(p.events)
			return
		default:
		}

		// The default upper-bound timestamp for this chunk is the lower-
		// bound timestamp plus the chunkWindow size.
		untilTs := sinceTs + int64(chunkWindow*1000) // convert chunkWindow from s to ms

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
		// chunk, just jump straight to producing new events from the
		// poller and skip producing past events. This can only ever
		// happen on the very first iteration of this loop.
		if maxPolledTs <= sinceTs {
			log.Info("skip straight to poller")
			break PAST_EVENTS
		}

		// If the upper-bound timestamp is greater than the timestamp that
		// the poller last polled before we registered with it, use the
		// poller's timetamp as the upper-bound instead. Also, if that's
		// the case, this will be our last chunk of producing past events
		// before we switch over to the poller.
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

		// Log current status every so often to prevent flooding the logs.
		select {
		case <-logTicker.C:
			p.logger.Infof("feed: still producing past CDC events (current chunk is from %d until %d)", sinceTs, untilTs)
		default:
		}

		// Get the CDC events between our lower-bound and upper-bound
		// timestamps, ordering by entity id and revision.
		filter := cdc.Filter{
			SinceTs: sinceTs,
			UntilTs: untilTs,
			SortBy:  cdc.SORT_BY_ENTID_REV,
		}
		events, err := p.cdcm.ListEvents(filter)
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
			p.logger.Info("producer has caught up with live events, switching to poller")
			break PAST_EVENTS
		}

		// Set the lower-bound timestamp for the next chunk to be
		// equal to the upper-bound timestamp of the current chunk.
		sinceTs = untilTs
	}

	// Retrieve new, live CDC events from the poller.
	p.logger.Info("starting to produce new events")
NEW_EVENTS:
	// This loop works by producing new events that come from the poller.
	for {
		// If the producer was stopped, close the events channel and return.
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
			// timestamp the producer was started with. This can happen when
			// startTs is greater than the maxPolledTs of the poller when
			// this producer was started.
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
				p.err = ErrProducerLag
			}
			return
		}
	}
	p.logger.Info("finished producing events")
}
