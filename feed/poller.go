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
	ErrPollerNotRunning = errors.New("poller is not running")
)

// A Poller will continuously poll for new CDC events while it is running,
// broadcasting each event that it encounters to all of its registered clients.
// There should only ever be one Poller per Etre instance. If any Etre
// components need access to a stream of new CDC events, they should register
// themselves with the Poller.
type Poller interface {
	// Start starts polling for new CDC events. Once the Poller is running,
	// clients can register with it to receive a stream of these events.
	Start()

	// Stop stops the Poller and deregisters all clients.
	Stop()

	// Register registers a client with the Poller. It takes a brief lock
	// on the Poller to ensure that we get a consistent snapshot of when
	// the client is registered. It returns a buffered channel that the
	// client can select on to get all events that the Poller polls after
	// the client is registered. It also returns a timestamp which
	// represents the last upper-bound timestamp that it polled before the
	// client was registered. The client can assume that all events after
	// this timestamp will come through the aforementioned channel. If the
	// buffer on the channel fills up (if the client isn't consuming from
	// it fast enough), the Poller will automatically deregister the client.
	//
	// The only argument that register takes is a string (UUID) that can be
	// used to uniquely identify a client.
	Register(uuid string) (<-chan etre.CDCEvent, int64, error)

	// Deregister takes the UUID of a registered client and closes the
	// event channel for it (i.e., stops sending it newly polled events).
	Deregister(uuid string)

	// Error returns the error that caused the Poller to stop. Start resets
	// the error.
	Error() error
}

type poller struct {
	cdcm              cdc.Manager
	dm                DelayManager
	sleepBetweenPolls int // the number of milliseconds to sleep between each poll
	clientBufferSize  int // the channel buffer size for registered clients
	logEvery          int // log every N seconds
	// --
	clients     map[string]chan etre.CDCEvent // clientUUID => clientChannel
	running     bool                          // true while the poller is running
	stopChan    chan struct{}                 // channel that gets closed when Stop is called
	err         error                         // last error the poller encountered
	maxPolledTs int64                         // the last upper-bound timestamp that the poller polled
	*sync.Mutex                               // guards function calls
}

func NewPoller(cdcm cdc.Manager, dm DelayManager, sleepBetweenPolls, clientBufferSize, logEvery int) Poller {
	return &poller{
		cdcm:              cdcm,
		dm:                dm,
		sleepBetweenPolls: sleepBetweenPolls,
		clientBufferSize:  clientBufferSize,
		logEvery:          logEvery,
		clients:           map[string]chan etre.CDCEvent{},
		Mutex:             &sync.Mutex{},
	}
}

func (p *poller) Start() {
	p.Lock()
	defer p.Unlock()
	log.Info("starting poller")

	if p.running {
		log.Info("poller already running")
		return
	}

	p.running = true
	p.err = nil
	p.stopChan = make(chan struct{})

	go p.poll()
}

func (p *poller) Stop() {
	p.Lock()
	defer p.Unlock()
	log.Info("stopping poller")

	if !p.running {
		log.Info("poller not running")
		return
	}
	p.running = false

	// Close the stop channel, but DO NOT close any client channels or
	// deregister any clients (that needs to happen in the private poll
	// method below to avoid sending on nil channel).
	close(p.stopChan)
}

func (p *poller) Register(id string) (<-chan etre.CDCEvent, int64, error) {
	p.Lock()
	defer p.Unlock()
	log.Infof("registering client id=%s with poller", id)

	if !p.running {
		log.Infof("poller not running")
		return nil, p.maxPolledTs, ErrPollerNotRunning
	}

	if v, ok := p.clients[id]; ok {
		// Client is already registered.
		log.Infof("client id=%s already registered with poller", id)
		return v, p.maxPolledTs, nil
	}
	p.clients[id] = make(chan etre.CDCEvent, p.clientBufferSize)

	return p.clients[id], p.maxPolledTs, nil
}

func (p *poller) Deregister(id string) {
	log.Infof("deregistering client id=%s with poller", id)
	p.Lock()
	defer p.Unlock()
	p.deregister(id)
}

func (p *poller) Error() error {
	p.Lock()
	defer p.Unlock()
	return p.err
}

// ------------------------------------------------------------------------- //

func (p *poller) poll() {
	logTicker := time.NewTicker(time.Duration(p.logEvery) * time.Second)

	// Start polling from time.Now().
	sinceTs := CurrentTimestamp()
	for {
		// If the poller was stopped, deregister all clients and return.
		select {
		case <-p.stopChan:
			p.Lock()
			p.deregisterAll()
			p.Unlock()
			return
		default:
		}

		// Get the maximum upper-bound timestamp that is safe to query.
		untilTs, err := p.dm.MaxTimestamp()
		if err != nil {
			// If there was an error, deregister all clients and return.
			p.Lock()
			p.err = err
			p.deregisterAll()
			p.Unlock()
			return
		}

		// Log current status every so often to prevent flooding the logs.
		select {
		case <-logTicker.C:
			log.Infof("poller: still polling new CDC events (current chunk is from %d until %d)", sinceTs, untilTs)
		default:
		}

		filter := cdc.Filter{
			SinceTs: sinceTs,
			UntilTs: untilTs,
			SortBy:  cdc.SORT_BY_ENTID_REV,
		}
		events, err := p.cdcm.ListEvents(filter)
		if err != nil {
			// If there was an error, deregister all clients and return.
			p.Lock()
			p.err = err
			p.deregisterAll()
			p.Unlock()
			return
		}

		p.Lock() // lock before sending events to clients

		// Send each event to all of the registered clients. If a client's
		// buffer is full, deregister it (this happens if the client is too
		// far behind to keep up with the poller).
		for _, event := range events {
			for id, cchan := range p.clients {
				select {
				case cchan <- event:
				default:
					p.deregister(id)
				}
			}
		}

		// Update the max timestamp that we've polled.
		p.maxPolledTs = untilTs

		p.Unlock() // unlock after sending events and setting maxPolledTs

		// Set the lower-bound timestamp for the next chunk to be
		// equal to the upper-bound timestamp of the current chunk.
		sinceTs = untilTs
	}
}

// Deregister a client. Caller must lock the poller before calling this method.
func (p *poller) deregister(id string) {
	if v, ok := p.clients[id]; ok {
		close(v)
		delete(p.clients, id)
	}
}

// Deregister all clients. Caller must lock the poller before calling this method.
func (p *poller) deregisterAll() {
	log.Infof("deregistering all clients")
	for id, cchan := range p.clients {
		close(cchan)
		delete(p.clients, id)
	}
}
