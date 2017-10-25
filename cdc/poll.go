// Copyright 2017, Square, Inc.

package cdc

import (
	"errors"
	"sync"
	"time"

	"github.com/square/etre"

	log "github.com/Sirupsen/logrus"
)

var (
	ErrPollerNotRunning     = errors.New("poller not running")
	ErrPollerAlreadyRunning = errors.New("poller already running")
)

// A Poller polls the Store for new events and broadcasts them to all registered
// clients. The Poller is a singleton. Having each client poll the Store for new
// events does not scale, and it causes duplicate reads of the same events.
// Instead, the Poller reads once from the Store and “writes many” to each client.
type Poller interface {
	// Run runs the Poller. It only stops running if it encounters an error
	// (which can be inspected with Error), else it runs forever. Clients
	// can register with the Poller once it is running.
	Run() error

	// Register registers a client with the Poller. It takes a brief lock
	// on the Poller to ensure that we get a consistent snapshot of when
	// the client is registered. It returns a buffered channel that the
	// client can select on to get all events that the Poller polls after
	// the client is registered. It also returns a timestamp which
	// represents the last upper-bound timestamp that it polled before the
	// client was registered. The client can assume that all events after
	// this timestamp will come through the aforementioned channel. If the
	// buffer on the channel fills up (if the client isn't consuming from
	// it fast enough), the Poller automatically deregisters the client.
	//
	// The only argument that register takes is a string (id) that must
	// uniquely identify a client.
	Register(id string) (<-chan etre.CDCEvent, int64, error)

	// Deregister takes the id of a registered client and closes the event
	// channel for it (i.e., stops sending it newly polled events).
	Deregister(id string)

	// Error returns the error that caused the Poller to stop running. This
	// allows clients to see why the Poller stopped. Start resets the error.
	Error() error
}

type poller struct {
	cdcs             Store
	delayer          Delayer
	clientBufferSize int          // the buffer size of registered client event channels
	pollInterval     *time.Ticker // polling interval
	// --
	clients     map[string]chan etre.CDCEvent // clientId => clientChannel
	running     bool                          // true while the poller is running
	err         error                         // last error the poller encountered
	maxPolledTs int64                         // the last upper-bound timestamp that the poller polled
	*sync.Mutex                               // guards function calls
}

// NewPoller creates a poller. The provided clientBufferSize is the buffer size
// used when creating event channels for registered clients (read the
// documentation on Poller.Register for more details on what this means).
func NewPoller(cdcs Store, delayer Delayer, clientBufferSize int, pollInterval *time.Ticker) Poller {
	return &poller{
		cdcs:             cdcs,
		delayer:          delayer,
		clientBufferSize: clientBufferSize,
		pollInterval:     pollInterval,
		clients:          map[string]chan etre.CDCEvent{},
		Mutex:            &sync.Mutex{},
	}
}

func (p *poller) Run() error {
	p.Lock()
	log.Info("running poller")

	if p.running {
		log.Info("poller already running")
		p.Unlock()
		return ErrPollerAlreadyRunning
	}

	p.running = true
	p.err = nil

	p.Unlock()
	return p.poll()
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

func (p *poller) poll() error {
	logInterval := time.NewTicker(time.Duration(DEFAULT_LOG_INTERVAL) * time.Second)

	// Start polling from time.Now().
	sinceTs := CurrentTimestamp()
	for {
		// Get the maximum upper-bound timestamp that is safe to query.
		untilTs, err := p.delayer.MaxTimestamp()
		if err != nil {
			// If there was an error, deregister all clients and return.
			p.Lock()
			p.deregisterAll()
			p.err = err
			p.Unlock()
			return err
		}

		// Log current status every so often.
		select {
		case <-logInterval.C:
			log.Infof("poller: still polling new CDC events (current chunk is from %d until %d)", sinceTs, untilTs)
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
			// If there was an error, deregister all clients and return.
			p.Lock()
			p.deregisterAll()
			p.err = err
			p.Unlock()
			return err
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

		<-p.pollInterval.C
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
