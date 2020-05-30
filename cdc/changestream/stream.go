// Copyright 2017-2020, Square, Inc.

package changestream

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/square/etre"
	"github.com/square/etre/cdc"
)

var (
	ClientBufferSize = 10
	ServerBufferSize = 1000
	BacklogWait      = 100 * time.Millisecond
)

var (
	ErrStopped            = errors.New("Streamer.Stop called")
	ErrServerClosedStream = errors.New("server closed the change stream")
	ErrBufferTooSmall     = errors.New("current events buffer reached ServerBufferSize before backlog streaming done")
)

// A Streamer produces a stream of CDC events on a channel. It can start
// streaming events from any point in the past, continually making its way closer
// and closer to present events. Once it catches up to new events that are being
// created in real-time, it seamlessly transitions to streaming those.
//
// Streamers are used by feeds to produce the events that a feed sends to its
// client. A feed can only have one streamer.
type Streamer interface {
	// Start starts streaming events from a given timestamp. It returns a
	// channel on which the caller can receive all of the events it streams
	// for as long as the streamer is running. A streamer runs until Stop
	// is called or until it encounters an error.
	Start(sinceTs int64) <-chan etre.CDCEvent

	InSync() chan struct{}

	Status() Status

	// Stop stops the streamer and closes the event channel returned by Start.
	Stop()

	// Error returns the error that caused the streamer to stop. Start resets
	// the error.
	Error() error
}

type StreamerFactory interface {
	Make(clientId string) Streamer
}

type ServerStreamFactory struct {
	Server Server
	Store  cdc.Store
}

func (f ServerStreamFactory) Make(clientId string) Streamer {
	return NewServerStream(clientId, f.Server, f.Store)
}

type Status struct {
	ClientId           string
	Running            bool
	InSync             bool
	SyncMethod         string
	Since              time.Time
	BacklogState       string
	BufferUsage        []int // [max, in, out]
	ServerClosedStream bool
}

var _ Streamer = &ServerStream{}

type ServerStream struct {
	clientId string
	server   Server
	store    cdc.Store
	// --
	toClientChan chan etre.CDCEvent // to WebsocketClient or plugin code using streamer directly

	revorder *etre.RevOrder
	stopChan chan struct{} // channel that gets closed when Stop is called
	wg       *sync.WaitGroup

	runMux   *sync.Mutex
	status   Status
	err      error // the last error the streamer encountered
	syncChan chan struct{}

	buffMux *sync.Mutex
	buff    []etre.CDCEvent

	serverClosed bool
}

// newStreamer creates a streamer that uses the provided Poller and Store
// to stream events. feedId is the id of the feed that created it.
func NewServerStream(clientId string, server Server, store cdc.Store) *ServerStream {
	return &ServerStream{
		clientId:     clientId,
		server:       server,
		store:        store,
		toClientChan: make(chan etre.CDCEvent, ClientBufferSize),
		stopChan:     make(chan struct{}),
		syncChan:     make(chan struct{}),
		runMux:       &sync.Mutex{},
		status:       Status{ClientId: clientId},
		wg:           &sync.WaitGroup{},
		buffMux:      &sync.Mutex{},
		revorder:     etre.NewRevOrder(etre.DEFAULT_MAX_ENTITIES, true),
	}
}

func (s *ServerStream) Start(sinceTs int64) <-chan etre.CDCEvent {
	s.runMux.Lock()
	defer s.runMux.Unlock()

	if s.status.Running {
		panic("ServerStream.Start called twice")
	}
	select {
	case <-s.stopChan:
		panic("ServerStream.Start called after Stop")
	default:
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				etre.Debug("PANIC: stream: %v", r)
				s.runMux.Lock()
				b := make([]byte, 4096)
				n := runtime.Stack(b, false)
				s.err = fmt.Errorf("PANIC: ServerStream.stream(): %s\n%s\n", r, string(b[0:n]))
				s.runMux.Unlock()
			}
			s.Stop()
		}()
		s.wg.Add(1)

		err := s.stream(sinceTs)
		s.runMux.Lock()
		s.err = err
		s.runMux.Unlock()
	}()

	s.status.Running = true

	return s.toClientChan
}

func (s *ServerStream) Stop() {
	etre.Debug("Stop call")
	defer etre.Debug("Stop return")
	s.runMux.Lock()
	defer s.runMux.Unlock()
	if !s.status.Running {
		return
	}
	s.status.Running = false
	close(s.stopChan) // stop all goroutines
	etre.Debug("wg.Wait begin")
	s.wg.Wait() // wait for them ^ to return
	etre.Debug("wg.Wait end")
	close(s.toClientChan) // signal client to stop
}

func (s *ServerStream) Error() error {
	s.runMux.Lock()
	defer s.runMux.Unlock()
	return s.err
}

func (s *ServerStream) Status() Status {
	etre.Debug("Status call")
	defer etre.Debug("Status return")

	s.runMux.Lock()
	defer s.runMux.Unlock()

	// Copy BufferUsage because it's a slice, so if we return s.status the caller
	// will have the same slice which creates a race condition
	var buf []int
	if s.status.BufferUsage != nil {
		buf = make([]int, len(s.status.BufferUsage))
		copy(buf, s.status.BufferUsage)
	}

	status := Status{
		ClientId:           s.status.ClientId,
		Running:            s.status.Running,
		InSync:             s.status.InSync,
		SyncMethod:         s.status.SyncMethod,
		Since:              s.status.Since,
		BacklogState:       s.status.BacklogState,
		BufferUsage:        buf,
		ServerClosedStream: s.status.ServerClosedStream,
	}
	return status
}

func (s *ServerStream) InSync() chan struct{} {
	etre.Debug("InSync call")
	defer etre.Debug("InSync return")
	return s.syncChan
}

// --------------------------------------------------------------------------

func (s *ServerStream) stream(sinceTs int64) error {
	etre.Debug("stream call")
	defer etre.Debug("stream return")
	defer s.wg.Done()

	serverStreamChan, err := s.server.Watch(s.clientId)
	if err != nil {
		return err
	}
	defer s.server.Close(s.clientId)

	// ----------------------------------------------------------------------
	// Backlog
	// ----------------------------------------------------------------------
	if sinceTs > 0 {
		s.wg.Add(1)
		if err := s.backlog(sinceTs, serverStreamChan); err != nil {
			return err
		}
	}

	// ----------------------------------------------------------------------
	// In Sync Stream
	// ----------------------------------------------------------------------
	// Send current events to client
	s.runMux.Lock()
	s.status.InSync = true
	close(s.syncChan)
	etre.Debug("in sync")
	s.runMux.Unlock()
	for {
		select {
		case e, ok := <-serverStreamChan:
			if !ok {
				etre.Debug("ServerClosedStream in stream")
				s.runMux.Lock()
				s.status.ServerClosedStream = true
				s.runMux.Unlock()
				return ErrServerClosedStream
			}
			if err := s.sendToClient(e); err != nil {
				return err
			}
		case <-s.stopChan:
			return ErrStopped
		}
	}
}

func (s *ServerStream) backlog(sinceTs int64, serverStreamChan <-chan etre.CDCEvent) error {
	etre.Debug("backlog call")
	defer etre.Debug("backlog return")
	defer s.wg.Done()

	backlogDoneChan := make(chan struct{})

	s.runMux.Lock()
	s.status.BufferUsage = []int{ServerBufferSize, 0, 0} //  size, in, out
	s.runMux.Unlock()

	// Buffer events until backlog is done. Recv from serverStreamChan and
	// buffer into s.backlog until streamBacklog is done and closes backlogDoneChan.
	s.wg.Add(1)
	buffErrChan := make(chan error)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				etre.Debug("PANIC: bufferCurrentEvents: %v", r)
				buffErrChan <- fmt.Errorf("ServerStream.bufferCurrentEvents panic: %v", r)
			}
		}()
		buffErrChan <- s.bufferCurrentEvents(serverStreamChan, backlogDoneChan)
	}()

	// Send backlog evnets to client. Close backlogDoneChan when done to stop
	// bufferCurrentEvents goroutine.
	s.wg.Add(1)
	if err := s.streamBacklog(sinceTs, backlogDoneChan); err != nil {
		etre.Debug("streamBacklog error: %v", err)
		return err
	}

	// Continue only if bufferCurrentEvents goroutine ^ returned no error, which
	// means we were able to buffer all the current events while streamBacklog
	// was running.
	select {
	case buffErr := <-buffErrChan:
		if buffErr != nil {
			etre.Debug("bufferCurrentEvents error: %v", buffErr)
			return buffErr
		}
	case <-time.After(1 * time.Second):
		return fmt.Errorf("timeout waiting for SeverStreamer.bufferCurrentEvents to return")
	}

	// Send buffered current events to client.
	s.buffMux.Lock()
	defer s.buffMux.Unlock()
	etre.Debug("%d buffered events", len(s.buff))
	for _, e := range s.buff {
		if err := s.sendToClient(e); err != nil {
			return err
		}
		s.runMux.Lock()
		s.status.BufferUsage[2] += 1
		s.runMux.Unlock()
	}

	return nil
}

func (s *ServerStream) bufferCurrentEvents(serverStreamChan <-chan etre.CDCEvent, backlogDoneChan chan struct{}) error {
	etre.Debug("bufferCurrentEvents call")
	defer etre.Debug("bufferCurrentEvents return")
	defer s.wg.Done()

	s.buffMux.Lock()
	defer s.buffMux.Unlock()
	s.buff = make([]etre.CDCEvent, 0, ServerBufferSize)
	i := 0
	for i < ServerBufferSize {
		select {
		case e, ok := <-serverStreamChan:
			if !ok {
				etre.Debug("ServerClosedStream in bufferCurrentEvents")
				s.runMux.Lock()
				s.status.ServerClosedStream = true
				s.runMux.Unlock()
				return ErrServerClosedStream
			}
			s.buff = append(s.buff, e)
			i += 1
			s.runMux.Lock()
			s.status.BufferUsage[1] = i
			s.runMux.Unlock()
		case <-backlogDoneChan:
			etre.Debug("backlogDoneChan closed")
			return nil
		case <-s.stopChan:
			return ErrStopped
		}
	}
	etre.Debug("ErrBufferTooSmall")
	return ErrBufferTooSmall
}

func (s *ServerStream) streamBacklog(sinceTs int64, backlogDoneChan chan struct{}) error {
	etre.Debug("streamBacklog call")
	defer etre.Debug("streamBacklog return")
	defer s.wg.Done()
	defer close(backlogDoneChan)

	/*
		BacklogWait is
		  TS  Write
		   ~    <- sinceTs
		 200
		 300
		 400  1
		 400  2 <- Watch()
		 400  3
		 410  4
		 395  5
		 400  N writes
		 401  N+1
		 500    <- untilTs

		1,000 writes/s = 1 write per 1ms = 100 writes per 100ms.
	*/
	time.Sleep(BacklogWait)
	untilTs := time.Now().UnixNano() / int64(time.Millisecond)
	etre.Debug("since %d  until %d", sinceTs, untilTs)

	events, err := s.store.Read(cdc.Filter{
		SinceTs: sinceTs, // >= sinceTs
		UntilTs: untilTs, //  < untilTs
		Order:   cdc.ByTsAsc{},
	})
	if err != nil {
		return err
	}
	etre.Debug("%d backlog events", len(events))
	for _, e := range events {
		if err := s.sendToClient(e); err != nil {
			return err
		}
	}
	return nil
}

// sendToClient sends the event to the client if it's in order. If not, the
// event is saved and sent later once revorder receives the other out-of-order
// event. This means a single call to sendToClient can send zero or more events.
// If the streamer is stopped, the event is not sent and ErrStopped is returned.
func (s *ServerStream) sendToClient(e etre.CDCEvent) error {
	inOrder, prevEvents := s.revorder.InOrder(e)

	// inOrder is false when this e is not rev+1 of its previous rev.
	// revorder saved this e and will return it later in prevEvents
	// once the complete sequence of revs between prev.Rev and e.Rev
	// is received.
	if !inOrder {
		etre.Debug("event out of order: %+v", e)
		return nil
	}

	// Normal case: e is in sync and there are no past revs
	if prevEvents == nil {
		return s.send(e)
	}

	// Past e were out of sync and revorder just got them in sync.
	// prevEvents include this e (it wasn't sent above).
	etre.Debug("sending %d ordered events", len(prevEvents))
	for _, e := range prevEvents {
		if err := s.send(e); err != nil {
			return err
		}
	}
	return nil
}

// send actually sends the event to the client, unless the streamer is stopped.
// Do not call this fucntion directly; always call sendToClient to ensure proper
// event ordering.
func (s *ServerStream) send(e etre.CDCEvent) error {
	// Don't send if already stopped
	select {
	case <-s.stopChan:
		return ErrStopped
	default:
	}
	// Send, block on recv or until stopped
	select {
	case s.toClientChan <- e:
	case <-s.stopChan:
		return ErrStopped
	}
	return nil
}
