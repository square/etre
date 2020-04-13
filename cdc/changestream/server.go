// Copyright 2020, Square, Inc.

package changestream

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/square/etre"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

var (
	ErrNoMoreClients   = errors.New("max clients reached, no more clients allowed")
	ErrDuplicateClient = errors.New("Watch called with duplicate clientId")
)

type Server interface {
	Run() error
	Stop()
	Watch(clientId string) (<-chan etre.CDCEvent, error)
	Close(clientId string)
}

type ServerConfig struct {
	CDCCollection *mongo.Collection
	MaxClients    uint
	BufferSize    uint
}

var _ Server = &MongoDBServer{}

type MongoDBServer struct {
	cfg ServerConfig
	*sync.Mutex
	stream   *mongo.ChangeStream
	clients  map[string]client
	ctx      context.Context
	cancel   context.CancelFunc
	doneChan chan struct{}
}

type client struct {
	clientId string
	c        chan etre.CDCEvent
}

func NewMongoDBServer(cfg ServerConfig) *MongoDBServer {
	s := &MongoDBServer{
		cfg:      cfg,
		Mutex:    &sync.Mutex{},
		clients:  map[string]client{},
		doneChan: make(chan struct{}),
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	return s
}

func (s *MongoDBServer) Watch(clientId string) (<-chan etre.CDCEvent, error) {
	s.Lock()
	defer s.Unlock()
	if len(s.clients)+1 > int(s.cfg.MaxClients) {
		return nil, ErrNoMoreClients
	}
	if _, ok := s.clients[clientId]; ok {
		return nil, ErrDuplicateClient
	}
	c := make(chan etre.CDCEvent, s.cfg.BufferSize)
	s.clients[clientId] = client{
		clientId: clientId,
		c:        c,
	}
	etre.Debug("added client %s", clientId)
	return c, nil
}

func (s *MongoDBServer) Close(clientId string) {
	etre.Debug("Close call: %s", clientId)
	defer etre.Debug("Close return: %s", clientId)
	s.Lock()
	defer s.Unlock()
	s.close(clientId)
}

func (s *MongoDBServer) close(clientId string) {
	if c, ok := s.clients[clientId]; ok {
		close(c.c)
		delete(s.clients, clientId)
		etre.Debug("removed client %s", clientId)
	}
}

type rawCDCEvent struct {
	EtreCDCEvent etre.CDCEvent `bson:"fullDocument"`
}

func (s *MongoDBServer) Run() error {
	etre.Debug("Run call")
	defer etre.Debug("Run return")
	defer close(s.doneChan)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	matchStage := bson.D{{"$match", bson.D{{"operationType", "insert"}}}}
	stream, err := s.cfg.CDCCollection.Watch(ctx, mongo.Pipeline{matchStage})
	cancel()
	if err != nil {
		return err
	}
	s.stream = stream

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		stream.Close(ctx)
		cancel()
	}()

	for stream.Next(s.ctx) {
		var e rawCDCEvent
		if err := stream.Decode(&e); err != nil {
			return err
		}
		s.Lock()
		for clientId, c := range s.clients {
			select {
			case c.c <- e.EtreCDCEvent:
			default:
				etre.Debug("client %s blocked, closing", clientId)
				s.close(clientId)
			}
		}
		s.Unlock()
	}

	if err := stream.Err(); err != nil {
		s.Lock()
		defer s.Unlock()
		for clientId := range s.clients {
			s.close(clientId)
		}
		return err
	}

	return nil
}

func (s *MongoDBServer) Stop() {
	etre.Debug("Stop call")
	defer etre.Debug("Stop return")
	s.cancel()
	<-s.doneChan
}
