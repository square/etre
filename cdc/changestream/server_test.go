// Copyright 2020, Square, Inc.

package changestream_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/square/etre"
	"github.com/square/etre/cdc"
	"github.com/square/etre/cdc/changestream"
	"github.com/square/etre/test"
)

var (
	store  cdc.Store
	client *mongo.Client
	coll   map[string]*mongo.Collection

	username    = "test_user"
	entityType  = "cdc"
	entityTypes = []string{entityType}
)

func setup(t *testing.T) {
	if coll == nil {
		var err error
		client, coll, err = test.DbCollections(entityTypes)
		require.NoError(t, err)
		store = cdc.NewStore(coll["cdc"], "", cdc.NoRetryPolicy)
	}

	// Reset the collection: delete all cdc events and insert the standard cdc events
	cdcColl := coll[entityType]
	_, err := cdcColl.DeleteMany(context.TODO(), bson.D{{}})
	require.NoError(t, err)
}

// --------------------------------------------------------------------------

func TestServer(t *testing.T) {
	//etre.DebugEnabled = true
	setup(t)

	server := changestream.NewMongoDBServer(changestream.ServerConfig{
		CDCCollection: coll["cdc"],
		MaxClients:    1,
		BufferSize:    1,
	})
	go server.Run()
	defer server.Stop()
	time.Sleep(200 * time.Millisecond) // given server.Run() a moment to start

	stream, err := server.Watch("c1")
	require.NoError(t, err)

	if err := store.Write(context.TODO(), events1[0]); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	var gotEvent etre.CDCEvent
	select {
	case gotEvent = <-stream:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for change stream channel")
	}
	assert.Equal(t, events1[0], gotEvent)

	server.Close("c1")
	select {
	case <-stream:
	default:
		t.Error("client channel not closed after calling server.Close")
	}
}

func TestServerClientBlock(t *testing.T) {
	setup(t)

	server := changestream.NewMongoDBServer(changestream.ServerConfig{
		CDCCollection: coll["cdc"],
		MaxClients:    1,
		BufferSize:    0,
	})
	go server.Run()
	defer server.Stop()
	time.Sleep(200 * time.Millisecond) // given server.Run() a moment to start

	stream, err := server.Watch("c1")
	require.NoError(t, err)

	if err := store.Write(context.TODO(), events1[0]); err != nil {
		t.Fatal(err)
	}

	time.Sleep(300 * time.Millisecond)
	select {
	case <-stream:
	case <-time.After(1 * time.Second):
		t.Error("client channel not closed by server after blocking (timeout)")
	default:
		t.Error("client channel not closed by server after blocking")
	}
}

func TestServerStop(t *testing.T) {
	setup(t)

	server := changestream.NewMongoDBServer(changestream.ServerConfig{
		CDCCollection: coll["cdc"],
		MaxClients:    1,
		BufferSize:    0,
	})
	go server.Run()
	time.Sleep(200 * time.Millisecond) // given server.Run() a moment to start

	stream, err := server.Watch("c1")
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	server.Stop()

	time.Sleep(200 * time.Millisecond)

	select {
	case <-stream:
	default:
		t.Error("client channel not closed when server stopped")
	}
}
