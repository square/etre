package changestream_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-test/deep"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

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
		if err != nil {
			t.Fatal(err)
		}

		store = cdc.NewStore(coll["cdc"], "", cdc.NoRetryPolicy)
	}

	// Reset the collection: delete all cdc events and insert the standard cdc events
	cdcColl := coll[entityType]
	_, err := cdcColl.DeleteMany(context.TODO(), bson.D{{}})
	if err != nil {
		t.Fatal(err)
	}
}

// --------------------------------------------------------------------------

func TestServer(t *testing.T) {
	setup(t)

	server := changestream.NewMongoDBServer(changestream.ServerConfig{
		CDCCollection: coll["cdc"],
		MaxClients:    1,
		BufferSize:    1,
	})
	go server.Run()
	defer server.Stop()

	stream, err := server.Watch("c1")
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Write(events1[0]); err != nil {
		t.Fatal(err)
	}

	var gotEvent etre.CDCEvent
	select {
	case gotEvent = <-stream:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for change stream channel")
	}

	if diff := deep.Equal(gotEvent, events1[0]); diff != nil {
		t.Error(diff)
	}

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

	stream, err := server.Watch("c1")
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Write(events1[0]); err != nil {
		t.Fatal(err)
	}

	time.Sleep(300 * time.Millisecond)
	select {
	case <-stream:
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

	stream, err := server.Watch("c1")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	server.Stop()

	time.Sleep(200 * time.Millisecond)

	select {
	case <-stream:
	default:
		t.Error("client channel not closed when server stopped")
	}
}
