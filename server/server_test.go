package server

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/square/etre/app"
	"github.com/square/etre/config"
	"github.com/square/etre/db"
	"github.com/square/etre/test/mock"
)

// TestDefaultPlugins tests server boot with default plugins
func TestDefaultPlugins(t *testing.T) {
	s := NewServer(app.Defaults())
	err := s.Boot("../test/config/empty.yaml")
	require.NoErrorf(t, err, "Error starting: %s", err)
	err = s.Stop()
	require.NoErrorf(t, err, "Error stopping: %s", err)
}

// TestDefaultPlugins tests server boot with a DB plugin
func TestDBPlugin(t *testing.T) {
	counter := 0
	dbp := &mock.DBPlugin{
		ConnectFunc: func(cfg config.DatasourceConfig) (*mongo.Client, error) {
			counter++
			return db.Default{}.Connect(cfg)
		},
	}

	ctx := app.Defaults()
	ctx.Plugins.DB = dbp
	s := NewServer(ctx)
	// Start the server
	err := s.Boot("../test/config/empty.yaml")
	require.NoErrorf(t, err, "Error starting: %s", err)

	// Check that the DB plugin was called twice: once for the main DB and once for the CDC DB.
	if counter != 2 {
		t.Errorf("Expected DB plugin to be called once, but got %d", counter)
	}

	// Stop the server
	err = s.Stop()
	require.NoErrorf(t, err, "Error stopping: %s", err)
}
