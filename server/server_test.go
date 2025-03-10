package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
	require.NoError(t, err, "Error booting server")

	err = s.Stop()
	require.NoError(t, err, "Error stopping server")
}

// TestDBPlugin tests server boot with a DB plugin
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
	require.NoError(t, err, "Error booting server")

	// Check that the DB plugin was called twice: once for the main DB and once for the CDC DB.
	assert.Equal(t, 2, counter, "Expected DB plugin to be called twice")

	// Stop the server
	err = s.Stop()
	require.NoError(t, err, "Error stopping server")
}
