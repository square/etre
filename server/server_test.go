package server

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/square/etre/app"
	"github.com/square/etre/auth"
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

func TestMapConfigACLRoles(t *testing.T) {
	// Check the number of fields in both ACL structs. Any time one of them is edited, it is likely the other one needs to be updated as well.
	// Testing the field counts ensures that any time someone edits one of these, they don't forget to update the other one (or this test) to match.
	assert.Equal(t, 6, reflect.TypeOf(config.ACL{}).NumField(), "Wrong number of fields in config.ACL. Did you edit the class and forget to update the test?")
	assert.Equal(t, 6, reflect.TypeOf(auth.ACL{}).NumField(), "Wrong number of fields in auth.ACL. Did you edit the class and forget to update the test?")

	testCases := []struct {
		name      string
		configACL config.ACL
		authACL   auth.ACL
	}{
		{name: "Empty role",
			configACL: config.ACL{
				Role: "t1",
			},
			authACL: auth.ACL{
				Role: "t1",
			},
		},
		{name: "Admin role",
			configACL: config.ACL{
				Role:  "t2",
				Admin: true,
			},
			authACL: auth.ACL{
				Role:  "t2",
				Admin: true,
			},
		},
		{name: "CDC role",
			configACL: config.ACL{
				Role: "t3",
				CDC:  true,
			},
			authACL: auth.ACL{
				Role: "t3",
				CDC:  true,
			},
		},
		{name: "Full role",
			configACL: config.ACL{
				Role:              "t4",
				Admin:             true,
				Read:              []string{"host", "dns"},
				Write:             []string{"host", "elasticache"},
				CDC:               true,
				TraceKeysRequired: []string{"key1", "key2"},
			},
			authACL: auth.ACL{
				Role:              "t4",
				Admin:             true,
				Read:              []string{"host", "dns"},
				Write:             []string{"host", "elasticache"},
				CDC:               true,
				TraceKeysRequired: []string{"key1", "key2"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			converted, err := MapConfigACLRoles([]config.ACL{tc.configACL})
			require.NoError(t, err)
			require.Len(t, converted, 1)
			assert.Equal(t, tc.authACL, converted[0])
		})
	}
}
