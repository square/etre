// Copyright 2019, Square, Inc.

// Package app provides app context and extensions: hooks and plugins.
package app

import (
	"log"
	"os"

	"github.com/square/etre/auth"
	"github.com/square/etre/cdc"
	"github.com/square/etre/config"
	"github.com/square/etre/entity"
	"github.com/square/etre/metrics"
)

// Context represents the config, core service singletons, and 3rd-party extensions.
// There is one immutable context shared by many packages, created in Server.Boot,
// called api.appCtx.
type Context struct {
	ConfigFile string
	Config     config.Config

	EntityStore     entity.Store
	EntityValidator entity.Validator
	CDCStore        cdc.Store
	FeedFactory     cdc.FeedFactory
	MetricsStore    metrics.Store
	MetricsFactory  metrics.Factory
	Auth            auth.Manager

	// 3rd-party extensions, all optional
	Hooks   Hooks
	Plugins Plugins
}

// Hooks allow users to modify system behavior at certain points. All hooks are
// optional; the defaults are sufficient to run Etre. For example, the LoadConfig
// hook allows the user to load and parse the config file, completely overriding
// the built-in code.
type Hooks struct {
	// LoadConfig loads the Etre config. The default hook loads the config from
	// a file. This hook overrides the default. Etre fails to start if it returns
	// an error.
	LoadConfig func(Context) (config.Config, error)

	// RunAPI runs the Etre API. It should block until the API is stopped by
	// a call to StopAPI. If this hook is provided, it is called instead of api.Run().
	// If you provide this hook, you need to provide StopAPI as well.
	RunAPI func() error

	// StopAPI stops running the Etre API. It is called after RunAPI when
	// Etre is shutting down, and it should cause RunAPI to return.
	// If you provide this hook, you need to provide RunAPI as well.
	StopAPI func() error
}

// Plugins allow users to provide custom components. All plugins are optional;
// the defaults are sufficient to run Etre. Whereas hooks are single,
// specific calls, plugins are complete components with more extensive functionality
// defined by an interface. A user plugin, if provided, must implement the interface
// completely. For example, the Auth plugin allows the user to provide a complete
// and custom system of authentication and authorization.
type Plugins struct {
	Auth auth.Plugin
}

// Defaults returns a Context with default (built-in) hooks and plugins.
// The default context is not sufficient to run Etre, but it provides the starting
// point for user customization by overriding the defaults.
//
// After customizing the default context, it is used to boot the server (see server
// package) which loads the configs and creates the core service singleton.
func Defaults() Context {
	return Context{
		Hooks: Hooks{
			LoadConfig: LoadConfig,
		},
		Plugins: Plugins{
			Auth: auth.AllowAll{},
		},
	}
}

func LoadConfig(ctx Context) (config.Config, error) {
	// No -config file specified, so try a default file if it exists
	if ctx.ConfigFile == "" {
		switch os.Getenv("ENVIRONMENT") {
		case "staging":
			ctx.ConfigFile = "config/staging.yaml"
		case "production":
			ctx.ConfigFile = "config/production.yaml"
		default:
			ctx.ConfigFile = "config/development.yaml"
		}
		if !exists(ctx.ConfigFile) {
			log.Printf("No -config file and %s does not exist; using built-in defaults", ctx.ConfigFile)
			return config.Default(), nil
		}
	}
	log.Printf("Loading -config file %s", ctx.ConfigFile)
	return config.Load(ctx.ConfigFile)
}

func exists(file string) bool {
	if _, err := os.Stat(file); err != nil {
		return false
	}
	return true
}
