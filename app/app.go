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

type Hooks struct {
	LoadConfig func(Context) (config.Config, error)
	RunAPI     func() error
	StopAPI    func() error
}

type Plugins struct {
	Auth auth.Plugin
}

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
