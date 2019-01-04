package app

import (
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
	return config.Load(ctx.ConfigFile)
}
