// Copyright 2017-2020, Square, Inc.

package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/square/etre"
	"github.com/square/etre/es"
	"github.com/square/etre/es/app"
)

// Default etre.EntityClient factory
type ecFactory struct{}

func (f ecFactory) Make(ctx app.Context) (etre.EntityClient, error) {
	httpClient := &http.Client{
		Timeout: time.Duration(ctx.Options.Timeout) * time.Millisecond,
	}
	retryWait, err := time.ParseDuration(ctx.Options.RetryWait)
	if err != nil {
		return nil, fmt.Errorf("invalid --retry-wait %s: %s\n", ctx.Options.RetryWait, err)
	}
	c := etre.EntityClientConfig{
		EntityType:   ctx.EntityType,
		Addr:         ctx.Options.Addr,
		HTTPClient:   httpClient,
		Retry:        ctx.Options.Retry,
		RetryWait:    retryWait,
		RetryLogging: true,
	}
	ec := etre.NewEntityClientWithConfig(c)
	return ec, nil
}

func main() {
	defaultContext := app.Context{
		In:    os.Stdin,
		Out:   os.Stdout,
		Hooks: app.Hooks{},
		Factories: app.Factories{
			EntityClient: ecFactory{},
		},
	}
	es.Run(defaultContext)
}
