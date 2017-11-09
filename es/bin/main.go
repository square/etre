// Copyright 2017, Square, Inc.

package main

import (
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
	ec := etre.NewEntityClient(ctx.EntityType, ctx.Options.Addr, httpClient)
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
