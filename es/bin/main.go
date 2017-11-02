// Copyright 2017, Square, Inc.

package main

import (
	"net/http"
	"os"

	"github.com/square/etre"
	"github.com/square/etre/es"
	"github.com/square/etre/es/app"
)

// Default etre.EntityClient factory
type ecFactory struct{}

func (f ecFactory) Make(ctx app.Context) (etre.EntityClient, error) {
	ec := etre.NewEntityClient(ctx.EntityType, ctx.Options.Addr, &http.Client{})
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
