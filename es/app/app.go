// Copyright 2017, Square, Inc.

// Package app provides app-wide data structs and functions.
package app

import (
	"io"
	"log"

	"github.com/square/etre"
	"github.com/square/etre/es/config"
)

// Context represents how to run es. A context is passed to es.Run().
// A default context is created in main.go. Wrapper code can integrate with
// es by passing a custom context to es.Run(). Integration is done
// primarily with hooks and factories.
type Context struct {
	// Set in main.go or by wrapper
	In        io.Reader // where to read user input (default: stdin)
	Out       io.Writer // where to print output (default: stdout)
	Hooks     Hooks     // for integration with other code
	Factories Factories // for integration with other code

	// Set automatically in es.Run()
	Options      config.Options // command line options (--addr, etc.)
	EntityType   string
	ReturnLabels []string // query
	Query        string   // query
	EntityId     string   // --update and --delete
	Patches      []string // --update
}

type EntityClientFactory interface {
	Make(Context) (etre.EntityClient, error)
}

type Factories struct {
	EntityClient EntityClientFactory
}

type Hooks struct {
	AfterParseOptions func(*config.Options)
	BeforeQuery       func(*Context) error
	AfterQuery        func(Context, []etre.Entity, error)
	BeforeDelete      func(ctx *Context) error
	BeforeUpdate      func(cxt *Context) error
	WriteResult       func(Context, etre.WriteResult, error)
}

func init() {
	log.SetPrefix("DEBUG ")
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
}

func Debug(fmt string, v ...interface{}) {
	log.Printf(fmt, v...)
}
