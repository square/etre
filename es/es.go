// Copyright 2017, Square, Inc.

// Package es provides a framework for integration with other programs.
package es

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/square/etre"
	"github.com/square/etre/es/app"
	"github.com/square/etre/es/config"
)

// Run runs es and exits when done. When using a standard es bin, Run is
// called by es/bin/main.go. When es is wrapped by custom code, that code
// imports this pkg then call es.Run() with its custom factories. If a factory
// is not set (nil), then the default/standard factory is used.
func Run(ctx app.Context) {
	// //////////////////////////////////////////////////////////////////////
	// Config and command line
	// //////////////////////////////////////////////////////////////////////

	// Options are set in this order: config -> env var -> cmd line option.
	// So first we must apply config files, then do cmd line parsing which
	// will apply env vars and cmd line options.

	// Parse cmd line to get --config files
	cmdLine := config.ParseCommandLine(config.Options{})

	// --config files override defaults if given
	configFiles := config.DEFAULT_CONFIG_FILES
	if cmdLine.Config != "" {
		configFiles = cmdLine.Config
	}

	// Parse default options from config files
	def := config.ParseConfigFiles(configFiles, cmdLine.Debug)
	if def.IFS == "" {
		def.IFS = config.DEFAULT_IFS
	}
	if def.OutputFormat == "" {
		def.OutputFormat = config.DEFAULT_OUTPUT_FORMAT
	}
	if def.Timeout == 0 {
		def.Timeout = config.DEFAULT_TIMEOUT
	}

	// Parse env vars and cmd line options, override default config
	cmdLine = config.ParseCommandLine(def)

	// Parse args: entity[.labels] query
	if len(cmdLine.Args) == 0 {
		config.Help()
		os.Exit(0)
	}
	if len(cmdLine.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Not enough arguments: entity and query are required\n")
		config.Help()
		os.Exit(1)
	}
	ctx.EntityType, ctx.ReturnLabels, ctx.Query = config.ParseArgs(cmdLine.Args)

	// Finalize options
	var o config.Options = cmdLine.Options
	if o.Debug {
		app.Debug("options: %#v\n", o)
		app.Debug("query: %s %s '%s'\n", ctx.EntityType, ctx.ReturnLabels, ctx.Query)
	}

	if ctx.Hooks.AfterParseOptions != nil {
		if o.Debug {
			app.Debug("calling hook AfterParseOptions")
		}
		ctx.Hooks.AfterParseOptions(&o)

		// Dump options again to see if hook changed them
		if o.Debug {
			app.Debug("options: %#v\n", o)
		}
	}
	ctx.Options = o

	// //////////////////////////////////////////////////////////////////////
	// Help and version
	// //////////////////////////////////////////////////////////////////////

	// es with no args (Args[0] = "es" itself). Print short request help
	// because Ryan is very busy.
	if len(os.Args) == 1 || o.Help {
		config.Help()
		os.Exit(0)
	}

	// es --version or es version
	if o.Version {
		fmt.Println("es v0.0.0")
		os.Exit(0)
	}

	// //////////////////////////////////////////////////////////////////////
	// Make etre.EntityClient
	// //////////////////////////////////////////////////////////////////////

	if ctx.Options.Addr == "" {
		fmt.Fprintf(os.Stderr, "Etre API address is not set."+
			" It is best to specify addr in a config file (%s). Or, specify"+
			" --addr on the command line option or set the ADDR environment"+
			" variable. Use --ping to test addr when set.\n", config.DEFAULT_CONFIG_FILES)
		os.Exit(1)
	}
	if ctx.Options.Debug {
		app.Debug("addr: %s", ctx.Options.Addr)
	}

	ec, err := ctx.Factories.EntityClient.Make(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error making etre.EntityClient: %s", err)
		os.Exit(1)
	}

	// //////////////////////////////////////////////////////////////////////
	// Ping
	// //////////////////////////////////////////////////////////////////////

	if o.Ping {
		// @todo
		fmt.Printf("-ping todo")
		os.Exit(0)
	}

	// //////////////////////////////////////////////////////////////////////
	// Query
	// //////////////////////////////////////////////////////////////////////

	if ctx.Hooks.BeforeQuery != nil {
		if o.Debug {
			app.Debug("calling hook BeforeQuery")
		}
		ctx.Hooks.BeforeQuery(&ctx)
		if o.Debug {
			app.Debug("query: %s %s '%s'\n", ctx.EntityType, ctx.ReturnLabels, ctx.Query)
		}
	}

	f := etre.QueryFilter{
		ReturnLabels: ctx.ReturnLabels,
	}
	entities, err := ec.Query(ctx.Query, f)
	if o.Debug {
		app.Debug("%d entities, err: %v", len(entities), err)
	}

	// //////////////////////////////////////////////////////////////////////
	// Handle response
	// //////////////////////////////////////////////////////////////////////

	// If Response hook set, let it handle the reponse.
	if ctx.Hooks.Response != nil {
		if o.Debug {
			app.Debug("calling hook Response")
		}
		ctx.Hooks.Response(ctx, entities, err)
		return
	}

	// Else, do the default: print the entities, if no error.
	if err != nil {
		fmt.Fprintf(os.Stderr, "API error: %s\n", err)
		os.Exit(1)
	}

	// No entities? No fun. :-(
	if len(entities) == 0 {
		return
	}

	switch ctx.Options.OutputFormat {
	case "line":
		// Yay, entities! If no return labels were specified, then Etre will have
		// returned complete entities (i.e. all labels), so default to that.
		returnLabels := ctx.ReturnLabels
		if len(ctx.ReturnLabels) == 0 {
			returnLabels = entities[0].Labels() // all labels, sorted
		}
		lastLabel := len(returnLabels) - 1 // don't print IFS after last label

		// Print every label value, in order of returnLabels. So if user queried
		// host.b,a,t then print values for b,a,t in that exact order. This is
		// critical because user might be doing this:
		//
		//   IFS=,
		//   set $target
		//
		// Which sets Bash $1=b, $2=a, $3=t. Of course, if user did not specify
		// return labels, they'll get all labels (above), sorted by label name.
		for _, e := range entities {
			for n, label := range returnLabels {
				fmt.Print(e[label])
				if n < lastLabel { // "b,a,t" not "b,a,t,"
					fmt.Print(ctx.Options.IFS)
				}
			}
			fmt.Println()
		}
	case "json":
		bytes, err := json.Marshal(entities)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			os.Exit(1)
		}
		fmt.Fprintln(ctx.Out, string(bytes))
	}
}
