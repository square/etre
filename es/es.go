// Copyright 2017, Square, Inc.

// Package es provides a framework for integration with other programs.
package es

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

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
	if def.Timeout == 0 {
		def.Timeout = config.DEFAULT_TIMEOUT
	}

	// Parse env vars and cmd line options, override default config
	cmdLine = config.ParseCommandLine(def)

	// es --version and exit
	if cmdLine.Options.Version {
		fmt.Println("es " + etre.VERSION)
		os.Exit(0)
	}

	// Print help and exit if --help or nothing given on cmd line
	if cmdLine.Options.Help || (len(cmdLine.Args) == 0 && (!cmdLine.Options.Delete && !cmdLine.Options.Update)) {
		config.Help()
		os.Exit(0)
	}

	// Validate cmd line options and args
	if cmdLine.Options.Delete && cmdLine.Options.Update {
		config.Help()
		fmt.Fprintf(os.Stderr, "--update and --delete are mutually exclusive\n")
		os.Exit(1)
	}

	if cmdLine.Options.Delete { // --delete
		if len(cmdLine.Args) < 2 {
			config.Help()
			fmt.Fprintf(os.Stderr, "Not enough arguments for --delete: entity and id are required\n")
			os.Exit(1)
		}
		if len(cmdLine.Args) > 2 {
			config.Help()
			fmt.Fprintf(os.Stderr, "Too many arguments for --delete: specify only entity and id (%d extra arguments: %s)\n",
				len(cmdLine.Args[2:]), cmdLine.Args[2:])
			os.Exit(1)
		}
	} else if cmdLine.Options.Update { // --update
		if len(cmdLine.Args) < 3 {
			config.Help()
			fmt.Fprintf(os.Stderr, "Not enough arguments for --update: entity, id, and patches are required\n")
			os.Exit(1)
		}
	} else { // query
		if len(cmdLine.Args) < 2 {
			config.Help()
			fmt.Fprintf(os.Stderr, "Not enough arguments for query: entity and query are required\n")
			os.Exit(1)
		}
	}

	var set etre.Set
	if cmdLine.Options.SetSize > 0 || cmdLine.Options.SetOp != "" || cmdLine.Options.SetId != "" {
		if cmdLine.Options.SetSize == 0 || cmdLine.Options.SetOp == "" || cmdLine.Options.SetId == "" {
			fmt.Fprintf(os.Stderr, "All three --set options (or environment variables: SET_OP, SET_ID, SET_SIZE) must be specified\n")
			os.Exit(1)
		}
		set = etre.Set{
			Op:   cmdLine.Options.SetOp,
			Id:   cmdLine.Options.SetId,
			Size: cmdLine.Options.SetSize,
		}
	}

	// Finalize options
	var o config.Options = cmdLine.Options
	if o.Debug {
		app.Debug("options: %#v\n", o)
		app.Debug("set: %#v\n", set)
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
	// Make etre.EntityClient
	// //////////////////////////////////////////////////////////////////////

	// cmdLine.Args validated above
	entityType := strings.SplitN(cmdLine.Args[0], ".", 2)
	ctx.EntityType = entityType[0]

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
		fmt.Fprintf(os.Stderr, "Error making etre.EntityClient: %s\n", err)
		os.Exit(1)
	}

	if set.Size > 0 {
		ec = ec.WithSet(set)
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
	// Update and exit, if --update
	// //////////////////////////////////////////////////////////////////////

	if o.Update {
		// cmdLine.Args validated above
		ctx.EntityId = cmdLine.Args[1]
		ctx.Patches = cmdLine.Args[2:]

		if ctx.Hooks.BeforeUpdate != nil {
			if o.Debug {
				app.Debug("calling hook BeforeUpdate")
			}
			ctx.Hooks.BeforeUpdate(&ctx)
		}

		patch := etre.Entity{}
		for _, kv := range ctx.Patches {
			p := strings.SplitN(kv, "=", 2)
			if len(p) != 2 {
				fmt.Fprintf(os.Stderr, "Invalid patch: %s: split on = yielded %d parts, expected 2\n", patch, len(p))
				os.Exit(1)
			}
			patch[p[0]] = p[1]
		}
		if o.Debug {
			app.Debug("patch: %#v", patch)
		}

		wr, err := ec.UpdateOne(ctx.EntityId, patch)
		if err != nil {
			fmt.Fprintf(os.Stderr, "API error: %s\n", err)
			os.Exit(1)
		}
		if o.Debug {
			app.Debug("wr: %#v (%v)", wr, err)
		}

		if ctx.Hooks.WriteResult != nil {
			if o.Debug {
				app.Debug("calling hook WriteResult")
			}
			ctx.Hooks.WriteResult(ctx, wr, err)
		}

		if wr.Error != "" {
			fmt.Fprintf(os.Stderr, "API write error: %s\n", wr.Error)
			os.Exit(1)
		}
		if ctx.Options.Old {
			for _, label := range wr.Diff.Labels() {
				fmt.Printf("# %s=%v\n", label, wr.Diff[label])
			}
		}
		fmt.Printf("OK, updated %s %s%s\n", ctx.EntityType, ctx.EntityId, setInfo(set))
		return
	}

	// //////////////////////////////////////////////////////////////////////
	// Delete and exit, if --delete
	// //////////////////////////////////////////////////////////////////////

	if o.Delete {
		// cmdLine.Args validated above
		ctx.EntityId = cmdLine.Args[1]

		if ctx.Hooks.BeforeDelete != nil {
			if o.Debug {
				app.Debug("calling hook BeforeDelete")
			}
			ctx.Hooks.BeforeDelete(&ctx)
		}

		wr, err := ec.DeleteOne(ctx.EntityId)
		if o.Debug {
			app.Debug("wr: %#v (%v)", wr, err)
		}

		if ctx.Hooks.WriteResult != nil {
			if o.Debug {
				app.Debug("calling hook WriteResult")
			}
			ctx.Hooks.WriteResult(ctx, wr, err)
		}

		if err != nil {
			switch err {
			case etre.ErrEntityNotFound:
				if ctx.Options.Strict {
					fmt.Fprintf(os.Stderr, "Not found: %s %s does not exist\n", ctx.EntityType, ctx.EntityId)
					os.Exit(1)
				} else {
					fmt.Printf("OK, but %s %s did not exist%s\n", ctx.EntityType, ctx.EntityId, setInfo(set))
					return
				}
			default:
				fmt.Fprintf(os.Stderr, "API error: %s\n", err)
				os.Exit(1)
			}
		}

		if wr.Error != "" {
			fmt.Fprintf(os.Stderr, "API write error: %s\n", wr.Error)
			os.Exit(1)
		}
		if ctx.Options.Old {
			for _, label := range wr.Diff.Labels() {
				fmt.Printf("# %s=%v\n", label, wr.Diff[label])
			}
		}
		fmt.Printf("OK, deleted %s %s%s\n", ctx.EntityType, ctx.EntityId, setInfo(set))
		return
	}

	// //////////////////////////////////////////////////////////////////////
	// Query
	// //////////////////////////////////////////////////////////////////////

	// Parse args: entity[.labels] query
	ctx.EntityType, ctx.ReturnLabels, ctx.Query = config.ParseArgs(cmdLine.Args)
	if o.Debug {
		app.Debug("query: %s %s '%s'\n", ctx.EntityType, ctx.ReturnLabels, ctx.Query)
	}

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

	// If Response hook set, let it handle the reponse.
	if ctx.Hooks.AfterQuery != nil {
		if o.Debug {
			app.Debug("calling hook AfterQuery")
		}
		ctx.Hooks.AfterQuery(ctx, entities, err)
		return
	}

	// Else, do the default: print the entities, if no error.
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

	// No entities? No fun. :-(
	if len(entities) == 0 {
		if ctx.Options.Strict {
			os.Exit(1)
		}
		return
	}

	if ctx.Options.JSON {
		bytes, err := json.Marshal(entities)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			os.Exit(1)
		}
		fmt.Fprintln(ctx.Out, string(bytes))
	} else {
		// Yay, entities! If no return labels were specified, then Etre will have
		// returned complete entities (i.e. all labels), so default to that.
		returnLabels := ctx.ReturnLabels
		withLabels := ctx.Options.Labels
		if len(ctx.ReturnLabels) == 0 {
			returnLabels = entities[0].Labels() // all labels, sorted
			withLabels = true
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
				var val interface{} = e[label]
				if val == nil {
					// Entity probably doesn't have the label requested, so there's
					// no value, which Go prints as "<nil>", which is misleading,
					// so we  print "" (empty string) instead.
					val = ""
				}
				if withLabels {
					fmt.Print(label, ":", val)
				} else {
					fmt.Print(val)
				}
				if n < lastLabel { // "b,a,t" not "b,a,t,"
					fmt.Print(ctx.Options.IFS)
				}
			}
			fmt.Println()
		}
	}
}

func setInfo(set etre.Set) string {
	if set.Size == 0 {
		return ""
	}
	// Appended to an "OK, ..." message
	return fmt.Sprintf(" (set %s %s)", set.Op, set.Id)
}
