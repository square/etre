// Copyright 2017, Square, Inc.

// Package config handles config files, -config, and env vars at startup.
package config

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/alexflint/go-arg"
	"gopkg.in/yaml.v2"
)

const (
	DEFAULT_CONFIG_FILES = "/etc/etre/es.yaml,~/.es.yaml"
	DEFAULT_IFS          = ","
	DEFAULT_TIMEOUT      = 5000 // 5s
)

var (
	ErrUnknownRequest = errors.New("request does not exist")
)

// Options represents typical command line options: --addr, --config, etc.
type Options struct {
	Addr    string `arg:"env" yaml:"addr"`
	Config  string `arg:"env"`
	Debug   bool   `arg:"env" yaml:"debug"`
	Delete  bool
	Env     string `arg:"env" yaml:"env"`
	Help    bool
	JSON    bool   `arg:"env" yaml:"json"`
	IFS     string `arg:"env" yaml:"ifs"`
	Labels  bool   `arg:"env" yaml:"labels"`
	Ping    bool
	Old     bool   `arg:"env" yaml:"old"`
	SetOp   string `arg:"--set-op,env:SET_OP"`
	SetId   string `arg:"--set-id,env:SET_ID"`
	SetSize int    `arg:"--set-size,env:SET_SIZE"`
	Strict  bool   `arg:"env" yaml:"strict"`
	Timeout uint   `arg:"env" yaml:"timeout"`
	Update  bool
	Version bool `arg:"-v"`
}

// CommandLine represents options (--addr, etc.) and args: entity type, return
// labels, and query predicates. The caller is expected to copy and use the embedded
// structs separately, like:
//
//   var o config.Options = cmdLine.Options
//   for i, arg := range cmdline.Args {
//
type CommandLine struct {
	Options
	Args []string `arg:"positional"`
}

// ParseCommandLine parses the command line and env vars. Command line options
// override env vars. Default options are used unless overridden by env vars or
// command line options. Defaults are usually parsed from config files.
func ParseCommandLine(def Options) CommandLine {
	var c CommandLine
	c.Options = def
	p, err := arg.NewParser(arg.Config{Program: "es"}, &c)
	if err != nil {
		fmt.Printf("arg.NewParser: %s", err)
		os.Exit(1)
	}
	if err := p.Parse(os.Args[1:]); err != nil {
		switch err {
		case arg.ErrHelp:
			c.Help = true
		case arg.ErrVersion:
			c.Version = true
		default:
			fmt.Printf("Error parsing command line: %s\n", err)
			os.Exit(1)
		}
	}
	return c
}

func Help() {
	fmt.Printf("Usage:\n"+
		"   Query: es [options] entity[.label,...] query\n"+
		"  Update: es [options] --update entity id patches\n"+
		"  Delete: es [options] --delete entity id\n\n"+
		"Args:\n"+
		"  entity     Valid entity type (Etre API config.entity.types)\n"+
		"  label      Comma-separated list of labels to return, like: host.zone,env\n"+
		"  query      Query string, like: env=production, zone in (east, west)\n"+
		"  id         Internal ID (_id) of an entity, like: 507f1f77bcf86cd799439011\n"+
		"  patches    New label=value pairs, like: zone=west status=online\n\n"+
		"Options:\n"+
		"  --addr     Etre API address (example: http://localhost:8080)\n"+
		"  --config   Config files (default: %s)\n"+
		"  --debug    Print debug to stderr\n"+
		"  --delete   Delete one entity by id\n"+
		"  --env      Environment (dev, staging, production)\n"+
		"  --help     Print help\n"+
		"  --ifs      Character to print between label values (default: %s)\n"+
		"  --json     Print entities as JSON\n"+
		"  --labels   Print label: before value\n"+
		"  --ping     Ping addr\n"+
		"  --old      Print old values on --update\n"+
		"  --set-id   User-defined set ID for --update and --delete\n"+
		"  --set-op   User-defined set op for --update and --delete\n"+
		"  --set-size User-defined set size for --update and --delete (must be > 0)\n"+
		"  --strict   Error if query or --delete does not match entities\n"+
		"  --timeout  API timeout, milliseconds (default: %d)\n"+
		"  --update   Apply patches to one entity by id\n"+
		"  --version  Print version\n\n",
		DEFAULT_CONFIG_FILES, DEFAULT_IFS, DEFAULT_TIMEOUT)
}

func ParseConfigFiles(files string, debug bool) Options {
	var def Options
	for _, file := range strings.Split(files, ",") {
		// If file starts with ~/, we need to expand this to the user home dir
		// because this is a shell expansion, not something Go knows about.
		if file[:2] == "~/" {
			usr, _ := user.Current()
			file = filepath.Join(usr.HomeDir, file[2:])
		}

		absfile, err := filepath.Abs(file)
		if err != nil {
			if debug {
				log.Printf("filepath.Abs(%s) error: %s", file, err)
			}
			continue
		}

		bytes, err := ioutil.ReadFile(absfile)
		if err != nil {
			if debug {
				log.Printf("Cannot read config file %s: %s", file, err)
			}
			continue
		}

		var o Options
		if err := yaml.Unmarshal(bytes, &o); err != nil {
			if debug {
				log.Printf("Invalid YAML in config file %s: %s", file, err)
			}
			continue
		}

		// Set options from this config file only if they're set
		if debug {
			log.Printf("Applying config file %s (%s)", file, absfile)
		}
		if o.Addr != "" {
			def.Addr = o.Addr
		}
		if o.Timeout != 0 {
			def.Timeout = o.Timeout
		}
	}
	return def
}

func ParseArgs(args []string) (string, []string, string) {
	if len(args) == 0 {
		return "", nil, ""
	}

	// Example inputs:
	// args 0        1   2
	//      node     a=b
	//      node.x   a=b
	//      node.x,y a=b
	//      node.z   a=b c=d

	entityType := args[0]
	var returnLabels []string

	p := strings.Split(entityType, ".")
	if len(p) == 2 {
		entityType = p[0]
		returnLabels = strings.Split(p[1], ",")
	}

	var query string
	if len(args) > 1 {
		query = strings.Join(args[1:], " ")
	}

	return entityType, returnLabels, query
}
