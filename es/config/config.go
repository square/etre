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
	DEFAULT_CONFIG_FILES  = "/etc/etre/es.yaml,~/.es.yaml"
	DEFAULT_IFS           = ","
	DEFAULT_OUTPUT_FORMAT = "line"
	DEFAULT_TIMEOUT       = 10000 // 10s
)

var (
	ErrUnknownRequest = errors.New("request does not exist")
)

// Options represents typical command line options: --addr, --config, etc.
type Options struct {
	Addr         string `arg:"env" yaml:"addr"`
	Config       string `arg:"env"`
	Debug        bool
	Env          string
	Help         bool
	IFS          string `arg:"env" yaml:"ifs"`
	OutputFormat string `arg:"env" yaml:"output-format"`
	Ping         bool
	Timeout      uint `arg:"env" yaml:"timeout"`
	Version      bool
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
	fmt.Printf("Usage: es [flags] entity[.label,...] query\n\n"+
		"Flags:\n"+
		"  --addr          Address of Request Managers (https://local.domain:8080)\n"+
		"  --config        Config files (default: %s)\n"+
		"  --debug         Print debug to stderr\n"+
		"  --env           Environment (dev, staging, production)\n"+
		"  --help          Print help\n"+
		"  --ifs           Internal field separator to print between label values for line output (default: %s)\n"+
		"  --output-format Output format to print entities: line or json (default: %s)\n"+
		"  --ping          Ping addr\n"+
		"  --timeout       API timeout, milliseconds (default: %d)\n"+
		"  --version       Print version\n",
		DEFAULT_CONFIG_FILES, DEFAULT_IFS, DEFAULT_OUTPUT_FORMAT, DEFAULT_TIMEOUT)
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
