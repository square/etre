// Copyright 2017-2020, Square, Inc.

// Package config handles config files, -config, and env vars at startup.
package config

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/alexflint/go-arg"
	"gopkg.in/yaml.v2"

	"github.com/square/etre"
)

const (
	DEFAULT_ADDR          = "http://127.0.0.1:32084"
	DEFAULT_CONFIG_FILES  = "/etc/etre/es.yaml,~/.es.yaml"
	DEFAULT_IFS           = ","
	DEFAULT_QUERY_TIMEOUT = "5s"
	DEFAULT_TIMEOUT       = "10s"
	DEFAULT_RETRY         = 0
	DEFAULT_RETRY_WAIT    = "1s"
)

var (
	ErrUnknownRequest = errors.New("request does not exist")
)

// Options represents typical command line options: --addr, --config, etc.
type Options struct {
	Addr         string `arg:"env:ES_ADDR" yaml:"addr"`
	Config       string `arg:"env:ES_CONFIG"`
	Debug        bool   `arg:"env:ES_DEBUG" yaml:"debug"`
	Delete       bool
	DeleteLabel  bool   `arg:"--delete-label"`
	Env          string `arg:"env:ES_ENV" yaml:"env"`
	Help         bool
	JSON         bool   `arg:"env:ES_JSON" yaml:"json"`
	IFS          string `arg:"env" yaml:"ifs"`
	Labels       bool   `arg:"env:ES_LABELS" yaml:"labels"`
	Old          bool   `arg:"env:ES_OLD" yaml:"old"`
	QueryTimeout string `arg:"--query-timeout,env:ES_QUERY_TIMEOUT" yaml:"query_timeout"`
	Retry        uint   `arg:"env:ES_RETRY" yaml:"retry"`
	RetryWait    string `arg:"--retry-wait,env:ES_RETRY_WAIT" yaml:"retry_wait"`
	SetOp        string `arg:"--set-op,env:ES_SET_OP"`
	SetId        string `arg:"--set-id,env:ES_SET_ID"`
	SetSize      int    `arg:"--set-size,env:ES_SET_SIZE"`
	Strict       bool   `arg:"env:ES_STRICT" yaml:"strict"`
	Timeout      string `arg:"env:ES_TIMEOUT" yaml:"timeout"`
	Trace        string `arg:"env:ES_TRACE" yaml:"trace"`
	Update       bool
	Unique       bool `arg:"-u"`
	Version      bool `arg:"-v"`
	Watch        bool
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
		"         Query: es [options] entity[.label,...] query\n"+
		" Update Entity: es [options] --update entity id patches\n"+
		" Delete Entity: es [options] --delete entity id\n\n"+
		"  Delete Label: es [options] --delete-label entity id label\n"+
		" Watch Changes: es [options] --watch cdc\n\n"+
		"Args:\n"+
		"  entity     Valid entity type (Etre API config.entity.types)\n"+
		"  label      Comma-separated list of return labels, like: host.zone,env\n"+
		"  query      Query string, like: env=production, zone in (east, west)\n"+
		"  id         Internal ID (_id) of an entity, like: 507f1f77bcf86cd799439011\n"+
		"  patches    New label=value pairs, like: zone=west status=online\n\n"+
		"Options:\n"+
		"  --addr          Etre API address (default: %s)\n"+
		"  --config        Config files (default: %s)\n"+
		"  --debug         Print debug to stderr\n"+
		"  --delete        Delete one entity by id\n"+
		"  --delete-label  Delete entity label\n"+
		"  --env           Environment (dev, staging, production)\n"+
		"  --help          Print help\n"+
		"  --ifs           Character to print between label values (default: %s)\n"+
		"  --json          Print entities as JSON\n"+
		"  --labels        Print label: before value\n"+
		"  --old           Print old values on --update\n"+
		"  --query-timeout Query timeout on server (default: %s)\n"+
		"  --retry         Retry count on network or API error (default: %d)\n"+
		"  --retry-wait    Wait time between retries (default: %s)\n"+
		"  --set-id        User-defined set ID for --update and --delete\n"+
		"  --set-op        User-defined set op for --update and --delete\n"+
		"  --set-size      User-defined set size for --update and --delete (must be > 0)\n"+
		"  --strict        Error if query or --delete does not match entities\n"+
		"  --timeout       Response timeout per try, includes --query-timeout (default: %s)\n"+
		"  --trace         Comma-separated key=val pairs for server metrics\n"+
		"  --update        Apply patches to one entity by id\n"+
		"  --unique (-u)   Return distinct values of a single return label\n"+
		"  --version       Print version\n"+
		"  --watch         Stream changes from CDC\n\n",
		DEFAULT_ADDR, DEFAULT_CONFIG_FILES, DEFAULT_IFS, DEFAULT_QUERY_TIMEOUT, DEFAULT_RETRY, DEFAULT_RETRY_WAIT, DEFAULT_TIMEOUT)
}

func ParseConfigFiles(files string) Options {
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
			etre.Debug("filepath.Abs(%s) error: %s", file, err)
			continue
		}

		bytes, err := ioutil.ReadFile(absfile)
		if err != nil {
			etre.Debug("Cannot read config file %s: %s", file, err)
			continue
		}

		var o Options
		if err := yaml.Unmarshal(bytes, &o); err != nil {
			etre.Debug("Invalid YAML in config file %s: %s", file, err)
			continue
		}

		// Set options from this config file only if they're set
		etre.Debug("Applying config file %s (%s)", file, absfile)
		if o.Addr != "" {
			def.Addr = o.Addr
		}
		if o.Timeout != "" {
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

func DefaultTrace() string {
	trace := []string{}

	// Prefer SUDO_USER, USER, or whatever Go can determine,
	// in that order
	username := os.Getenv("SUDO_USER")
	if username == "" {
		username = os.Getenv("USER")
		if username == "" {
			usr, _ := user.Current()
			username = usr.Username
		}
	}
	if username != "" {
		trace = append(trace, "user="+username)
	}

	host := os.Getenv("HOSTNAME")
	if host == "" {
		host, _ = os.Hostname()
	}
	if host != "" {
		trace = append(trace, "host="+host)
	}

	if app := os.Getenv("APP"); app != "" {
		trace = append(trace, "app="+app)
	}

	return strings.Join(trace, ",")
}
