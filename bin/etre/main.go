// Copyright 2017-2019, Square, Inc.

package main

import (
	"flag"
	"log"

	"github.com/square/etre/app"
	"github.com/square/etre/server"
)

var configFile string

func init() {
	flag.StringVar(&configFile, "config", "", "Config file")
}

func main() {
	flag.Parse()
	s := server.NewServer(app.Defaults())
	if err := s.Boot(configFile); err != nil {
		log.Fatal(err)
	}
	if err := s.Run(); err != nil {
		log.Fatal(err)
	}
	log.Println("Etre has stopped")
}
