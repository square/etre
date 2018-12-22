// Copyright 2017-2018, Square, Inc.

package main

import (
	"log"
	"os"
	"strings"

	"github.com/square/etre/app"
	"github.com/square/etre/config"
	"github.com/square/etre/server"
)

func main() {
	var cfg config.Config
	var err error
	switch len(os.Args) {
	case 2:
		cfg, err = config.Load(os.Args[1])
		if err != nil {
			log.Fatal(err)
		}
	case 1:
		log.Println("config file not specified")
		log.Fatalf("usage: %s CONFIG_FILE", os.Args[0])
	default:
		log.Printf("unknown args after config file: %s", strings.Join(os.Args[2:], " "))
		log.Fatalf("usage: %s CONFIG_FILE", os.Args[0])
	}

	s := server.NewServer(app.DefaultContext(cfg))
	if err := s.Boot(); err != nil {
		log.Fatal(err)
	}
	if err := s.Run(); err != nil {
		log.Fatal(err)
	}
	log.Println("server has stopped, no error")
}
