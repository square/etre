// Copyright 2017-2018, Square, Inc.

package main

import (
	"log"

	"github.com/square/etre/app"
	"github.com/square/etre/server"
)

func main() {
	s := server.NewServer(app.Defaults())
	if err := s.Boot(); err != nil {
		log.Fatal(err)
	}
	if err := s.Run(); err != nil {
		log.Fatal(err)
	}
	log.Println("Etre has stopped")
}
