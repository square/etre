package server

import (
	"log"
	"net/http"

	"github.com/square/etre"
	"github.com/square/etre/api"
	"github.com/square/etre/app"
)

type Server struct {
	appCtx app.Context
	api    api.API
}

func NewServer(appCtx app.Context) Server {
	return Server{
		appCtx: appCtx,
		api:    api.NewAPI(appCtx),
	}
}

func (s Server) Boot() error {
	// @todo: no background components to start atm?
	return nil
}

func (s Server) Run() error {
	log.SetFlags(log.Lshortfile | log.Ldate | log.Lmicroseconds)
	log.Printf("Etre %s\n", etre.VERSION)

	addr := s.appCtx.Config.Server.Addr
	crt := s.appCtx.Config.Server.TLSCert
	key := s.appCtx.Config.Server.TLSKey
	if crt != "" && key != "" {
		log.Printf("Listening on %s with TLS", addr)
		return http.ListenAndServeTLS(addr, crt, key, s.api)
	} else {
		log.Printf("Listening on %s", addr)
		return http.ListenAndServe(addr, s.api)
	}
}

func (s Server) API() api.API {
	return s.api
}
