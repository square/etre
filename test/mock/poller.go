// Copyright 2017, Square, Inc.

package mock

import (
	"github.com/square/etre"
)

type Poller struct {
	StartFunc      func()
	StopFunc       func()
	RegisterFunc   func(string) (<-chan etre.CDCEvent, int64, error)
	DeregisterFunc func(string)
	ErrorFunc      func() error
}

func (p *Poller) Start() {
	if p.StartFunc != nil {
		p.StartFunc()
	}
}

func (p *Poller) Stop() {
	if p.StopFunc != nil {
		p.StopFunc()
	}
}

func (p *Poller) Register(uuid string) (<-chan etre.CDCEvent, int64, error) {
	if p.RegisterFunc != nil {
		return p.RegisterFunc(uuid)
	}
	return nil, 0, nil
}

func (p *Poller) Deregister(uuid string) {
	if p.DeregisterFunc != nil {
		p.DeregisterFunc(uuid)
	}
}

func (p *Poller) Error() error {
	if p.ErrorFunc != nil {
		return p.ErrorFunc()
	}
	return nil
}
