// Copyright 2017, Square, Inc.

package mock

import (
	"errors"

	"github.com/square/etre"
)

var (
	ErrPoller = errors.New("error in poller")
)

type Poller struct {
	RunFunc        func() error
	RegisterFunc   func(string) (<-chan etre.CDCEvent, int64, error)
	DeregisterFunc func(string)
	ErrorFunc      func() error
}

func (p *Poller) Run() error {
	if p.RunFunc != nil {
		p.RunFunc()
	}
	return nil
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
