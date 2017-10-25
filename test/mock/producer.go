// Copyright 2017, Square, Inc.

package mock

//import (
//	"errors"
//
//	"github.com/square/etre"
//	"github.com/square/etre/cdc"
//)
//
//var (
//	ErrProducer = errors.New("error in producer")
//)
//
//type Producer struct {
//	StartFunc func(int64, int) <-chan etre.CDCEvent
//	StopFunc  func()
//	ErrorFunc func() error
//}
//
//func (p *Producer) Start(startTs int64, chunkWindow int) <-chan etre.CDCEvent {
//	if p.StartFunc != nil {
//		return p.StartFunc(startTs, chunkWindow)
//	}
//	return nil
//}
//
//func (p *Producer) Stop() {
//	if p.StopFunc != nil {
//		p.StopFunc()
//	}
//}
//
//func (p *Producer) Error() error {
//	if p.ErrorFunc != nil {
//		return p.ErrorFunc()
//	}
//	return nil
//}
//
//type ProducerFactory struct {
//	MakeFunc func(string) cdc.Producer
//}
//
//func (pf *ProducerFactory) Make(id string) cdc.Producer {
//	if pf.MakeFunc != nil {
//		return pf.MakeFunc(id)
//	}
//	return nil
//}
