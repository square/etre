// Copyright 2017-2020, Square, Inc.

package etre_test

import (
	"strings"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/square/etre"
)

func TestRevOrder(t *testing.T) {
	revo := etre.NewRevOrder(0, false)

	e := etre.CDCEvent{
		EntityId:  "abc",
		EntityRev: 0,
		Op:        "i",
	}

	ok, buf := revo.InOrder(e)
	if !ok {
		t.Error("not ok, expected ok")
	}
	if buf != nil {
		t.Errorf("buf not nil, expected nil: %#v", buf)
	}

	ok, buf = revo.InOrder(e)
	if ok {
		t.Error("ok, expected not ok becuase it's the same rev")
	}
	if buf != nil {
		t.Errorf("buf not nil, expected nil: %#v", buf)
	}

	e.EntityRev = 1
	ok, buf = revo.InOrder(e)
	if !ok {
		t.Error("not ok, expected ok becuase rev += 1")
	}
	if buf != nil {
		t.Errorf("buf not nil, expected nil: %#v", buf)
	}

	e.EntityRev = 3
	ok, buf = revo.InOrder(e)
	if ok {
		t.Error("ok, expected not ok becuase rev 2 not sent yet")
	}
	if buf != nil {
		t.Errorf("buf not nil, expected nil: %#v", buf)
	}

	e.EntityRev = 4
	ok, buf = revo.InOrder(e)
	if ok {
		t.Error("ok, expected not ok becuase rev 2 not sent yet")
	}
	if buf != nil {
		t.Errorf("buf not nil, expected nil: %#v", buf)
	}

	e.EntityRev = 2
	ok, buf = revo.InOrder(e)
	if !ok {
		t.Error("not ok, expected ok becuase rev set complete (2, 3)")
	}
	if buf == nil {
		t.Errorf("buf nil, expected [2,3]")
	}
	expect := []etre.CDCEvent{
		{
			EntityId:  "abc",
			EntityRev: 2,
			Op:        "i",
		},
		{
			EntityId:  "abc",
			EntityRev: 3,
			Op:        "i",
		},
		{
			EntityId:  "abc",
			EntityRev: 4,
			Op:        "i",
		},
	}
	if diffs := deep.Equal(buf, expect); diffs != nil {
		t.Error(diffs)
	}
}

func TestRevOrderPanicRRltQR(t *testing.T) {
	recoverChan := make(chan interface{}, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				recoverChan <- r
			} else {
				recoverChan <- nil
			}
		}()
		revo := etre.NewRevOrder(10, false)
		e := etre.CDCEvent{
			EntityId:  "abc",
			EntityRev: 1,
			Op:        "i",
		}
		revo.InOrder(e)
		e.EntityRev = 0 // causes panic
		revo.InOrder(e)
	}()

	select {
	case r := <-recoverChan:
		if r == nil {
			t.Errorf("nil panic/recover, expected non-nil recover() value")
		} else {
			pat := "revision 1 received first and revision 0 received second"
			if !strings.Contains(r.(string), pat) {
				t.Errorf("panic msg doesn't contain '%s': %s", pat, r)
			}
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for goroutine to panic")
	}
}

func TestRevOrderPanicEvict(t *testing.T) {
	recoverChan := make(chan interface{}, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				recoverChan <- r
			} else {
				recoverChan <- nil
			}
		}()
		revo := etre.NewRevOrder(2, false)
		e := etre.CDCEvent{
			EntityId:  "abc",
			EntityRev: 1,
			Op:        "i",
		}
		revo.InOrder(e)
		e.EntityRev = 2
		revo.InOrder(e)
		e.EntityRev = 4
		revo.InOrder(e)
		// Now abc is being reordered

		e.EntityId = "def"
		e.EntityRev = 1
		revo.InOrder(e)
		e.EntityRev = 2
		revo.InOrder(e)
		// Now LRU cache is full: abc and def

		// Add entity ghi which evicts abc and causes the panic
		e.EntityId = "ghi"
		revo.InOrder(e)
	}()

	select {
	case r := <-recoverChan:
		if r == nil {
			t.Errorf("nil panic/recover, expected non-nil recover() value")
		} else {
			pat1 := "Entity abc evicted"
			if !strings.Contains(r.(string), pat1) {
				t.Errorf("panic msg doesn't contain '%s': %s", pat1, r)
			}
			pat2 := "revisions > 2. Received revisions: 4"
			if !strings.Contains(r.(string), pat2) {
				t.Errorf("panic msg doesn't contain '%s': %s", pat2, r)
			}
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for goroutine to panic")
	}
}

func TestRevOrderIgnorePastRevs(t *testing.T) {
	// Same as TestRevOrderPanicRRltQR but ignorePastRevs=true which ignores
	// the past rev instead of panicing. This is used in changestream.ServerStreamer
	// because it has two inputs: events from the backlog (cdc.Store) and
	// events from a MongoDB change stream. These two can overlap at the start.
	recoverChan := make(chan interface{}, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				recoverChan <- r
			} else {
				recoverChan <- nil
			}
		}()
		revo := etre.NewRevOrder(10, true) // = ignorePastRevs
		e := etre.CDCEvent{
			EntityId:  "abc",
			EntityRev: 1,
			Op:        "i",
		}
		revo.InOrder(e)
		e.EntityRev = 0 // does NOT cause panic
		revo.InOrder(e)
	}()

	select {
	case r := <-recoverChan:
		if r != nil {
			t.Errorf("panic, expected nil: %v", r)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for goroutine to panic")
	}
}
