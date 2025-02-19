// Copyright 2017-2020, Square, Inc.

package etre_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	assert.True(t, ok)
	assert.Nil(t, buf)

	ok, buf = revo.InOrder(e)
	assert.False(t, ok, "expected not ok becuase it's the same rev")
	assert.Nil(t, buf)

	e.EntityRev = 1
	ok, buf = revo.InOrder(e)
	assert.True(t, ok, "expected ok becuase rev += 1")
	assert.Nil(t, buf)

	e.EntityRev = 3
	ok, buf = revo.InOrder(e)
	assert.False(t, ok, "expected not ok becuase rev 2 not sent yet")
	assert.Nil(t, buf)

	e.EntityRev = 4
	ok, buf = revo.InOrder(e)
	assert.False(t, ok, "expected not ok becuase rev 2 not sent yet")
	assert.Nil(t, buf)

	e.EntityRev = 2
	ok, buf = revo.InOrder(e)
	assert.True(t, ok, "expected ok becuase rev set complete (2, 3)")
	assert.NotNil(t, buf, "expected buf [2,3]")
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
	assert.Equal(t, expect, buf)
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
		require.NotNil(t, r, "nil panic/recover, expected non-nil recover() value")
		assert.Contains(t, r.(string), "revision 1 received first and revision 0 received second")
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
		require.NotNil(t, r)
		assert.Contains(t, r.(string), "Entity abc evicted")
		assert.Contains(t, r.(string), "revisions > 2. Received revisions: 4")
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
		assert.Nil(t, r)
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for goroutine to panic")
	}
}
