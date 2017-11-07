// Copyright 2017, Square, Inc.

package etre

import (
	"fmt"
	"sort"

	"github.com/golang/groupcache/lru"
)

type ByRev []CDCEvent

func (a ByRev) Len() int           { return len(a) }
func (a ByRev) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByRev) Less(i, j int) bool { return a[i].Rev < a[j].Rev }

const DEFAULT_MAX_ENTITIES = 1000

// RevOrder handles entity revision ordering. Normally, an Etre CDC feed
// sends entities in revision order (ascending revision numbers). It is
// possible, although extremely unlikely, that revisions can be received
// out of order. A RevOrder handles this by buffering the out-of-order
// revisions until a complete, in-order sequence is received. It can be
// used by a CDC feed consumer like:
//
//   revo := etre.NewRevOrder(0) // 0 = DEFAULT_MAX_ENTITIES
//
//   // Handle out-of-order revisions (per entity ID)
//   ok, prev := revo.InOrder(event)
//   if !ok {
//       // This rev is out of order, skip (buffered in revo)
//       continue
//   }
//
//   // Sync ordered set of previously out-of-order revs
//   if prev != nil {
//       for _, p := range prev {
//           if err := sync(p); err != nil {
//               return err
//           }
//       }
//   }
//
//   // Sync the current rev (in order)
//   if err := sync(event); err != nil {
//       return err
//   }
//
// Using a RevOrder is optional but a best practice to be safe.
type RevOrder struct {
	reorder map[string]*events // keyed on CDCEvent.EntityId
	lru     *lru.Cache
}

type events struct {
	qR  uint       // previous rev (see algorithm below)
	buf []CDCEvent // ordered set of revisions > qR
}

// NewRevOrder returns a new RevOrder that tracks the last revision of at most
// maxEntities using an LRU cache. If NewRevOrder is zero, DEFAULT_MAX_ENTITIES
// is used. It's not possible or necessary to track all entities. If an entity
// hasn't been seen in awhile (e.g. 1,000 entities later), we presume that it
// won't be seen again, but if it is we can further presume that the next
// revision is the correct next revision to the previous revision seen but
// evicted from the LRU cache.
func NewRevOrder(maxEntities uint) *RevOrder {
	if maxEntities == 0 {
		maxEntities = DEFAULT_MAX_ENTITIES
	}
	r := &RevOrder{
		reorder: map[string]*events{},
		lru:     lru.New(int(maxEntities)),
	}
	r.lru.OnEvicted = r.onEvictedCallback
	return r

}

// InOrder returns true if the given event is in order. If not (false), the caller
// should skip and ignore the event. If true, a non-nil slice of previous revisions
// is also returned when they complete an in-order sequnce to but not including the
// given event. In this case, the caller should sync all previous revisions first,
// then sync the given event.
func (r *RevOrder) InOrder(e CDCEvent) (bool, []CDCEvent) {
	// The algorithm is:
	//	 Let N = revision of current event, not synced
	//	 Let R = revision of event, synced
	//	 Let {qR, rR+1} = ordered set of events (ascending revisions)
	//	 Let B = ordered set of events > qR
	//	 1) Assume qR=N for first event
	//	  a) return
	//	 2) Let rR=N
	//	 3) Panic  if rR < qR
	//	 4) Return if rR = qR
	//	 5) Iff rR = qR+1, and rR not in B
	//	  a) sync rR
	//	  b) set qR=rR
	//	  c) return
	//	 6) Add rR to B, sort
	//	 7) Iff for all B, B[i] = qR+(1+i)
	//	  a) sync B
	//	  b) qR=B[-1]

	// Get entity by ID from LRU cache
	v, seen := r.lru.Get(e.EntityId)

	// First time we see entity, we assume its rev to be a safe starting point
	if !seen {
		debug("add id: %s", e.EntityId)
		r.lru.Add(e.EntityId, e.Rev)
		return true, nil // sync event
	}

	// We've seen this entity before. Compare previous rev (qR) to current (rR)
	qR := v.(uint)
	rR := e.Rev
	debug("id %s qR %d rR %d", e.EntityId, qR, rR)

	// If current rev < previous rev, we've hit the most rare edge case and
	// there's no recovering here. User will need to re-sync from an earlier
	// time. This happens if the very first time we see the entity the rev=N+1
	// and seond time it's N. In other words: when the assumption noted above ^
	// turns out to be false.
	if rR < qR {
		msg := fmt.Sprintf("Entity %s is out of order because revision %d"+
			" received first and revision %d received second. The first"+
			" revision received is presumed to be in-order. This is a rare"+
			" edge case that could be corrected by replaying the CDC feed"+
			" from a time before entity %s revision %d. If this panic happens"+
			" again, file a bug report at https://github.com/square/etre",
			e.EntityId, qR, rR, e.EntityId, rR)
		panic(msg)
	}

	// Same rev as last time? Ignore it; don't sync (it should have already been
	// synced first time we saw it.)
	if rR == qR {
		return false, nil // don't sync
	}

	// Are we reordering revs?
	re, ok := r.reorder[e.EntityId]

	// The normal case: we're not reordering and this rev is exactly +1 of the
	// previous rev. This should be the 99.99999% case.
	if !ok && rR == qR+1 {
		r.lru.Add(e.EntityId, e.Rev)
		return true, nil // sync event
	}

	// This rev is out of order: +2 or more > previous. Buffer and wait for the
	// missing (earlier) revs.

	// Save and return early if first out-of-order rev.
	if !ok {
		debug("reorder id %s from %d", e.EntityId, qR)
		r.reorder[e.EntityId] = &events{
			qR:  qR,
			buf: []CDCEvent{e},
		}
		return false, nil // don't sync
	}

	// This is 2nd and subsequent out-of-order- rev. E.g. 3, and 4, if prev rev = 2
	// and first out-of-order received was 5. Add to buffer and sort.
	debug("buffer rev %d", e.Rev)
	re.buf = append(re.buf, e)
	sort.Sort(ByRev(re.buf))

	// We've got all the revs in sequence when we can walk from prev rev through
	// the buff +1 by +1. So if prev rev = 2, eventually we'll receive 3, 4, and 5,
	// so 2+1 = 3, 3+1 = 4, 4+1 = 5 == complete rev sequence.
	for i, b := range re.buf {
		if b.Rev != re.qR+1+uint(i) {
			debug("reorder fails at %d: %d != %d", i, b.Rev, re.qR+1+uint(i))
			return false, nil // don't sync
		}
	}

	// Complete rev sequence. Return only the buf of previously out-of-order events,
	// _not_ the current event--the caller already has that one.
	buf := re.buf
	qR = buf[len(buf)-1].Rev
	r.lru.Add(e.EntityId, qR)
	debug("reorder complete: id %s (%d, %d]", e.EntityId, re.qR, qR)

	delete(r.reorder, e.EntityId) // done reordering

	return true, buf // sync buf then event
}

// This func is called by r.lru when it evicts a key. We never call it directly.
func (r *RevOrder) onEvictedCallback(key lru.Key, value interface{}) {
	id := key.(string)
	debug("evict id: %s", id)

	// This should never happen: the oldest key (entity ID) is still being
	// reordered. If maxEntities is tiny (< 10), then maybe this is just bad luck,
	// but more than likely we'll never receive the out-of-order revs, which
	// indicates a serious problem: we lost revs.
	if re, ok := r.reorder[id]; ok {
		msg1 := fmt.Sprintf("Entity %s evicted from the LRU cache because"+
			" it is the oldest entity but still waiting to receive revisions"+
			" > %d. Received revisions:", id, re.qR)
		for _, b := range re.buf {
			msg1 += fmt.Sprintf(" %d", b.Rev)
		}
		msg2 := fmt.Sprintf(". This is a rare edge case that indicates some revisions"+
			" were not received or extremely delayed. Increasing maxEntities and replaying"+
			" the CDC feed from a time before entity %s revision %d could fix this problem."+
			" Please file a bug at https://github.com/square/etre",
			id, re.qR)
		panic(msg1 + msg2)
	}
}
