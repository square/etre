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
func (a ByRev) Less(i, j int) bool { return a[i].EntityRev < a[j].EntityRev }

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
	reorder        map[string]*events // keyed on CDCEvent.EntityId
	lru            *lru.Cache
	ignorePastRevs bool
}

type events struct {
	qR  int64      // previous rev (see algorithm below)
	buf []CDCEvent // ordered set of revisions > qR
}

// NewRevOrder returns a new RevOrder that tracks the last revision of at most
// maxEntities using an LRU cache. If NewRevOrder is zero, DEFAULT_MAX_ENTITIES
// is used. It's not possible or necessary to track all entities. If an entity
// hasn't been seen in awhile (e.g. 1,000 entities later), we presume that it
// won't be seen again, but if it is we can further presume that the next
// revision is the correct next revision to the previous revision seen but
// evicted from the LRU cache.
//
// If ignorePastRevs is true, a revision less than the current in-sync revision
// is ignored; else, it causes a panic. If ordering entities from a single source,
// such as cdc.Store.Read(), ignorePastRevs should be false. But if merging
// multiple sources that can overlap (i.e. return the same event more than once,
// ignorePastRevs sholud be true to ignore past revisions. RevOrder returns
// correct results in both cases, the only difference is that ignorePastRevs = false
// is more strict.
func NewRevOrder(maxEntities uint, ignorePastRevs bool) *RevOrder {
	if maxEntities == 0 {
		maxEntities = DEFAULT_MAX_ENTITIES
	}
	r := &RevOrder{
		reorder:        map[string]*events{},
		lru:            lru.New(int(maxEntities)),
		ignorePastRevs: ignorePastRevs,
	}
	r.lru.OnEvicted = r.onEvictedCallback
	return r

}

// InOrder returns true if the given event is in order. If not (false), the caller
// should skip and ignore the event. If true, a non-nil slice of previous revisions
// is returned when they complete an in-order sequnce including the given event.
// In this case, the caller should only sync the slice of events.
func (r *RevOrder) InOrder(e CDCEvent) (bool, []CDCEvent) {
	// The algorithm is:
	//	 Let q = last event
	//	 Let r = current event
	//	 Let R = revision of event
	//	 Let {qR, rR+1} = ordered set of events (ascending revisions)
	//	 Let B = ordered set of events > qR (re.buf)
	//	 1) Assume qR=rR for first event
	//	  a) sync rR
	//	 2) Iff rR = qR+1 and B is empty:
	//	  a) set qR=rR
	//	  b) sync rR
	//	 2) If rR = qR (duplicate): ignore
	//	 3) If rR < qR
	//	  a) If ignorePastRevs = true: ignore
	//	  b) Else: panic
	//	 4) If B is empty:
	//	  a) B[0]=rR
	//	  b) ignore
	//	 5) Add rR to B, sort
	//	 6) Iff for all B, B[i] = qR+(1+i)
	//	  a) set qR=B[-1]
	//	  b) sync B
	//	  c) empty B

	// Get entity by ID from LRU cache
	v, seen := r.lru.Get(e.EntityId)

	// First time we see entity, we assume its rev to be a safe starting point
	if !seen {
		Debug("add id: %s", e.EntityId)
		r.lru.Add(e.EntityId, e.EntityRev)
		return true, nil // sync event
	}

	// We've seen this entity before. Compare previous rev (qR) to current (rR)
	qR := v.(int64)
	rR := e.EntityRev
	Debug("id %s qR %d rR %d", e.EntityId, qR, rR)

	// Are we reordering revs?
	re, reordering := r.reorder[e.EntityId]

	// The normal case: we're not reordering and this rev is exactly +1 of the
	// previous rev. This should be the 99.99999% case.
	if !reordering && rR == qR+1 {
		r.lru.Add(e.EntityId, e.EntityRev)
		return true, nil // sync event
	}

	// Normal case (in order) ^
	// ----------------------------------------------------------------------
	// Out of order (reordering) below...

	// Same rev as last time? Ignore it; don't sync (it should have already been
	// synced first time we saw it.)
	if rR == qR {
		Debug("duplicate (current), ignore")
		return false, nil // don't sync
	}

	// If current rev < previous rev, we've hit the most rare edge case and
	// there's no recovering here. User will need to re-sync from an earlier
	// time. This happens if the very first time we see the entity the rev=N+1
	// and seond time it's N. In other words: when the assumption noted above ^
	// turns out to be false.
	if rR < qR {
		if r.ignorePastRevs {
			Debug("duplicate (past), ignore")
			return false, nil // don't sync
		} else {
			msg := fmt.Sprintf("Entity %s is out of order because revision %d"+
				" received first and revision %d received second. The first"+
				" revision received is presumed to be in-order. This is a rare"+
				" edge case that could be corrected by replaying the CDC feed"+
				" from a time before entity %s revision %d. If this panic happens"+
				" again, file a bug report at https://github.com/square/etre",
				e.EntityId, qR, rR, e.EntityId, rR)
			panic(msg)
		}
	}

	// ----------------------------------------------------------------------
	// Out of order
	// ----------------------------------------------------------------------

	// First rev out of order? If yes, save and return early, wait for missing
	// (earlier) revs. When they arrive, reordering will be true and we'll
	// fall through to the next code block: Reordering.
	if !reordering {
		Debug("reorder id %s from %d", e.EntityId, qR)
		r.reorder[e.EntityId] = &events{
			qR:  qR,
			buf: []CDCEvent{e},
		}
		return false, nil // don't sync
	}

	// ----------------------------------------------------------------------
	// Reordering
	// ----------------------------------------------------------------------

	// This is 2nd and subsequent out-of-order- rev. E.g. 3, and 4, if prev rev = 2
	// and first out-of-order received was 5. Add to buffer and sort.
	Debug("buffer rev %d", e.EntityRev)
	re.buf = append(re.buf, e)
	sort.Sort(ByRev(re.buf))

	// We've got all the revs in sequence when we can walk from prev rev through
	// the buff +1 by +1. So if prev rev = 2, eventually we'll receive 3, 4, and 5,
	// so 2+1 = 3, 3+1 = 4, 4+1 = 5 == complete rev sequence.
	for i, b := range re.buf {
		if b.EntityRev != re.qR+1+int64(i) {
			Debug("reorder fails at %d: %d != %d", i, b.EntityRev, re.qR+1+int64(i))
			return false, nil // don't sync
		}
	}

	// Complete rev sequence. Return only the buf of previously out-of-order events,
	// _not_ the current event--the caller already has that one.
	buf := re.buf
	qR = buf[len(buf)-1].EntityRev
	r.lru.Add(e.EntityId, qR)
	Debug("reorder complete: id %s (%d, %d]", e.EntityId, re.qR, qR)

	delete(r.reorder, e.EntityId) // done reordering

	return true, buf // sync buf then event
}

// This func is called by r.lru when it evicts a key. We never call it directly.
func (r *RevOrder) onEvictedCallback(key lru.Key, value interface{}) {
	id := key.(string)
	Debug("evict id: %s", id)

	// This should never happen: the oldest key (entity ID) is still being
	// reordered. If maxEntities is tiny (< 10), then maybe this is just bad luck,
	// but more than likely we'll never receive the out-of-order revs, which
	// indicates a serious problem: we lost revs.
	if re, reordering := r.reorder[id]; reordering {
		msg1 := fmt.Sprintf("Entity %s evicted from the LRU cache because"+
			" it is the oldest entity but still waiting to receive revisions"+
			" > %d. Received revisions:", id, re.qR)
		for _, b := range re.buf {
			msg1 += fmt.Sprintf(" %d", b.EntityRev)
		}
		msg2 := fmt.Sprintf(". This is a rare edge case that indicates some revisions"+
			" were not received or extremely delayed. Increasing maxEntities and replaying"+
			" the CDC feed from a time before entity %s revision %d could fix this problem."+
			" Please file a bug at https://github.com/square/etre",
			id, re.qR)
		panic(msg1 + msg2)
	}
}
