package main

import "sync/atomic"

// GC reclaims everything no live transaction can still see or need: shadowed
// versions, dead txn registry entries, and stale SIREAD records.
//
// the cutoff is the OLDEST ACTIVE SNAPSHOT, never "now". collecting anything a
// live reader could still read corrupts that reader's snapshot, that is the bug
// this whole function is shaped to avoid.
func (s *Store) GC() {
	s.storageMu.Lock()
	defer s.storageMu.Unlock()

	// if no txn is active, everything below the current clock is collectible.
	oldestActive := atomic.LoadInt64(&globalTS)
	for _, txn := range s.txns {
		if txn.State == Active && txn.Snapshot < oldestActive {
			oldestActive = txn.Snapshot
		}
	}

	// order matters: versions first (txn lookup must still work), then the txn
	// registry (coupled to surviving versions), then the read records.
	survivors := s.versionGC(oldestActive)
	s.txnGC(oldestActive, survivors)
	s.siReadsGC(oldestActive)
}

// commitTSOf resolves a version's commit time via its writer. caller holds the lock.
func (s *Store) commitTSOf(v *ValueVersion) int64 {
	writer, ok := s.txns[v.created_by]
	if !ok || writer == nil {
		// unknown writer (e.g. a raw store.Set version): treat as ancient so it
		// is never wrongly kept above the cutoff.
		return -1
	}
	return writer.CommitTS
}

// versionGC trims each key's chain. it keeps every version above the cutoff
// (a live reader may need it) plus exactly ONE version at/below the cutoff (the
// "floor" the oldest reader sits on). it returns the set of writer ids that a
// kept version still references, so txnGC knows which txns must survive.
//
// caller holds storageMu.
func (s *Store) versionGC(oldestActive int64) map[int64]bool {
	survivors := make(map[int64]bool)
	for key, chain := range s.storage {
		kept := make(Value, 0, len(chain))
		keptFloor := false
		// walk newest -> oldest; the chain is in ascending commit order.
		for i := len(chain) - 1; i >= 0; i-- {
			v := chain[i]
			cts := s.commitTSOf(v)
			if cts > oldestActive {
				// a reader newer than the cutoff may still see this version.
				kept = append(Value{v}, kept...)
				survivors[v.created_by] = true
			} else if !keptFloor {
				// THE floor: newest version at/below the cutoff. keep exactly one,
				// dropping it would leave the oldest reader with nothing to read.
				kept = append(Value{v}, kept...)
				keptFloor = true
				survivors[v.created_by] = true
			}
			// else: at/below the cutoff AND shadowed by the floor -> drop.
		}
		s.storage[key] = kept
	}
	return survivors
}

// txnGC drops committed/aborted txns that nothing needs anymore. caller holds storageMu.
func (s *Store) txnGC(oldestActive int64, survivors map[int64]bool) {
	for id, txn := range s.txns {
		if txn.State == Active {
			continue // still live
		}
		if survivors[id] {
			continue // a kept version resolves visibility through it
		}
		if txn.CommitTS > oldestActive {
			continue // still concurrent with the oldest reader, could be flagged
		}
		delete(s.txns, id)
	}
}

// siReadsGC drops read records whose txn can no longer conflict with a future
// writer (it ended before the cutoff, or is already gone). caller holds storageMu.
func (s *Store) siReadsGC(oldestActive int64) {
	for key, ids := range s.siReads {
		kept := make([]int64, 0, len(ids))
		for _, id := range ids {
			txn := s.txns[id]
			if txn != nil && (txn.State == Active || txn.CommitTS > oldestActive) {
				kept = append(kept, id)
			}
		}
		if len(kept) == 0 {
			delete(s.siReads, key)
		} else {
			s.siReads[key] = kept
		}
	}
}
