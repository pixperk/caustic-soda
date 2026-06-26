package main

import (
	"sync"
	"sync/atomic"
)

// globalTS is the shared monotonic clock. every version is stamped from it, so
// version numbers are comparable across keys. snapshots compare against
// this same clock.
var globalTS int64

type Store struct {
	storageMu sync.Mutex
	storage   map[Key]Value
	txns      map[int64]*Txn // all transactions, indexed by their ID
	//map from the key to all the txn ids that have read it
	siReads map[Key][]int64
}

type Key string

type ValueVersion struct {
	value      string
	created_by int64 // the transaction that created this
	deleted    bool  // a delete is a tombstone version, not a map removal
}

type Value []*ValueVersion

func NewStore() *Store {
	return &Store{
		storage: make(map[Key]Value),
		txns:    make(map[int64]*Txn),
		siReads: make(map[Key][]int64),
	}
}

// !! this should not be used
// it only exists to demonstrate that the store is not safe for concurrent access without transactions
func (s *Store) Set(keyStr, value string) {
	s.storageMu.Lock()
	defer s.storageMu.Unlock()
	key := Key(keyStr)
	// append a new version stamped from the global clock, never overwrite.
	ts := atomic.AddInt64(&globalTS, 1)
	s.storage[key] = append(s.storage[key], &ValueVersion{value: value, created_by: ts})
}

// !! this should not be used
// it only exists to demonstrate that the store is not safe for concurrent access without transactions
func (s *Store) Get(keyStr string) (string, bool) {
	s.storageMu.Lock()
	defer s.storageMu.Unlock()
	key := Key(keyStr)
	value, ok := s.storage[key]

	//handle the case where there are no versions yet
	if !ok || len(value) == 0 {
		return "", false
	}
	// return the newest version, but a tombstone reads as "not found".
	newest := value[len(value)-1]
	if newest.deleted {
		return "", false
	}
	return newest.value, true
}

// !! this should not be used
// it only exists to demonstrate that the store is not safe for concurrent access without transactions
func (s *Store) Delete(keyStr string) {
	s.storageMu.Lock()
	defer s.storageMu.Unlock()
	key := Key(keyStr)
	// append a tombstone version stamped from the global clock, never overwrite.
	ts := atomic.AddInt64(&globalTS, 1)
	s.storage[key] = append(s.storage[key], &ValueVersion{deleted: true, created_by: ts})
}

// returns a txn to read from the store
// the txn will have a snapshot of the store at the time it was created
func (s *Store) Begin() *Txn {
	ts := atomic.AddInt64(&globalTS, 1)
	txn := &Txn{
		ID:             ts,
		Snapshot:       ts,
		store:          s,
		State:          Active,
		writes:         make(map[Key]string),
		inConflict:     false,
		outConflict:    false,
		inConflictFrom: []int64{},
		outConflictTo:  []int64{},
		markedForAbort: false,
	}

	s.storageMu.Lock()
	defer s.storageMu.Unlock()
	s.txns[txn.ID] = txn
	return txn
}

// two transactions are concurrent if their lifetimes overlap, that means
// txn1 started before txn2 committed AND txn2 started before txn1 committed.
// equivalently: they are NOT concurrent only if one finished before the other began.
func (s *Store) concurrent(id1, id2 int64) bool {
	txn1 := s.txns[id1]
	txn2 := s.txns[id2]
	if txn1 == nil || txn2 == nil {
		return false
	}

	// CommitTS == 0 means not committed yet, so that side is still live and
	// cannot have ended before the other began.
	txn1EndedBeforeTxn2 := txn1.CommitTS != 0 && txn1.CommitTS <= txn2.Snapshot
	txn2EndedBeforeTxn1 := txn2.CommitTS != 0 && txn2.CommitTS <= txn1.Snapshot

	return !(txn1EndedBeforeTxn2 || txn2EndedBeforeTxn1)
}

func (s *Store) checkAndMark(txn *Txn) {
	if txn.inConflict && txn.outConflict {
		s.markForAbort(txn)
	}
}

func (s *Store) markForAbort(pivot *Txn) {
	//prefer the pivot if it is still active
	if pivot.State == Active {
		pivot.markedForAbort = true
		return
	}

	//pivot already committed
	//we choose an adjacent transaction to abort
	for _, id := range append(pivot.inConflictFrom, pivot.outConflictTo...) {
		adj := s.txns[id]
		if adj != nil && adj.State == Active {
			adj.markedForAbort = true
			return
		}
	}

	// if every neighbor is also committed, the structure was already broken
	// by an earlier abort (or it's a false-positive remnant). nothing to do.
}

func main() {
	demoWriteSkewSafeRetry()
	demoGC()
}
