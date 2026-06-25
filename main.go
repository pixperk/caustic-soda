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
		ID:       ts,
		Snapshot: ts,
		store:    s,
		State:    Active,
		writes:   make(map[Key]string),
	}

	s.storageMu.Lock()
	defer s.storageMu.Unlock()
	s.txns[txn.ID] = txn
	return txn
}

func main() {
	demoLostUpdate()
	demoNonRepeatableRread()
	demoRepeatableRead()
	demoDirtyRead()
	demoLostUpdateTxn()
}
