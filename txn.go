package main

import "sync/atomic"

// txn state enum
type TxnState int

const (
	Active TxnState = iota
	Committed
	Aborted
)

type Txn struct {
	//every transaction has a id which is a ts from the global clock
	ID int64
	//snapshot is the timestamp this transaction started
	//used to determine which versions of keys are visible to this transaction
	Snapshot int64
	store    *Store

	State TxnState
	//when did this transaction commit, if it did
	//if it didn't commit, this will be 0
	CommitTS int64
}

func (txn *Txn) Get(k string) (string, bool) {
	txn.store.storageMu.Lock()
	defer txn.store.storageMu.Unlock()
	key := Key(k)
	value, ok := txn.store.storage[key]
	if !ok || len(value) == 0 {
		return "", false
	}

	for i := len(value) - 1; i >= 0; i-- {
		if value[i].created_by <= txn.Snapshot {
			if value[i].deleted {
				return "", false
			}
			return value[i].value, true
		}
	}

	return "", false
}

func (txn *Txn) Set(k, v string) {
	txn.store.storageMu.Lock()
	defer txn.store.storageMu.Unlock()
	key := Key(k)
	//version is renamed to created_by to reflect that it is the transaction that created this version, not a global timestamp
	txn.store.storage[key] = append(txn.store.storage[key], &ValueVersion{value: v, created_by: txn.ID})
}

func (txn *Txn) Commit() {
	txn.store.storageMu.Lock()
	defer txn.store.storageMu.Unlock()
	if txn.State != Active {
		return
	}
	txn.State = Committed
	txn.CommitTS = atomic.AddInt64(&globalTS, 1)
}

func (txn *Txn) Abort() {
	txn.store.storageMu.Lock()
	defer txn.store.storageMu.Unlock()
	if txn.State != Active {
		return
	}
	txn.State = Aborted
}
