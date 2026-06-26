package main

import (
	"fmt"
	"sync/atomic"
)

const (
	ErrorTxnNotActive      = "transaction is not active"
	ErrWriteWriteConflict  = "write-write conflict"
	ErrAlreadyCommitted    = "transaction is already committed"
	ErrorTxnAlreadyAborted = "transaction is already aborted"
	ErrSSIConflict         = "transaction aborted due to SSI conflict : dangerous structure (txnA -rw-> txnPivot -rw-> txnB) detected"
)

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
	//map for buffered writes, so we don't write to the store until commit
	writes map[Key]string
	//bools for checking txn A -rw-> txnPivot -rw-> txnB conflicts
	inConflict  bool
	outConflict bool
}

func (txn *Txn) Get(k string) (string, bool) {
	key := Key(k)

	//read own buffer first, if we have a write for this key, return it
	if v, ok := txn.writes[key]; ok {
		return v, true
	}

	//otherwise, find the newest version that is visible to this transaction

	txn.store.storageMu.Lock()
	defer txn.store.storageMu.Unlock()

	//read attemp : gotta add to siReads map
	//notice how we don't append OUR buffer writes to the store until commit, so we don't have to worry about that here
	txn.store.siReads[key] = append(txn.store.siReads[key], txn.ID)

	value, ok := txn.store.storage[key]
	if !ok || len(value) == 0 {
		return "", false
	}

	for i := len(value) - 1; i >= 0; i-- {
		if txn.visible(value[i]) {
			if value[i].deleted {
				return "", false
			}
			return value[i].value, true
		}
	}

	return "", false
}

func (txn *Txn) Set(k, v string) {
	key := Key(k)
	//we just record the write in the txns buffer,
	// we don't actually write to the store until commit
	txn.writes[key] = v
}

func (txn *Txn) Commit() error {
	txn.store.storageMu.Lock()
	defer txn.store.storageMu.Unlock()
	if txn.State != Active {
		return fmt.Errorf(ErrorTxnNotActive)
	}

	if err := txn.errOnWriteWriteConflict(); err != nil {
		return err
	}

	//set the conflict flags for this transaction and any other transactions that have read the keys we wrote
	txn.setConflictFlags()

	if err := txn.errOnSSIConflict(); err != nil {
		return err
	}
	//no conflict found, write our buffered writes to the store
	for k, v := range txn.writes {
		txn.store.storage[k] = append(txn.store.storage[k], &ValueVersion{value: v, created_by: txn.ID})
	}
	txn.CommitTS = atomic.AddInt64(&globalTS, 1)
	txn.State = Committed
	return nil
}

func (txn *Txn) Abort() {
	txn.store.storageMu.Lock()
	defer txn.store.storageMu.Unlock()
	if txn.State != Active {
		return
	}
	txn.State = Aborted
}

func (txn *Txn) visible(v *ValueVersion) bool {
	//if i am the transaction that created this version, i can see it
	if v.created_by == txn.ID {
		return true
	}

	writer, ok := txn.store.txns[v.created_by]
	if !ok {
		//this should never happen, but if it does, we will treat it as not visible
		return false
	}

	//if the writer is not committed, this version is not visible
	if writer == nil || writer.State != Committed {
		return false
	}

	//only if the writer committed before my snapshot can i see this version
	return writer.CommitTS <= txn.Snapshot
}

// conflict check
// for each key we wrote,
// check if there is a newer version that was committed after our snapshot
func (txn *Txn) errOnWriteWriteConflict() error {
	for k := range txn.writes {
		//scan the versions for this key
		versions, ok := txn.store.storage[k]
		if !ok {
			continue
		}
		//check each version to see
		// if it was created by a committed transaction after our snapshot
		for i := len(versions) - 1; i >= 0; i-- {
			v := versions[i]
			if v.created_by == txn.ID {
				continue
			}
			writer, ok := txn.store.txns[v.created_by]
			if !ok {
				continue
			}
			//if yes, this is a write-write conflict, abort the transaction
			if writer.State == Committed && writer.CommitTS > txn.Snapshot {
				txn.State = Aborted
				return fmt.Errorf(ErrWriteWriteConflict)
			}

		}
	}

	return nil
}

func (txn *Txn) setConflictFlags() {
	for k := range txn.writes {
		for _, readerID := range txn.store.siReads[k] {
			if readerID == txn.ID {
				continue
			}
			if txn.store.concurrent(readerID, txn.ID) {
				reader := txn.store.txns[readerID]
				reader.outConflict = true
				txn.inConflict = true
			}

		}
	}
}

func (txn *Txn) errOnSSIConflict() error {
	if txn.store.checkPivot(txn) {
		txn.State = Aborted
		return fmt.Errorf(ErrSSIConflict)
	}

	return nil
}
