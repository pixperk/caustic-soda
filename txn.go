package main

type Txn struct {
	//every transaction has a id which is a ts from the global clock
	ID int64
	//snapshot is the timestamp this transaction started
	//used to determine which versions of keys are visible to this transaction
	Snapshot int64
	store    *Store
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
		if value[i].version <= txn.Snapshot {
			if value[i].deleted {
				return "", false
			}
			return value[i].value, true
		}
	}

	return "", false
}
