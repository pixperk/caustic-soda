package main

import (
	"fmt"
	"strconv"
)

// demoLostUpdate forces the classic lost-update anomaly on the naive store.
//
// two clients each do "read balance, add 50, write it back". each client's code
// looks correct on its own. but with the wrong interleaving (both read 100 before
// either writes), one write clobbers the other and we land on 150 instead of 200.
//
// a's read is done up front in main, modeling "a already read 100 and is about to
// write it back stale". the gate then orders a's write to land after b's, so a
// clobbers b every run. deterministic, not a coin flip.
func demoLostUpdate() {
	store := NewStore()
	store.Set("balance", "100") // start at 100

	gate := NewGate()           // holds a's write until b has written
	done := make(chan struct{}) // closed once a's write has landed

	// client a read 100 earlier and is holding that stale value.
	v, _ := store.Get("balance")
	a, _ := strconv.Atoi(v) // a holds 100

	// a's pending write, fired only after b writes.
	go func() {
		gate.Wait()                              // wait until b has written 150
		store.Set("balance", strconv.Itoa(a+50)) // a writes its stale 100+50, clobbering b
		close(done)
	}()

	// client b reads 100 too and writes its result.
	w, _ := store.Get("balance")
	b, _ := strconv.Atoi(w)                  // b holds 100
	store.Set("balance", strconv.Itoa(b+50)) // b writes 150

	gate.Release() // let a's stale write land on top
	<-done         // wait until it has

	// expected: 150. b's +50 was lost.
	final, _ := store.Get("balance")
	fmt.Printf("lost update: final balance = %s (correct would be 200; one +50 was lost)\n", final)
}

// a non repeatable
// read is a read that returns different values when repeated in the same transaction.
// this demo shows how a naive store can
// return different values for the same key in the same transaction, violating repeatable reads.
func demoNonRepeatableRread() {
	store := NewStore()
	store.Set("k", "v1")

	gate := NewGate()
	done := make(chan struct{})

	//first read
	first, _ := store.Get("k")

	// a's pending read, fired only after b writes.
	go func() {
		gate.Wait() // wait until b has written v2
		second, _ := store.Get("k")
		fmt.Printf("non repeatable read: first read = %s, second read = %s (correct would be v1; the value changed)\n", first, second)
		close(done)
	}()

	//b writes v2
	store.Set("k", "v2")

	gate.Release() // let a's second read happen
	<-done         // wait until it has

}

// fixed by using transactions and snapshots,
// a repeatable read is a read that returns the
//
//	same value when repeated in the same transaction.
func demoRepeatableRead() {
	store := NewStore()

	// committed baseline so there is a v1 to read.
	g := store.Begin()
	g.Set("k", "v1")
	_ = g.Commit()

	gate := NewGate()
	done := make(chan struct{})

	//start a transaction (snapshot is frozen here, after g committed)
	txn := store.Begin()
	//first read
	first, _ := txn.Get("k")

	// a's pending second read, fired only after b writes.
	go func() {
		gate.Wait() // wait until b has committed v2
		second, _ := txn.Get("k")
		fmt.Printf("repeatable read: first read = %s, second read = %s (correct would be v1; the value did not change)\n", first, second)
		close(done)
	}()

	//b writes v2 and commits, with a commitTS after a's snapshot
	b := store.Begin()
	b.Set("k", "v2")
	_ = b.Commit()

	gate.Release() // let a's second read happen
	<-done         // wait until it has
}

// a dirty read is reading a value some other transaction wrote but never
// committed. no concurrency needed to show it: a writes, b reads it, a aborts.
// with the naive predicate (created_by <= snapshot, no committed check) b sees
// a's "dirty" value, which never existed in any committed state.

// now fixed
func demoDirtyRead() {
	s := NewStore()

	// genesis: a committed baseline so there is an "old" value to fall back to.
	g := s.Begin()
	g.Set("k", "old")
	_ = g.Commit()

	// a writes a new value but does NOT commit.
	a := s.Begin()
	a.Set("k", "dirty")

	// b reads while a is still uncommitted.
	b := s.Begin()
	read, _ := b.Get("k")

	// a rolls back. the value b read never committed.
	a.Abort()

	fmt.Printf("dirty read: b read = %s \n", read)
}

// demoLostUpdateTxn shows first-committer-wins killing the lost update. a and b
// both read 100. a commits 150. b's commit hits a write-write conflict (a committed
// to the same key after b's snapshot) and is rejected, so b cannot clobber a. b then
// retries on fresh data (reads 150) and reaches the correct 200.
func demoLostUpdateTxn() {
	store := NewStore()

	// committed baseline: balance = 100.
	g := store.Begin()
	g.Set("balance", "100")
	_ = g.Commit()

	// a and b both begin and read 100 from their own snapshots.
	a := store.Begin()
	av, _ := a.Get("balance")
	abal, _ := strconv.Atoi(av) // a reads 100

	b := store.Begin()
	bv, _ := b.Get("balance")
	bbal, _ := strconv.Atoi(bv) // b reads 100

	// a writes its +50 and commits first, it wins.
	a.Set("balance", strconv.Itoa(abal+50))
	_ = a.Commit() // writes 150

	// b writes its +50 and tries to commit, first-committer-wins rejects it.
	b.Set("balance", strconv.Itoa(bbal+50))
	if err := b.Commit(); err != nil {
		fmt.Printf("lost update (txn): b rejected on commit: %v (no silent clobber)\n", err)

		// b retries on fresh data: re-read, recompute, commit.
		b2 := store.Begin()
		v, _ := b2.Get("balance")
		bal, _ := strconv.Atoi(v) // now reads 150
		b2.Set("balance", strconv.Itoa(bal+50))
		_ = b2.Commit() // writes 200
	}

	// read the final committed value.
	r := store.Begin()
	final, _ := r.Get("balance")
	fmt.Printf("lost update (txn): final balance = %s (correct 200; first-committer-wins + retry)\n", final)
}

func demoWriteSkew() {
	s := NewStore()
	g := s.Begin()
	g.Set("alice", "on_call")
	g.Set("bob", "on_call")
	_ = g.Commit()

	// alice and bob are both on call.
	// they both want to go off call, but at least one must stay on call.

	a := s.Begin()
	av, _ := a.Get("alice")
	bv, _ := a.Get("bob")

	if av == "on_call" && bv == "on_call" {
		a.Set("alice", "off_call")
	}

	b := s.Begin()
	av2, _ := b.Get("alice")
	bv2, _ := b.Get("bob")

	if av2 == "on_call" && bv2 == "on_call" {
		b.Set("bob", "off_call")
	}

	if err := a.Commit(); err != nil {
		fmt.Printf("write skew: a failed to commit: %v\n", err)
	}
	if err := b.Commit(); err != nil {
		fmt.Printf("write skew: b failed to commit: %v\n", err)
	}

	// now both alice and bob are off call, violating the constraint.
	r := s.Begin()
	finalAlice, _ := r.Get("alice")
	finalBob, _ := r.Get("bob")
	fmt.Printf("write skew: final state = alice: %s, bob: %s\n", finalAlice, finalBob)
}

// demoWriteSkewReadAfterWrite shows a write skew our detection STILL misses,
// because the edge b->rw a is born at b's READ (after a already committed), and
// we only run detection point 2 (on write). b begins concurrent with a but reads
// after a commits, so a's write to alice was never seen as a conflict.
func demoWriteSkewReadAfterWrite() {
	s := NewStore()
	g := s.Begin()
	g.Set("alice", "on_call")
	g.Set("bob", "on_call")
	_ = g.Commit()

	// both begin and are concurrent (b's snapshot is frozen before a commits).
	a := s.Begin()
	b := s.Begin()

	// a reads both, writes alice, commits first.
	av, _ := a.Get("alice")
	bv, _ := a.Get("bob")
	if av == "on_call" && bv == "on_call" {
		a.Set("alice", "off_call")
	}
	if err := a.Commit(); err != nil {
		fmt.Printf("write skew (raw): a failed to commit: %v\n", err)
	}

	// b reads AFTER a committed, but sees old on_call via its older snapshot.
	av2, _ := b.Get("alice")
	bv2, _ := b.Get("bob")
	if av2 == "on_call" && bv2 == "on_call" {
		b.Set("bob", "off_call")
	}
	if err := b.Commit(); err != nil {
		fmt.Printf("write skew (raw): b failed to commit: %v\n", err)
	}

	r := s.Begin()
	finalAlice, _ := r.Get("alice")
	finalBob, _ := r.Get("bob")
	fmt.Printf("write skew (read-after-write): final state = alice: %s, bob: %s\n", finalAlice, finalBob)
}

// demoWriteSkewSafeRetry shows the payoff of safe retry: the loser doesn't just
// abort, it retries until it makes progress. a wins and commits first. b's first
// attempt is concurrent with a, hits the SSI conflict, and aborts. b then retries
// with a fresh transaction that begins AFTER a committed, so it is no longer
// concurrent with a, sees alice already off_call, correctly leaves bob on_call,
// and commits. the retry cannot reproduce the conflict, so it converges.
func demoWriteSkewSafeRetry() {
	s := NewStore()
	g := s.Begin()
	g.Set("alice", "on_call")
	g.Set("bob", "on_call")
	_ = g.Commit()

	// a and b both begin concurrently.
	a := s.Begin()
	b := s.Begin()

	// a takes alice off call and commits first, it wins.
	av, _ := a.Get("alice")
	bv, _ := a.Get("bob")
	if av == "on_call" && bv == "on_call" {
		a.Set("alice", "off_call")
	}
	_ = a.Commit()

	// b retries until it commits.
	attempt := 1
	for {
		alice, _ := b.Get("alice")
		bob, _ := b.Get("bob")
		if alice == "on_call" && bob == "on_call" {
			b.Set("bob", "off_call")
		}
		if err := b.Commit(); err == nil {
			break // success, b made progress
		} else {
			fmt.Printf("write skew (safe retry): attempt %d aborted: %v\n", attempt, err)
		}
		attempt++
		b = s.Begin() // fresh transaction, now begins after a committed
	}

	r := s.Begin()
	finalAlice, _ := r.Get("alice")
	finalBob, _ := r.Get("bob")
	fmt.Printf("write skew (safe retry): b committed on attempt %d, final state = alice: %s, bob: %s\n", attempt, finalAlice, finalBob)
}

// demoGC shows GC reclaiming a shadowed version while refusing to collect the
// version a long-running reader still needs.
//
// we commit v0 then v1, then start a long reader whose snapshot sits on v1.
// more writers commit v2 and v3 (invisible to the reader). GC then drops v0
// (shadowed by v1 below the cutoff) but keeps v1 (the reader's floor), so the
// long reader can still read its consistent value after GC.
func demoGC() {
	s := NewStore()

	// two committed versions before the long reader begins.
	g0 := s.Begin()
	g0.Set("x", "v0")
	_ = g0.Commit()

	g1 := s.Begin()
	g1.Set("x", "v1")
	_ = g1.Commit()

	// the long reader: its snapshot is frozen here, so it will always see v1.
	longReader := s.Begin()
	beforeRead, _ := longReader.Get("x")

	// newer writers commit v2 and v3. these commit after the long reader began,
	// so they are invisible to it, but they pile up in the chain.
	g2 := s.Begin()
	g2.Set("x", "v2")
	_ = g2.Commit()

	g3 := s.Begin()
	g3.Set("x", "v3")
	_ = g3.Commit()

	versionsBefore := len(s.storage["x"])
	txnsBefore := len(s.txns)

	s.GC()

	versionsAfter := len(s.storage["x"])
	txnsAfter := len(s.txns)

	// the long reader must STILL read v1, GC kept its floor version.
	afterRead, _ := longReader.Get("x")

	fmt.Printf("gc: long reader read %s before and %s after gc (must be v1 both)\n", beforeRead, afterRead)
	fmt.Printf("gc: versions of x %d -> %d, txns in registry %d -> %d (v0 and its txn reclaimed)\n",
		versionsBefore, versionsAfter, txnsBefore, txnsAfter)
}
