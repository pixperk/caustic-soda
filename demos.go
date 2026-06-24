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

	gate := NewGate()         // holds a's write until b has written
	done := make(chan struct{}) // closed once a's write has landed

	// client a read 100 earlier and is holding that stale value.
	v, _ := store.Get("balance")
	a, _ := strconv.Atoi(v) // a holds 100

	// a's pending write, fired only after b writes.
	go func() {
		gate.Wait()                            // wait until b has written 150
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
