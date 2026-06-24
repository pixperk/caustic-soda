package main

// gate is a deterministic pause button for one goroutine, controlled by the test.
//
// it turns "run it 100 times and hope the race happens" into a forced, reproducible
// interleaving. a transaction calls wait() to freeze itself at a precise point; the
// test calls release() to thaw it. the channel carries no data, only the signal
// matters: a receive blocks, and close() wakes every blocked receive at once.
//
// usage in a demo (forcing "a reads before b writes"):
//
//	gate := NewGate()
//	go func() {
//	    v, _ := store.Get("balance") // a reads 100
//	    gate.Wait()                  // a freezes here
//	    store.Set("balance", v+50)   // runs only after release
//	}()
//	// ... b does its full read-modify-write here, to completion ...
//	gate.Release()                   // a thaws and writes its stale 100+50
//
// move the wait() call to change exactly where the pause lands. one release per
// gate only, close()-ing a channel twice panics.
type Gate struct {
	ch chan struct{}
}

func NewGate() *Gate {
	return &Gate{
		ch: make(chan struct{}),
	}
}

// wait blocks the calling goroutine until the test calls release.
func (g *Gate) Wait() {
	<-g.ch
}

// release unblocks every goroutine waiting on the gate. call it at most once.
func (g *Gate) Release() {
	close(g.ch)
}
