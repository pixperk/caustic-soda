# caustic-soda

a single-node, serializable, garbage-collected MVCC engine in go. built from scratch, one anomaly at a time.

we never overwrite a value, we append a new version. a read returns the version visible to the reader's snapshot. readers never block writers and writers never block readers, because they touch different versions. snapshot isolation falls out of this, and then we go past it to full serializability with cahill's serializable snapshot isolation (SSI).

full writeup (the why behind every line) : [pixperk.tech/blog/ssi-mvcc-engine-in-go](https://pixperk.tech/blog/ssi-mvcc-engine-in-go)

## what it does

each anomaly is killed by the minimum mechanism that kills it, and every kill is proven with a deterministic demo.

| anomaly | killed by | mechanism |
| --- | --- | --- |
| lost update | first-committer-wins | ww-conflict at commit |
| dirty read | visibility predicate | a version is visible only if its writer committed before our snapshot |
| non-repeatable read | snapshots | frozen begin-time read |
| phantom (KV) | snapshots | existence is versioned (tombstones) |
| write skew | SSI | dangerous-structure detection (two adjacent rw-edges) + safe retry |

## the isolation ladder

| level | what it stops | how |
| --- | --- | --- |
| read committed | dirty reads | visibility requires committed |
| repeatable read | also non-repeatable read | frozen snapshot |
| snapshot isolation | also lost update and phantoms | first-committer-wins |
| serializable (SSI) | also write skew | rw-edge detection + abort the pivot + safe retry |

the one line to remember : **SSI = snapshot isolation + watch for two adjacent rw-edges, and abort the pivot (choosing the victim that retries safely).**

## run it

```bash
go run .
```

every demo forces a precise interleaving with a tiny channel-based gate, so the breaks and fixes are reproducible, not lucky races.

## layout

- `main.go` : the store, versions, transactions, begin, the concurrency helper
- `txn.go` : get / set / commit / abort, the visibility predicate, conflict detection (both detection points), victim selection
- `gc.go` : version / txn-registry / siReads garbage collection (oldest-active cutoff)
- `harness.go` : the deterministic gate
- `demos.go` : one demo per anomaly, break and fix

## scope

this is the single-node story : in-memory store, goroutine "clients", the gate harness, SI, full SSI with safe retry. a complete, correct, serializable single-node MVCC engine.

part 2 (someday) : a network server over TCP, and distributed MVCC (a timestamp oracle / TrueTime-style clock, cross-node snapshots, distributed conflict detection). the ideas don't change, they just learn to survive without a shared mutex and a single clock.
