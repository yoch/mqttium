# Reducing a lease after PUBREC

The Kani harness calls the product's `LeaseTable::lighten` on a two-entry
table. One unit of bytes and one slot belong to a separate session. A
durable row restores the target lease at a symbolic generation. Old and
new costs range over both integer dimensions, subject to the old costs
fitting alongside the other lease. The checked outcomes include reducing
to the one-slot, zero-byte obligation retained after PUBREC.

For a decrease, the proof checks the exact total account charge, unchanged
number of held slots, the other lease's identity, the new generation and
invalidation of the old key. For growth, equality, an exhausted generation
or a key outside the live entry, it checks the result and preserved state.
Eight covers require a witness for each named branch. The unwind limit is
five for this two-entry construction. This harness covers valid tables
constructed through the public API. It establishes no property about a
table of arbitrary size or asynchronous recovery.

Run both proofs with the pinned toolchain from the broker workspace.

~~~sh
proofs/credits/run.sh
python3 proofs/lease-lighten/run.py
~~~

The second command requires a verified harness and every listed witness.
It then copies the source to a temporary directory, changes the restore to
charge the original cost after issuing the new key, and requires the total
credit assertion to fail. Compilation errors, timeouts, and failures at
other assertions fail this control. Logs are written to
`proofs/target/proof-lease-lighten` or the directory in `LIGHTEN_PROOF_OUT`.
The pilot for `release_if_current` retains its separate scope and digest.
