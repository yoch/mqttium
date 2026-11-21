# Independent normal QoS 1 accounting review

The fresh reviewer inspected source at `49bbaaa4d1d080f458980e3498e90638e317668d`.
The performed scope was ordinary QoS 1 admission, SQLite atomicity, Claim,
Finish and live receipt retirement. It found no accounting discrepancy in that
scope and executed no tests. It did not independently review the whole durable
store or the whole PR. The attempted broader delegated review was blocked by
automated tool review, so its coverage is explicitly limited here.

SQLite admission stages the publication, the shared payload and all recipient
references inside the command transaction. QoS 1 creates no durable incoming
exchange. A claim reserves one exchange slot. Finish compares its DeliveryId,
claim operation, destination revision and packet identifier before decrementing
one reference. Payload ownership remains until its last reference disappears.
The receipt collector changes receipt rows and compact closure state while live
claims retain their own identity, independently of historical receipts.

The inspected paths are `mqttiumd-store-sqlite/src/lib.rs` at lines908–917,
959–1078,1131–1186,1871 and2794–2848, `pubsub.rs` at173–253 and `receipts.rs`
at212–337. Core paths are `owner.rs` at2157–2187 and `owner/pubsub.rs`
at1333–1376,2243–2274 and2862–2887. All paths are relative to
`mqttiumd/crates/` at the named candidate.

The reviewer inspected the existing SQLite pubsub and receipts tests and
`tests/system/qos1_reception.py` and `pubsub_reception.py`. The independent
review establishes inspected predicates and coverage only. The load and restore
reports provide the separately identified implementation observations.
