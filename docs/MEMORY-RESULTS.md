# Memory Correction Results

## Comparison basis

The before-state was captured on GitHub Actions benchmark run 22 from commit
`1a9fe49bd6b2a3018f26171668cb5b18f44a0066`. The after-state below was captured
locally with the same scenario counts and payload sizes after the admission,
delivery-budget, pagination, and lazy-hydration changes. GitHub Actions will
produce the authoritative after artifact for the pull request head.

Absolute RSS varies by runner. The important comparisons are logical work,
relative amplification, and whether configured budgets cap retained state.

## Main results

| Scenario | Before RSS delta | After RSS delta | Before traced peak | After traced peak |
|---|---:|---:|---:|---:|
| QoS queue, explicitly unlimited, 30,000 × 64 B | 19.69 MiB | 20.62 MiB | 10.41 MiB | 11.66 MiB |
| QoS queue, explicitly unlimited, 12,000 × 4 KiB | 53.89 MiB | 55.25 MiB | 50.09 MiB | 51.02 MiB |
| Iterator queue, explicitly below default budget, 6,000 × 4 KiB | 25.44 MiB | 27.50 MiB | 24.23 MiB | 25.16 MiB |
| Memory store, 12,000 × 4 KiB | 53.28 MiB | 52.75 MiB | 49.49 MiB | 49.49 MiB |
| SQLite hydration, 6,000 × 4 KiB | 60.50 MiB | 11.12 MiB | 50.62 MiB | 4.51 MiB |

The explicitly unlimited scenarios intentionally remain unbounded. They prove
that the benchmark still performs equivalent work and that the correction is a
configurable admission bound rather than a hidden workload reduction.

## Outbound byte budget

With a configured 8 MiB logical outbound budget and no count limit:

- attempted messages: 6,000;
- accepted messages: 2,042;
- rejected messages: 3,958;
- retained logical bytes: 8,388,536;
- configured limit: 8,388,608;
- loaded RSS delta: approximately 8.9 MiB.

Rejected messages leave no packet identifier, store record, receipt, effect, or
logical-byte reservation.

## Shared inbound delivery budget

With an approximately 8 MiB shared delivery budget:

- accepted messages: 2,042;
- the next message remained blocked;
- retained logical bytes: 8,388,536;
- iterator and callback delivery share one byte charge for the same `Message`;
- the charge is released only after every selected delivery path releases its
  reference.

The reader processes bounded packet batches and does not issue another transport
read while effect application is waiting for delivery capacity.

## SQLite paging and lazy loading

The SQLite result changed from:

```text
23.51 MiB logical session
60.50 MiB loaded RSS delta
50.62 MiB traced allocation peak
```

to approximately:

```text
23.51 MiB logical session
11.12 MiB loaded RSS delta
4.51 MiB traced allocation peak
```

The store now exposes ordered keyset-paginated summary pages that exclude payload
BLOBs. `ProtocolEngine` hydrates packet identifiers, states, logical-byte
accounting, and lightweight queue entries from those summaries. A full
`OutboundMessage` is fetched only when the flow-control window permits launch or
retransmission.

This makes startup memory proportional to metadata and page size rather than the
total persisted payload volume.

## Semantics covered by tests

The unit suite covers:

- finite, zero, and unlimited outbound limits;
- payload + UTF-8 topic + encoded-property accounting;
- atomic single and batch rejection;
- blocking and immediate-refusal async publication;
- caller cancellation before commit and after committed effect ownership;
- one dedicated effect flusher;
- stale connection-epoch effects;
- immediate Paho queue-size failure;
- shared iterator/callback delivery accounting;
- iterator and callback release paths;
- writer queue and decoder cleanup on forced close;
- QoS 2 encoded PUBLISH release after PUBREC;
- byte-bounded WebSocket frame batches;
- paginated SQLite ordering and mutation safety;
- payload-free SQLite hydration;
- bounded batch failure details, aggregate counts, and full failure sinks.

## Throughput-preserving fast paths

The first implementation routed every committed publication through a dedicated
async task. That made cancellation ownership simple, but inserted a scheduler
round trip into every `publish()` and reduced the QoS 1 benchmark by roughly
85%.

The final design keeps the same ownership model without making the background
flusher the nominal path:

- effects that fit the writer queue are transferred inline, without suspension;
- the background flusher owns only effects that encounter real backpressure;
- the reader yields after a full packet batch, not after every individual ACK;
- `nowait=True` preflights writer capacity before mutating protocol state;
- the retained logical size lives on the outbound record instead of in a
  parallel `mid -> size` dictionary.

In alternating same-process-environment measurements using the fake broker and a
QoS 1 window of 100, the final median was approximately 32.35k messages/s versus
32.03k messages/s before the memory overhaul. The GitHub benchmark likewise
kept mqttium's advantage over Paho: the mqttium/Paho ratio changed from 2.80x in
the baseline run to 2.86x after the fast-path correction.

## Count-derived small-message delivery pool

Exact byte accounting is still used for large inbound messages. For common
small telemetry messages, doing a dictionary reservation and release for every
callback or iterator delivery was measurable overhead.

When the configured byte budget is sufficiently larger than the maximum packet
size, one eighth of that budget is therefore reserved statically for small
messages. The maximum eligible message size is derived conservatively from the
configured iterator and callback queue counts. Consequently, even if every
available queue slot contains the largest eligible small message, that pool
cannot exceed its reserved slice.

The remaining seven eighths use exact logical-byte accounting and backpressure.
The partition is disabled automatically for small/custom budgets whenever it
would reduce the capacity of a single packet that could otherwise fit.

This restores the zero-delay delivery benchmarks close to their pre-overhaul
level while preserving a hard total bound. Local measurements after the change
were approximately 263k callbacks/s and 330k iterator deliveries/s, compared
with 204k and 272k respectively in the original GitHub baseline artifact.
`pending_delivery_bytes` reports the exact-accounted large-message pool;
`delivery_small_budget_bytes` and `delivery_small_message_limit` expose the
static fast-path reservation.
