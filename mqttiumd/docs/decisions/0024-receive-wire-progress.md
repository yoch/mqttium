# 0024 — Receive credits follow bounded transport progress

Status: **Accepted** (21 November 2026, by the repository owner).

Receive Maximum accounting must not confuse a delayed internal callback with
an outstanding MQTT exchange. A peer can receive PUBACK and send a new
identifier before the reactor consumes Written. Seven held QoS 2 exchanges
plus sequential acknowledged QoS 1 exchanges exposed a false 0x93 refusal.

Each connection has a separate bounded 13-record WireProgress lane. The socket
writer publishes successful completion there before its ordinary Written
receipt. The reactor reconciles exact effect, generation, PUBLISH occurrence
and terminal QoS before charging another input. An ack.receipt test hold affects
the core receipt, not this independent transport fact. Written never releases
the same credit twice; stale/duplicate receipt guards remain.

The writer can be preempted after bytes become visible but before publishing
progress. At a full window with a matching accepted terminal ACK, retain at
most one decoded PUBLISH per peer, for at most 500 milliseconds, and retry
after actual progress. Submission alone never returns credit or admits the
deferred packet. Closure/deadline stays explicit and bounded. A full window
without a terminal ACK candidate refuses a genuine ninth exchange. Neither
the advertised maximum eight nor its occurrence/QoS guards are increased.

Deterministic real-reactor tests hold Written after client receipt and pause
the private test-only write/progress gap. A normal bounded TCP trial keeps
seven QoS 2 exchanges open while completing 65 QoS 1 publications with new
identifiers. Existing same-ID and cross-QoS delayed-receipt cases and the
stale_receipt_released mutation remain obligations. The private gap seam is
absent from both normal and product-hook executables.
