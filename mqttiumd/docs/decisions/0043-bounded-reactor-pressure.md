# ADR 0043 — Bounded reactor progress under transport pressure

Status: **Accepted with amendments** (21 November 2026, decided under the
repository owner's delegation on the advice of the independent audit of
`8e4303d`).

Amendment of acceptance (21 November 2026).
Stopping socket input on a blocked generation can also delay unread
acknowledgements. Reserve writer and terminal capacity. Require an
executable progress test that covers ingress, effect admission and output
before claiming end-to-end control progress. Record 0048 states the
capacities of the runtime and their derivation; the sixteen-connection
numbers below describe the bounded qualification profile.

Note (21 November 2026): the budgets this record states for sixteen connections are
the bounded profile of `runtime::Limits`. The connection limit is an option
since record 0046 (proposed), and the queues sized for sixteen connections
grow with it.

An occupied output channel is a refusal for writes on that connection
generation. The outbox retains the refused effect with the same identity.
Later wire effects on that generation also remain in their original order.
A bounded scan may submit independent effects. A refusal for a shared
resource still stops the scan and preserves the relative order of the
remaining effects. The same preallocated outbox holds the deferred frames.
The adapter's actual transport terminal or write deadline retires the
connection and its outstanding facts through the existing close path.
The reactor stops dequeuing further packets on a backpressured generation
and still collects write receipts and transport terminals. A previously
held write keeps an observable terminal even for PINGRESP.

Reserve the room a packet can use before removing it from the socket
channel. Polling starts at a rotating connection. A CONNECT reserves
room for its correlated Open while the Will check is outstanding.
The ordinary input FIFO has 128 slots, with 32 reserved for replies to
accepted writer work and four for ordinary deadlines. The order of packet
and writer observations in that FIFO is preserved. Business writer and
page requests reserve their reply slot at submission. Receipt collection
and lifecycle controls keep their existing dedicated capacity. The
declared connection, effect, TCP and TLS channel and store quotas stand.

An effect entering or leaving a socket queue is a scheduler observation.
It does not establish socket write progress or a durable commit. The
existing correlation, application and receive-window boundaries remain
authoritative. This change introduces a transient submission refusal and
does not alter the store, wire, event or trace encoding.
