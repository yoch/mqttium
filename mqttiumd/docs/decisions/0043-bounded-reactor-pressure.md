# 0043 — Bounded reactor progress under transport pressure

Status: proposed.

An occupied output channel is a refusal for writes on that connection
generation. The outbox retains the refused effect with the same identity.
Later wire effects on that generation also remain in their original order.
A bounded scan may submit independent effects. A refusal for a shared
resource still stops the scan and preserves the relative order of the
remaining effects. The same preallocated outbox holds the deferred frames.
The adapter's actual transport terminal or write deadline retires the
connection and its outstanding facts through the existing close path.

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
