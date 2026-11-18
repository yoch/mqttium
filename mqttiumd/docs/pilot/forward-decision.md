# Q9 deferred-delivery qualification

The original [direct-publication decision](decision.md) records the 128-reference
limit and the shared effect of an offline dashboard. Its measured evidence
remains attached to the earlier package revision.

For the deferred profile, the source records each event in its own SQLite
outbox before attempting MQTT. The producer's confirmed sequence advances after
a correlated positive PUBACK. Each dashboard separately records the event and
its envelope in a durable ledger before returning its own PUBACK. A negative
quota PUBACK only ends the current attempt. An unknown attempt retains the same
event ID on a fresh MQTT session. An identical repeat is acknowledged after the
dashboard has matched its existing committed bytes. A contradiction stops the
affected dashboard and preserves the original application row.

The Q9 decision remains open for deferred delivery while the full reception
runs. The short reception verifies all 500 events at each of the three
boundaries. GO with restrictions requires the full accepted sequence at both
dashboards, a measured positive catch-up rate under continuing generation and
explicit operator acceptance of the measured delay and storage envelope.

Independent real-time supervision remains NO-GO. A full budget for an offline
destination holds admissions whose fanout also targets the live dashboard.
Separate live and historical flows would need distinct routing and failure
policies. Per-recipient admission would require a new contract for durable
responsibility, publisher acknowledgements and missing recipients.
