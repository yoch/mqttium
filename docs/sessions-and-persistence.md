# Sessions and persistence

Reliable restart recovery requires cooperation between three owners:

1. the broker retains the MQTT session;
2. MQTTium retains unfinished protocol state;
3. the application makes its own business processing idempotent and durable.

These layers solve different problems. A SQLite store cannot make a broker keep
a clean session, and an MQTT session is not an application database.

## What an MQTT session contains

A persistent broker session may retain subscriptions, queued messages and
unfinished QoS exchanges according to the protocol version and broker policy.
On reconnect, CONNACK tells MQTTium whether that previous session is present.

When `session_present` is true, MQTTium replays persisted outbound PUBLISH or
PUBREL state and restores inbound QoS 2 deduplication state. When it is false,
the broker can no longer complete those old exchanges; MQTTium fails pending
receipts with `SessionDiscardedError` and releases the stale local state.

MQTTium does not periodically retransmit QoS messages on a healthy connection.
Protocol replay happens after reconnect, with DUP set where MQTT requires it.

## MQTT 3.1.1 sessions

For MQTT 3.1.1, request a persistent session with `clean_start=False`:

```python
from mqttium import MQTTProtocolVersion
from mqttium.api import AsyncClient, ReconnectPolicy
from mqttium.persistence import SqliteInflightStore

store = SqliteInflightStore("mqtt-v311.sqlite")
client = AsyncClient(
    "stable-client-id",
    protocol=MQTTProtocolVersion.MQTTv311,
    clean_start=False,
    reconnect=ReconnectPolicy(),
    store=store,
)
```

Use a stable, non-empty client identifier. The broker controls how long it keeps
the session and may impose administrative limits outside the MQTT protocol.

## MQTT 5 sessions

MQTT 5 separates Clean Start from Session Expiry Interval. Requesting
`clean_start=False` is not sufficient if the expiry interval remains zero.

```python
from mqttium import MQTTProtocolVersion
from mqttium.api import AsyncClient, Properties, ReconnectPolicy
from mqttium.persistence import SqliteInflightStore

store = SqliteInflightStore("mqtt-v5.sqlite")
client = AsyncClient(
    "stable-client-id",
    protocol=MQTTProtocolVersion.MQTTv5,
    clean_start=False,
    connect_properties=Properties({"session_expiry_interval": 86_400}),
    reconnect=ReconnectPolicy(),
    store=store,
)
```

The broker may return a different negotiated expiry. Inspect
`client.negotiated.session_expiry_interval` after CONNACK when that distinction
matters operationally.

When the first connection uses an empty ClientID, MQTT 5 requires the broker to
return an Assigned Client Identifier. MQTTium reuses that identifier for
durable reconnects made by the same client instance, so `Clean Start=0`
addresses the same broker Session.

The assigned identifier is not persisted by `InflightStore`. Restart-safe
session recovery therefore requires an explicit, stable, non-empty `client_id`.
If a new engine finds resumable QoS state while configured with an empty
ClientID and `clean_start=False`, it raises `ProtocolError` before sending
CONNECT rather than replaying that state under a different broker-assigned
identity. An empty local store cannot reveal broker-only subscriptions from a
previous process, so applications that depend on retaining those subscriptions
must also configure a stable ClientID.

## What SQLite persists

`SqliteInflightStore` persists protocol state that must remain consistent across
a process restart:

- outbound QoS 1 PUBLISH while waiting for PUBACK;
- outbound QoS 2 PUBLISH or PUBREL and its current transition;
- inbound QoS 2 state used for deduplication and final acknowledgement;
- logical size and transition metadata needed to restore admission accounting.

It does not persist:

- arbitrary application jobs or business results;
- messages already handed to a callback or iterator;
- callback and iterator queues;
- QoS 0 publications after process loss;
- subscription intent independently of the broker session;
- credentials, connection targets or reconnect policy;
- broker-assigned Client Identifiers.

Applications that must recreate subscriptions when the broker reports no
session should do so from `on_connect` or their connection workflow.

## Store ownership and shutdown

The store is synchronous and belongs to the application. Close it only after
the client has stopped using it:

```python
client = AsyncClient("worker", store=store)

try:
    await client.connect("broker.example", 1883)
    await run_application(client)
finally:
    await client.disconnect()
    store.close()
```

`SqliteInflightStore` is also a synchronous context manager when application
structure makes that convenient. `close()` is idempotent, but closing inside an
active store batch is rejected.

The database uses WAL mode. Its schema is versioned with SQLite
`PRAGMA user_version`; supported older schemas migrate atomically on open. A
database written by a newer MQTTium schema is refused rather than interpreted
unsafely.

`SqliteInflightStore` follows Python's synchronous filesystem, DB-API, and data
conversion boundaries. It does not wrap them in a second MQTTium exception
hierarchy:

| Failure boundary | Exception exposed |
| --- | --- |
| Creating the database's parent directory | `OSError`, including `PermissionError` |
| Opening, locking, querying, committing, or using a closed SQLite connection | the relevant `sqlite3.Error` subclass |
| A future or structurally inconsistent MQTTium schema; invalid batch/close lifecycle | `RuntimeError` |
| Invalid persisted storage classes, enum/flag/size values, JSON syntax, or MQTTium JSON markers | `ValueError` (including `json.JSONDecodeError`) |
| Updating an outbound or inbound record that is absent | `KeyError` |
| A non-positive page, message, or byte bound | `ValueError` |

Page and replay iterators execute SQL and hydrate rows lazily, so these failures
may be raised by `next()` rather than when the iterator is created. Invalid
record values supplied by the application can likewise fail during JSON
serialization or SQLite parameter binding. These Provisional persistence
exceptions are separate from the Stable asynchronous client's `MQTTError`
hierarchy. Catch only the specific failure the application can recover from;
do not retry schema incompatibility, data corruption, or `ProgrammingError` as
a transient lock or broker failure.

## Incremental replay and memory

Both shipped stores implement paged replay. SQLite first reads ordered,
payload-free metadata and then loads bounded payload pages. Reopening a large
session therefore does not require materialising every retained payload at
once.

Replay still obeys current message and byte limits. If historical state is
already above a newly reduced outbound limit, MQTTium permits it to drain but
does not admit more work until usage falls below the limit. Inbound replay is
also accounted against the configured inbound byte budget.

Third-party `InflightStore` implementations remain supported. Implementing the
optional paged and transition protocols avoids eager replay and payload reads on
acknowledgement; the minimum store protocol remains correct but may use more
memory.

The runtime capability matrix is deliberately additive rather than a second
store hierarchy:

| Contract | Required guarantee | Operational consequence |
| --- | --- | --- |
| `InflightStore` | atomic `batch()` mutations and ordered whole-record iteration | correctness and third-party compatibility; replay may materialise the store |
| `PagedInflightStore` | ordered pages and payload-free outbound summaries | outbound recovery memory proportional to one page |
| `BoundedInboundReplayStore` | metadata count plus message/byte-bounded hydration | inbound replay memory bounded by one batch, including large sessions |
| `TransitionInflightStore` | conditional atomic state changes and metadata-only lookup | acknowledgements avoid payload reads; QoS 2 phase-two compaction is durable |

Both shipped stores implement every extension, and sessions resolve those
capabilities once when the engine is constructed. A legacy third-party store
therefore keeps the base correctness semantics with eager replay and
best-effort phase-two compaction; it does not silently acquire atomic
conditional-transition guarantees from a read/update fallback.

## Reconnect policy

The native client does not reconnect unless a `ReconnectPolicy` is supplied.
The policy provides:

- jittered exponential backoff;
- an optional maximum retry count;
- a connection timeout;
- reset after a sufficiently stable connection;
- terminal handling for permanent authentication, authorisation and protocol
  failures.

Transport loss does not immediately fail pending receipts when a retained
session can still settle them. They remain pending across reconnect attempts and
complete after replay. They fail when reconnect becomes terminal or when the
broker reports that the previous session is absent.

Each reconnect creates a new decoder and connection epoch. Topic aliases and
other connection-local state are cleared. Deferred work from an older epoch is
discarded instead of being applied to the replacement transport.

## Manual acknowledgement and process failure

With `manual_ack=True`, inbound QoS 1 acknowledgement and the final QoS 2
acknowledgement wait for `await client.ack(message)`. This lets an application
align MQTT acknowledgement with its own durable operation.

It does not create exactly-once business processing. A crash can occur after
the business transaction commits but before the acknowledgement reaches the
broker. The broker may then redeliver. Use an application key, transaction or
deduplication record when duplicate side effects matter.

## Recovery checklist

When session recovery does not behave as expected, retain:

- protocol version, client identifier and clean-start settings;
- requested and negotiated session expiry for MQTT 5;
- CONNACK `session_present`;
- reconnect policy and terminal reason code;
- `client.stats()` before disconnect and after reconnect;
- the last successful QoS transition;
- broker session and persistence configuration;
- SQLite file path, schema version and MQTTium version.

Continue with [Operations](operations.md) for runtime diagnostics and
[Implementation Guide](implementation-guide.md) for the exact QoS transition
contract.
