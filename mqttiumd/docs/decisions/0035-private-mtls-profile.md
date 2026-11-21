# 0035 — TLS 1.3, durable session ownership and static topic grants

Status: **Accepted with amendments** (21 November 2026, decided under the
repository owner's delegation on the advice of the independent audit of
`8e4303d`). This decision applies to the private-network MQTT 5 product and
preserves the existing clear-text loopback profile.

Amendment of acceptance (21 November 2026).
Amended by 0039. Authorize each SUBSCRIBE filter independently, encode one
SUBACK result per position and apply the commit fence to granted changes.
Size unauthenticated handshakes, certificate chains, policies and durable
ownership independently of total transport capacity. Record 0048 states the
capacities of the runtime and their derivation; the sixteen-connection
numbers below describe the bounded qualification profile.

Amended by 0037 (record 0047): schemas 10 and 11 are refused without
migration; "Schema 10 continues to serve the loopback profile" below no
longer holds.

Note (21 November 2026): the budgets this record states for sixteen connections are
the bounded profile of `runtime::Limits`. The connection limit is an option
since record 0046 (proposed), and the queues sized for sixteen connections
grow with it.

## Trust and storage authority

The listener loads its complete configuration before it starts a writer.
Rustls 0.23.46 with ring negotiates TLS 1.3 exclusively. WebPki verifies
client chains, dates, client-auth usage and signatures of the handshake.
The verified leaf must match a configured SHA-256 DER pin. The server
certificate and key must match and its chain must verify under the configured
trust anchors for server-auth usage. The client independently verifies the
server name. The application offers no TLS tickets, resumption or early data.
The certificate verifier needs an admissible system wall-clock reading.
Certificate dates and the storage clock for Message Expiry have separate roles.

A generation receives its immutable principal after the handshake finishes.
A policy assigns each MQTT ClientID to exactly one named principal. The
runtime checks CONNECT including Will authorization before allocating an
Open operation, examining the old attachment for takeover or entering the
lifecycle cut. Unauthorized CONNECT receives CONNACK 0x87 with SP0. A new
certificate pinned to the same principal can regain the same authority.

SQLite schema 11 extends the schema 10 tables with a singleton policy hash,
a bounded client owner registry and one owner row for each live session.
Creation installs the registry before any Open. A session INSERT adds its
owner in the same SQLite transaction; session deletion removes that owner
when existing lifecycle cleanup removes the session. Open resume preserves
it. The existing OperationId and attachment continue to identify the
lifecycle command and its replay. They keep their canonical encodings.
The rules and normalized ownership map are checked read-only before settling
the writer or changing an epoch. Refusals preserve the file. Schema 10
continues to serve the loopback profile. Opening either mode's database in
the other mode fails; neither mode migrates or resets a file.

Canonical hashing includes all identities, ClientIDs and literal prefixes
with length delimiters and normalized ordering. Pins and CA material do
not participate. Rotating credentials under the same authorization registry
preserves checked ownership. Any change to the registry or topic grants
requires an explicit offline data transition because existing obligations
and retained replay were authorized under the recorded policy. The trusted
operator and filesystem control the database and configuration.

## Topic authority

An allowed prefix consists of literal MQTT levels, including any empty
levels and dollar-prefixed levels. A topic is inside the grant when it is
equal to the root or follows it at a slash boundary. A validated subscription
filter must have the same literal leading levels. Wildcards can occur after
those levels, so a valid filter cannot escape the granted subtree. The
selector still implements the MQTT dollar rule and preserves case and UTF-8.
Publish, retain, Will, subscribe and unsubscribe checks apply before store
operations and before queuing a core packet. The profile rejects a whole
forbidden MQTT packet with DISCONNECT 0x87. Established sessions obey normal
Will rules for abnormal disconnect. A rejected packet cannot create a
business admission. Session reopening retains the recorded rights for
durable routes and obligations.

## Transport progress

One nonblocking task owns the socket I/O and a single TLS state. Eight
unauthenticated handshakes may occupy the existing 16 transport slots. Each
has a two-second and 64 KiB incoming budget. Up to four certificates of
up to 8 KiB each are permitted per chain. The TLS library send buffers
have a 96 KiB limit; the application holds one bounded MQTT frame and
one sealed ciphertext frame in addition to existing bounded input and
output queues. The static configuration admits 128 principals, 128 distinct
ClientIDs, 256 certificate pins and 512 prefix rules in at most 256 KiB.

Handshake, alert and KeyUpdate outputs use the control path. The TLS task
drains queued control ciphertext before sealing one MQTT frame. It verifies
the resulting length against the pinned TLS 1.3 record framing and refuses
ambiguous output before writing it. The first positive socket write of that
ciphertext publishes the correlated Started fact. MQTT plaintext acceptance
does not publish Started. Before each zero-progress retry the task checks
the original expiration timestamp. A sealed expired frame reports NotStarted
and closes, since skipping previously sealed records would leave a TLS
sequence gap. If the encoded remaining lifetime ages before first progress,
the task closes without claiming transmission. After positive progress the
same frame completes within the write budget or its connection closes.
Started and completion remain independent observations, and the existing
durable claim handles early PUBACK and an interrupted write.

TLS close_notify is a transport observation. Without MQTT DISCONNECT it
follows the Will loss path. Failed TLS handshakes cannot arm a Will.
The test hooks tls.frame.sealed and tls.frame.first_bytes pause the actual
TLS task before any ciphertext or after three application-record bytes.
These hooks are excluded from the normal binary.

## Reception scope

A source-pinned receiver exercises real public clients, negative certificates,
unauthorized ClientIDs and topics, authenticated replay, resource limits,
and TLS fragmentation at the socket boundary. It uses private, ephemeral
credentials and keeps only diagnostic and business observations. Linux
monotonic observations correlate physical close, SQLite commit return and
recipient PUBLISH in the finite service measurement, using the same workload
on clear-text loopback and mTLS. Keep Alive has a separate detection
measurement. The finite tests and measurements cover this profile.
