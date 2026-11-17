//! The schema of version 9. Every table is `STRICT`; identities and 64-bit
//! values are 8-byte big-endian BLOBs (32 bytes for an operation-scoped
//! identity, 16 for an exchange, 24 for an opening attempt); large BLOBs
//! come last in their row.

/// `PRAGMA user_version` of the schema below. Any other version is
/// refused before anything is written.
pub const SCHEMA_VERSION: i64 = 9;

/// The format named in `meta`.
pub const FORMAT: &str = "mqttiumd-store-sqlite";

pub const CREATE: &str = "
CREATE TABLE meta (
    one INTEGER PRIMARY KEY CHECK (one = 1),
    format TEXT NOT NULL,
    incarnation BLOB NOT NULL CHECK (length(incarnation) = 8),
    last_opened BLOB NOT NULL CHECK (length(last_opened) = 8),
    opened_by BLOB CHECK (opened_by IS NULL OR length(opened_by) = 24),
    collected_through BLOB NOT NULL CHECK (length(collected_through) = 8),
    revision BLOB NOT NULL CHECK (length(revision) = 8),
    next_session BLOB NOT NULL CHECK (length(next_session) = 8),
    route_revision BLOB NOT NULL CHECK (length(route_revision) = 8),
    limit_payload_bytes BLOB NOT NULL CHECK (length(limit_payload_bytes) = 8),
    limit_references BLOB NOT NULL CHECK (length(limit_references) = 8),
    limit_exchange_slots BLOB NOT NULL CHECK (length(limit_exchange_slots) = 8),
    limit_receipt_slots BLOB NOT NULL CHECK (length(limit_receipt_slots) = 8),
    max_targets_per_admission BLOB NOT NULL CHECK (length(max_targets_per_admission) = 8),
    max_topic_bytes BLOB NOT NULL CHECK (length(max_topic_bytes) = 8),
    max_payload_bytes BLOB NOT NULL CHECK (length(max_payload_bytes) = 8),
    max_sessions BLOB NOT NULL CHECK (length(max_sessions) = 8),
    max_route_targets BLOB NOT NULL CHECK (length(max_route_targets) = 8),
    used_payload_bytes BLOB NOT NULL CHECK (length(used_payload_bytes) = 8),
    used_references BLOB NOT NULL CHECK (length(used_references) = 8),
    used_exchange_slots BLOB NOT NULL CHECK (length(used_exchange_slots) = 8),
    used_receipt_slots BLOB NOT NULL CHECK (length(used_receipt_slots) = 8)
) STRICT;

CREATE TABLE sessions (
    id BLOB PRIMARY KEY CHECK (length(id) = 8),
    revision BLOB NOT NULL CHECK (length(revision) = 8),
    state INTEGER NOT NULL,
    expiry INTEGER NOT NULL,
    opened_by BLOB CHECK (opened_by IS NULL OR length(opened_by) = 32),
    detached_by BLOB CHECK (detached_by IS NULL OR length(detached_by) = 32),
    deadline BLOB CHECK (deadline IS NULL OR length(deadline) = 8),
    client_id BLOB NOT NULL
) STRICT, WITHOUT ROWID;

CREATE TABLE session_clock (
    one INTEGER PRIMARY KEY CHECK (one = 1),
    domain BLOB NOT NULL CHECK (length(domain) = 32),
    tick BLOB NOT NULL CHECK (length(tick) = 8),
    logical BLOB NOT NULL CHECK (length(logical) = 8)
) STRICT;

CREATE TABLE routes (
    topic BLOB NOT NULL,
    position BLOB NOT NULL CHECK (length(position) = 8),
    session BLOB NOT NULL CHECK (length(session) = 8),
    qos INTEGER NOT NULL,
    options INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (topic, position)
) STRICT, WITHOUT ROWID;

CREATE TABLE operations (
    id BLOB PRIMARY KEY CHECK (length(id) = 32),
    decision INTEGER NOT NULL,
    publication BLOB CHECK (publication IS NULL OR length(publication) = 32),
    exchange BLOB CHECK (exchange IS NULL OR length(exchange) = 16),
    reason INTEGER,
    resource INTEGER,
    recipe BLOB NOT NULL,
    facts BLOB CHECK (facts IS NULL OR length(facts) <= 96)
) STRICT, WITHOUT ROWID;

CREATE TABLE fences (
    id BLOB PRIMARY KEY CHECK (length(id) = 32)
) STRICT, WITHOUT ROWID;

CREATE TABLE receipt_origins (
    epoch BLOB NOT NULL CHECK (length(epoch) = 8),
    owner BLOB NOT NULL CHECK (length(owner) = 8),
    consumed_through BLOB NOT NULL CHECK (length(consumed_through) = 8),
    publication_through BLOB NOT NULL CHECK (length(publication_through) = 8),
    payload_through BLOB NOT NULL CHECK (length(payload_through) = 8),
    PRIMARY KEY (epoch, owner)
) STRICT, WITHOUT ROWID;

CREATE TABLE publications (
    id BLOB PRIMARY KEY CHECK (length(id) = 32),
    operation BLOB NOT NULL CHECK (length(operation) = 32),
    source BLOB NOT NULL CHECK (length(source) = 8),
    qos INTEGER NOT NULL,
    retain INTEGER NOT NULL CHECK (retain IN (0, 1)),
    payload BLOB NOT NULL CHECK (length(payload) = 32),
    retained_version BLOB CHECK (retained_version IS NULL OR length(retained_version) = 32),
    expiry_domain BLOB,
    expiry_tick BLOB,
    expiry_interval INTEGER,
    topic BLOB NOT NULL,
    CHECK ((expiry_domain IS NULL AND expiry_tick IS NULL AND expiry_interval IS NULL) OR
           (typeof(expiry_domain) = 'blob' AND length(expiry_domain) = 32 AND
            typeof(expiry_tick) = 'blob' AND length(expiry_tick) = 8 AND
            typeof(expiry_interval) = 'integer' AND expiry_interval >= 0 AND expiry_interval <= 4294967295))
) STRICT, WITHOUT ROWID;

CREATE INDEX publications_by_payload ON publications (payload);

CREATE TABLE retained (
    topic BLOB PRIMARY KEY,
    publication BLOB NOT NULL CHECK(length(publication)=32),
    producer_client_id BLOB NOT NULL
) STRICT, WITHOUT ROWID;

CREATE TABLE payloads (
    id BLOB PRIMARY KEY CHECK (length(id) = 32),
    refs BLOB NOT NULL CHECK (length(refs) = 8),
    length BLOB NOT NULL CHECK (length(length) = 8),
    bytes BLOB NOT NULL
) STRICT, WITHOUT ROWID;

CREATE TABLE obligations (
    publication BLOB NOT NULL CHECK (length(publication) = 32),
    idx INTEGER NOT NULL CHECK (idx >= 0 AND idx <= 4294967295),
    target BLOB NOT NULL CHECK (length(target) = 8),
    target_revision BLOB NOT NULL CHECK (length(target_revision) = 8),
    qos INTEGER NOT NULL,
    cause INTEGER NOT NULL,
    retain INTEGER NOT NULL DEFAULT 0,
    phase INTEGER NOT NULL,
    PRIMARY KEY (publication, idx)
) STRICT, WITHOUT ROWID;

CREATE INDEX obligations_by_target ON obligations (target, qos, publication, idx);

CREATE TABLE outbound (
    publication BLOB NOT NULL CHECK(length(publication)=32),
    idx INTEGER NOT NULL CHECK(idx>=0 AND idx<=4294967295),
    target BLOB NOT NULL CHECK(length(target)=8),
    target_revision BLOB NOT NULL CHECK(length(target_revision)=8),
    packet_id INTEGER NOT NULL CHECK(packet_id>=1 AND packet_id<=65535),
    claim BLOB NOT NULL CHECK(length(claim)=32),
    started INTEGER NOT NULL DEFAULT 0 CHECK(started IN (0,1)),
    PRIMARY KEY(publication,idx),
    UNIQUE(target,packet_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE inbound (
    session BLOB NOT NULL CHECK (length(session) = 8),
    packet_id INTEGER NOT NULL CHECK (packet_id >= 1 AND packet_id <= 65535),
    exchange BLOB NOT NULL CHECK (length(exchange) = 16),
    publication BLOB NOT NULL CHECK (length(publication) = 32),
    phase INTEGER NOT NULL,
    PRIMARY KEY (session, packet_id)
) STRICT, WITHOUT ROWID;
";
