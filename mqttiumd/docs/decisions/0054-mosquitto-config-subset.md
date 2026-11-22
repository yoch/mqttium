# Record 0054: Native `mosquitto.conf` subset reader and static checker

Status: **Proposed** (22 November 2026).

## Context

`mqttiumd` previously accepted configuration exclusively through command-line
flags, while ADR 0042 provided an external cutover kit bound to a historical
commit. Operators migrating from Mosquitto need a native `-c`/`--config PATH`
loader and an offline `mqttiumd check-config PATH` verifier that honour
CFG-001, CFG-002, CFG-006, and SEC-005 without silently ignoring unknown or
security-weakening directives.

## Decision

1. **Bounded native parser (`mqttiumd::config`)**:
   - Reads UTF-8 configuration files up to 256 KiB, at most 4,096 total lines,
     and at most 64 `*.conf` files from non-nested `include_dir` directories
     loaded in deterministic lexicographical order. Symlinks and cycles are
     refused.
   - Assigns every encountered directive exactly one `DirectiveStatus`
     (`Equivalent`, `Translated`, `RestartRequired`, or `Blocking`) together
     with file path, line number, and diagnostic reason (`CFG-001`).
   - Any `Blocking` directive or cross-directive validation error aborts
     startup before opening any socket or SQLite database (`CFG-002`).
2. **Supported Mosquitto subset**:
   - Network: single `listener PORT [BIND_IP]` (or legacy `port`/`bind_address`),
     `protocol mqtt`, `max_connections` (`-1` translated to `Limits::MAX_CONNECTIONS`,
     or `1..=4096`), `max_packet_size` (`0` or `ANNOUNCED_PACKET_BYTES`),
     `max_inflight_messages 8`, `max_qos 2`, `retain_available true`.
   - Durability (`CFG-006`, `DUR-001`, `DUR-002`): `persistence true` translates
     explicitly to `StorageMode::Durable` (highlighting per-commit WAL
     durability/cost differences vs Mosquitto periodic snapshots);
     `persistence false` translates explicitly to `StorageMode::Volatile`;
     `persistence_location`, `persistence_file`, and `autosave_interval`
     (mapped to periodic atomic SQLite snapshot interval in Volatile mode,
     explicitly flagged as not a proven loss bound in Durable mode).
   - Security (`SEC-005`): TLS listeners require `certfile`, `keyfile`, `cafile`,
     `require_certificate true`, and `mqttiumd_identities` (with optional
     `tls_version tlsv1.3` and `use_identity_as_username true`). Plain
     `allow_anonymous true` is permitted only on loopback addresses without TLS.
     `password_file`, `acl_file`, `psk_*`, `plugin`, `require_certificate false`,
     non-TLS `1.3` versions, websockets, bridging directives, multi-listener
     configs, and all unknown directives are explicitly `Blocking`.
   - Observability: `log_dest file PATH` and `log_type` map to JSONL events
     path and `Level::Ops`/`Level::Audit`.
   - Native extensions (`Equivalent`): `mqttiumd_db`, `mqttiumd_existing_db`,
     `mqttiumd_storage_mode`, `mqttiumd_snapshot_interval`, `mqttiumd_events`,
     `mqttiumd_event_level`, `mqttiumd_identities`, `mqttiumd_io_threads`.
3. **CLI surface**:
   - `mqttiumd check-config PATH` prints the JSON directive report and exits
     `0` if valid or `2` if any directive or cross-check is blocking.
   - `mqttiumd -c PATH` / `--config PATH` loads the validated configuration
     before applying explicit CLI overrides.

## Consequences

- Operators can validate and run supported Mosquitto configurations directly
  with `mqttiumd check-config` and `mqttiumd -c`, with zero silent fallback
  or ignored directives.
