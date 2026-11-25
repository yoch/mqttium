//! Strict native `mosquitto.conf` subset reader (ADR 0054).
//!
//! Satisfies CFG-001, CFG-002, CFG-006, and SEC-005:
//! - Every encountered directive receives an explicit classification
//!   (`Equivalent`, `Translated`, `RestartRequired`, or `Blocking`).
//! - Unknown or security-weakening directives are never silently ignored; any
//!   `Blocking` directive prevents startup before any socket or database is
//!   opened.
//! - File reads and `include_dir` traversal are strictly bounded.

use std::collections::BTreeSet;
use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use serde_json::{Value, json};

use crate::codec::ANNOUNCED_PACKET_BYTES;
use crate::events::Level;
use crate::runtime::{Config, Limits};
use crate::security::TlsFiles;
use crate::writer::{Hooks, StorageMode};

/// Maximum size of a single configuration file (256 KiB).
pub const MAX_CONFIG_FILE_BYTES: usize = 256 * 1024;
/// Maximum total directives/lines parsed across the main file and includes.
pub const MAX_CONFIG_LINES: usize = 4_096;
/// Maximum number of files loaded via `include_dir`.
pub const MAX_INCLUDED_FILES: usize = 64;

/// Per-directive compatibility classification (CFG-001).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DirectiveStatus {
    /// Identical semantics in `mqttiumd`.
    Equivalent,
    /// Supported with explicitly documented semantic or cost differences.
    Translated,
    /// Recognized option whose change requires restarting the broker.
    RestartRequired,
    /// Unsupported, ambiguous, unknown, or security-weakening: blocks startup.
    Blocking,
}

impl DirectiveStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Equivalent => "equivalent",
            Self::Translated => "translated",
            Self::RestartRequired => "restart_required",
            Self::Blocking => "blocking",
        }
    }
}

/// Decision recorded for one directive in a configuration file.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DirectiveReport {
    pub file: PathBuf,
    pub line: usize,
    pub directive: String,
    pub status: DirectiveStatus,
    pub reason: String,
}

/// Complete static analysis of a configuration file (and its `include_dir`s).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ConfigReport {
    pub ok: bool,
    pub reports: Vec<DirectiveReport>,
    pub errors: Vec<String>,
    /// What a launch with this file alone still lacks, as the options of
    /// the command line that give it. A valid file can leave the event
    /// journal or the database to the command line.
    pub launch_requires: Vec<&'static str>,
}

impl ConfigReport {
    #[must_use]
    pub fn to_json(&self) -> Value {
        let directives: Vec<Value> = self
            .reports
            .iter()
            .map(|r| {
                json!({
                    "file": r.file.display().to_string(),
                    "line": r.line,
                    "directive": r.directive,
                    "status": r.status.as_str(),
                    "reason": r.reason,
                })
            })
            .collect();
        json!({
            "ok": self.ok,
            "directives": directives,
            "errors": self.errors,
            "launch_requires": self.launch_requires,
        })
    }
}

/// Validated configuration ready to construct a runtime [`Config`].
#[derive(Clone)]
pub struct ParsedConfig {
    pub db: Option<PathBuf>,
    pub require_existing_db: bool,
    pub storage_mode: StorageMode,
    pub listen: SocketAddr,
    pub events: Option<PathBuf>,
    pub event_level: Level,
    pub limits: Limits,
    pub tls: Option<TlsFiles>,
    pub io_threads: Option<usize>,
    pub delivery_shards: Option<usize>,
    pub reports: Vec<DirectiveReport>,
}

impl ParsedConfig {
    /// Disk storage or snapshotting needs a database path.
    fn needs_db(&self) -> bool {
        matches!(
            self.storage_mode,
            StorageMode::Durable
                | StorageMode::Log
                | StorageMode::Volatile {
                    snapshot_interval: Some(_),
                }
        ) || self.require_existing_db
    }

    /// The options a launch must still be given, because this file names
    /// neither the path they give. The same conditions as
    /// [`ParsedConfig::into_runtime_config`], which refuses without them.
    #[must_use]
    pub fn launch_requires(&self) -> Vec<&'static str> {
        let mut missing = Vec::new();
        if self.needs_db() && self.db.is_none() {
            missing.push("--db");
        }
        if self.events.is_none() {
            missing.push("--events");
        }
        missing
    }

    /// Finalizes into a runnable [`Config`], requiring `--db` when disk
    /// storage or snapshotting is active and `--events` either from the
    /// configuration file or CLI overrides.
    pub fn into_runtime_config(self, hooks: Hooks) -> Result<Config, String> {
        let needs_db = self.needs_db();
        let db = if needs_db {
            self.db.ok_or_else(|| {
                "database path missing (set persistence_location/mqttiumd_db or --db)".to_owned()
            })?
        } else {
            self.db.unwrap_or_default()
        };
        Ok(Config {
            db,
            require_existing_db: self.require_existing_db,
            storage_mode: self.storage_mode,
            listen: self.listen,
            events: self.events.ok_or_else(|| {
                "events log path missing (set `log_dest file PATH`, `mqttiumd_events PATH`, or --events)"
                    .to_owned()
            })?,
            event_level: self.event_level,
            limits: self.limits,
            hooks,
            tls: self.tls,
            delivery_shards: self.delivery_shards.unwrap_or(0),
        })
    }
}

#[derive(Default)]
struct Accumulator {
    listener_set: bool,
    legacy_port: Option<u16>,
    legacy_bind: Option<IpAddr>,
    listen: Option<SocketAddr>,
    allow_anonymous: Option<bool>,
    max_connections: Option<usize>,
    persistence: Option<bool>,
    persistence_location: Option<PathBuf>,
    persistence_file: Option<String>,
    autosave_interval: Option<u64>,
    explicit_db: Option<PathBuf>,
    require_existing_db: bool,
    explicit_storage_mode: Option<String>,
    explicit_snapshot_interval: Option<u64>,
    certfile: Option<PathBuf>,
    keyfile: Option<PathBuf>,
    cafile: Option<PathBuf>,
    require_certificate: Option<bool>,
    identities: Option<PathBuf>,
    events: Option<PathBuf>,
    event_level: Option<Level>,
    io_threads: Option<usize>,
    delivery_shards: Option<usize>,
    reports: Vec<DirectiveReport>,
    errors: Vec<String>,
    total_lines: usize,
    included_files: usize,
    seen_files: BTreeSet<PathBuf>,
}

impl Accumulator {
    fn record(
        &mut self,
        file: &Path,
        line: usize,
        directive: &str,
        status: DirectiveStatus,
        reason: impl Into<String>,
    ) {
        self.reports.push(DirectiveReport {
            file: file.to_path_buf(),
            line,
            directive: directive.to_owned(),
            status,
            reason: reason.into(),
        });
    }

    fn has_blocking(&self) -> bool {
        !self.errors.is_empty()
            || self
                .reports
                .iter()
                .any(|r| r.status == DirectiveStatus::Blocking)
    }
}

fn resolve_path(base_dir: &Path, raw: &str) -> PathBuf {
    let path = Path::new(raw);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base_dir.join(path)
    }
}

fn parse_bool(raw: &str) -> Option<bool> {
    match raw {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn is_plain_filename(name: &str) -> bool {
    if name.is_empty() || name.contains('/') || name.contains('\\') {
        return false;
    }
    let mut components = Path::new(name).components();
    matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none()
}

fn read_bounded_file(path: &Path) -> Result<String, String> {
    let meta = fs::symlink_metadata(path)
        .map_err(|e| format!("cannot stat config file {}: {e}", path.display()))?;
    if meta.file_type().is_symlink() {
        return Err(format!(
            "refusing symlink configuration file: {}",
            path.display()
        ));
    }
    if !meta.file_type().is_file() {
        return Err(format!(
            "configuration path is not a regular file: {}",
            path.display()
        ));
    }
    if meta.len() > MAX_CONFIG_FILE_BYTES as u64 {
        return Err(format!(
            "configuration file {} exceeds {} bytes",
            path.display(),
            MAX_CONFIG_FILE_BYTES
        ));
    }
    let bytes =
        fs::read(path).map_err(|e| format!("cannot read config file {}: {e}", path.display()))?;
    if bytes.len() > MAX_CONFIG_FILE_BYTES {
        return Err(format!(
            "configuration file {} exceeds {} bytes",
            path.display(),
            MAX_CONFIG_FILE_BYTES
        ));
    }
    String::from_utf8(bytes)
        .map_err(|_| format!("configuration file {} is not valid UTF-8", path.display()))
}

fn parse_file(acc: &mut Accumulator, path: &Path, depth: usize) -> Result<(), String> {
    let canonical_key = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    if !acc.seen_files.insert(canonical_key) {
        return Err(format!(
            "duplicate or cyclic configuration file: {}",
            path.display()
        ));
    }
    let content = read_bounded_file(path)?;
    let base_dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));

    for (idx, raw_line) in content.lines().enumerate() {
        let line_no = idx + 1;
        acc.total_lines += 1;
        if acc.total_lines > MAX_CONFIG_LINES {
            return Err(format!(
                "configuration exceeds maximum line budget ({MAX_CONFIG_LINES})"
            ));
        }
        let trimmed = raw_line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let mut parts = trimmed.splitn(2, char::is_whitespace);
        let directive = parts.next().unwrap_or("");
        let rest = parts.next().map(str::trim).unwrap_or("");
        if rest.is_empty() {
            acc.record(
                path,
                line_no,
                directive,
                DirectiveStatus::Blocking,
                "directive requires a value",
            );
            continue;
        }

        match directive {
            "include_dir" => {
                if depth >= 1 {
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Blocking,
                        "nested include_dir exceeds maximum depth (1)",
                    );
                    continue;
                }
                let dir = resolve_path(base_dir, rest);
                let meta = match fs::symlink_metadata(&dir) {
                    Ok(m) => m,
                    Err(e) => {
                        acc.record(
                            path,
                            line_no,
                            directive,
                            DirectiveStatus::Blocking,
                            format!("cannot access include_dir {}: {e}", dir.display()),
                        );
                        continue;
                    }
                };
                if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Blocking,
                        format!("include_dir {} must be a regular directory", dir.display()),
                    );
                    continue;
                }
                let mut entries = Vec::new();
                let read_dir = match fs::read_dir(&dir) {
                    Ok(rd) => rd,
                    Err(e) => {
                        acc.record(
                            path,
                            line_no,
                            directive,
                            DirectiveStatus::Blocking,
                            format!("cannot read include_dir {}: {e}", dir.display()),
                        );
                        continue;
                    }
                };
                for entry in read_dir {
                    let Ok(entry) = entry else {
                        acc.record(
                            path,
                            line_no,
                            directive,
                            DirectiveStatus::Blocking,
                            "error reading include_dir entry",
                        );
                        continue;
                    };
                    let entry_path = entry.path();
                    if entry_path.extension().and_then(|s| s.to_str()) == Some("conf") {
                        entries.push(entry_path);
                    }
                }
                entries.sort();
                if acc.included_files + entries.len() > MAX_INCLUDED_FILES {
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Blocking,
                        format!("include_dir exceeds max included files ({MAX_INCLUDED_FILES})"),
                    );
                    continue;
                }
                acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Equivalent,
                    format!(
                        "loading {} *.conf file(s) in lexicographical order",
                        entries.len()
                    ),
                );
                for entry_path in entries {
                    acc.included_files += 1;
                    if let Err(err) = parse_file(acc, &entry_path, depth + 1) {
                        acc.errors.push(err);
                    }
                }
            }
            "listener" => {
                if acc.listener_set {
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Blocking,
                        "multiple listeners are not supported (single listener per broker)",
                    );
                    continue;
                }
                if acc.legacy_port.is_some() || acc.legacy_bind.is_some() {
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Blocking,
                        "cannot mix `listener` with legacy `port` or `bind_address`",
                    );
                    continue;
                }
                let tokens: Vec<&str> = rest.split_whitespace().collect();
                if tokens.is_empty() || tokens.len() > 2 {
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Blocking,
                        "listener syntax is `listener PORT [BIND_IP]`",
                    );
                    continue;
                }
                let Ok(port) = tokens[0].parse::<u16>() else {
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Blocking,
                        "invalid listener port",
                    );
                    continue;
                };
                let (ip, status, reason) = if let Some(addr_str) = tokens.get(1) {
                    match addr_str.parse::<IpAddr>() {
                        Ok(ip) => (
                            ip,
                            DirectiveStatus::Equivalent,
                            format!("single TCP listener on {ip}:{port}"),
                        ),
                        Err(_) => {
                            acc.record(
                                path,
                                line_no,
                                directive,
                                DirectiveStatus::Blocking,
                                "listener bind address must be a literal IPv4 or IPv6 address",
                            );
                            continue;
                        }
                    }
                } else {
                    (
                        IpAddr::V4(Ipv4Addr::LOCALHOST),
                        DirectiveStatus::Translated,
                        format!(
                            "single TCP listener on 127.0.0.1:{port} (omitted bind IP narrowed to loopback; specify explicit bind IP for non-loopback TLS)"
                        ),
                    )
                };
                acc.listener_set = true;
                acc.listen = Some(SocketAddr::new(ip, port));
                acc.record(path, line_no, directive, status, reason);
            }
            "port" => {
                if acc.listener_set {
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Blocking,
                        "cannot mix legacy `port` with `listener`",
                    );
                    continue;
                }
                match rest.parse::<u16>() {
                    Ok(port) => {
                        acc.legacy_port = Some(port);
                        acc.record(
                            path,
                            line_no,
                            directive,
                            DirectiveStatus::Translated,
                            format!("legacy `port {port}` mapped to single listener"),
                        );
                    }
                    Err(_) => acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Blocking,
                        "invalid port number",
                    ),
                }
            }
            "bind_address" => {
                if acc.listener_set {
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Blocking,
                        "cannot mix legacy `bind_address` with `listener`",
                    );
                    continue;
                }
                match rest.parse::<IpAddr>() {
                    Ok(ip) => {
                        acc.legacy_bind = Some(ip);
                        acc.record(
                            path,
                            line_no,
                            directive,
                            DirectiveStatus::Translated,
                            format!("legacy `bind_address {ip}` mapped to single listener"),
                        );
                    }
                    Err(_) => acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Blocking,
                        "bind_address must be a literal IPv4 or IPv6 address",
                    ),
                }
            }
            "protocol" => {
                if rest == "mqtt" {
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Equivalent,
                        "native MQTT over TCP/TLS",
                    );
                } else {
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Blocking,
                        format!("protocol `{rest}` is unsupported (only `mqtt` is supported)"),
                    );
                }
            }
            "max_connections" => match rest.parse::<i64>() {
                Ok(-1) => {
                    acc.max_connections = Some(Limits::MAX_CONNECTIONS);
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Translated,
                        format!(
                            "unbounded Mosquitto `-1` mapped to bounded profile ceiling ({})",
                            Limits::MAX_CONNECTIONS
                        ),
                    );
                }
                Ok(n) if (1..=Limits::MAX_CONNECTIONS as i64).contains(&n) => {
                    acc.max_connections = Some(n as usize);
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Equivalent,
                        format!("max_connections = {n}"),
                    );
                }
                _ => acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Blocking,
                    format!(
                        "max_connections must be -1 or 1..={}",
                        Limits::MAX_CONNECTIONS
                    ),
                ),
            },
            "max_packet_size" => match rest.parse::<u32>() {
                Ok(0) => acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Translated,
                    format!(
                        "0 (unbounded in Mosquitto) mapped to advertised broker limit ({ANNOUNCED_PACKET_BYTES} bytes)"
                    ),
                ),
                Ok(n) if n == ANNOUNCED_PACKET_BYTES => acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Equivalent,
                    format!("matches advertised packet limit ({ANNOUNCED_PACKET_BYTES} bytes)"),
                ),
                _ => acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Blocking,
                    format!(
                        "custom max_packet_size is unsupported (advertised limit is fixed at {ANNOUNCED_PACKET_BYTES} bytes)"
                    ),
                ),
            },
            "max_inflight_messages" => match rest.parse::<usize>() {
                Ok(n) if n == crate::receiving::MAXIMUM => acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Equivalent,
                    format!("matches advertised Receive Maximum ({n})"),
                ),
                _ => acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Blocking,
                    format!(
                        "custom max_inflight_messages is unsupported (Receive Maximum is fixed at {})",
                        crate::receiving::MAXIMUM
                    ),
                ),
            },
            "max_qos" => {
                if rest == "2" {
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Equivalent,
                        "QoS 0, 1, and 2 supported",
                    );
                } else {
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Blocking,
                        "only max_qos 2 is supported",
                    );
                }
            }
            "retain_available" => match parse_bool(rest) {
                Some(true) => acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Equivalent,
                    "retained messages enabled",
                ),
                _ => acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Blocking,
                    "disabling retained messages is not supported",
                ),
            },
            "persistence" => match parse_bool(rest) {
                Some(true) => {
                    acc.persistence = Some(true);
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Translated,
                        "CFG-006: mapped to Durable SQLite WAL mode (per-commit fsync/group-commit; stronger durability and different cost profile than Mosquitto periodic snapshots)",
                    );
                }
                Some(false) => {
                    acc.persistence = Some(false);
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Translated,
                        "DUR-001: mapped to pure in-memory Volatile mode (no disk reads, no snapshots; ignores `autosave_interval` and existing DB files)",
                    );
                }
                None => acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Blocking,
                    "persistence expects `true` or `false`",
                ),
            },
            "persistence_location" => {
                let dir = resolve_path(base_dir, rest);
                acc.persistence_location = Some(dir.clone());
                acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Translated,
                    format!(
                        "SQLite database directory set to {} (uses mqttiumd schema, not mosquitto.db)",
                        dir.display()
                    ),
                );
            }
            "persistence_file" => {
                if !is_plain_filename(rest) {
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Blocking,
                        "persistence_file must be a plain filename without path separators",
                    );
                } else {
                    acc.persistence_file = Some(rest.to_owned());
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Translated,
                        format!("SQLite database filename set to `{rest}`"),
                    );
                }
            }
            "autosave_interval" => match rest.parse::<u64>() {
                Ok(secs) if secs <= 86_400 => {
                    acc.autosave_interval = Some(secs);
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Translated,
                        "CFG-006: not a proven loss bound; inactive when `persistence false`, ignored in Durable mode (which commits per transaction), and mapped to periodic atomic SQLite snapshot interval when `mqttiumd_storage_mode volatile` is enabled without `persistence false`",
                    );
                }
                _ => acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Blocking,
                    "autosave_interval must be an integer 0..=86400 seconds",
                ),
            },
            "allow_anonymous" => match parse_bool(rest) {
                Some(v) => {
                    acc.allow_anonymous = Some(v);
                    acc.record(
                        path,
                        line_no,
                        directive,
                        if v {
                            DirectiveStatus::Translated
                        } else {
                            DirectiveStatus::Equivalent
                        },
                        if v {
                            "anonymous plain access permitted only on loopback without TLS (SEC-005)"
                        } else {
                            "anonymous access disabled; requires mTLS + mqttiumd_identities"
                        },
                    );
                }
                None => acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Blocking,
                    "allow_anonymous expects `true` or `false`",
                ),
            },
            "certfile" => {
                let p = resolve_path(base_dir, rest);
                acc.certfile = Some(p.clone());
                acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Equivalent,
                    format!("server certificate chain: {}", p.display()),
                );
            }
            "keyfile" => {
                let p = resolve_path(base_dir, rest);
                acc.keyfile = Some(p.clone());
                acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Equivalent,
                    format!("server private key: {}", p.display()),
                );
            }
            "cafile" => {
                let p = resolve_path(base_dir, rest);
                acc.cafile = Some(p.clone());
                acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Equivalent,
                    format!("client CA bundle: {}", p.display()),
                );
            }
            "require_certificate" => match parse_bool(rest) {
                Some(true) => {
                    acc.require_certificate = Some(true);
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Equivalent,
                        "mandatory client certificates (mTLS)",
                    );
                }
                Some(false) => {
                    acc.require_certificate = Some(false);
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Blocking,
                        "SEC-005: TLS without mandatory client certificates is forbidden",
                    );
                }
                None => acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Blocking,
                    "require_certificate expects `true` or `false`",
                ),
            },
            "tls_version" => {
                if rest == "tlsv1.3" {
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Equivalent,
                        "TLS 1.3 enforced",
                    );
                } else {
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Blocking,
                        format!(
                            "SEC-005: tls_version `{rest}` is refused (only `tlsv1.3` is permitted)"
                        ),
                    );
                }
            }
            "use_identity_as_username" => match parse_bool(rest) {
                Some(true) => acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Translated,
                    "client certificate identity bound via `mqttiumd_identities` policy",
                ),
                _ => acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Blocking,
                    "SEC-005: disabling certificate identity binding is forbidden",
                ),
            },
            "log_dest" => {
                let mut parts = rest.splitn(2, char::is_whitespace);
                let kind = parts.next().unwrap_or("");
                let target = parts.next().map(str::trim).unwrap_or("");
                if kind == "file" && !target.is_empty() {
                    let p = resolve_path(base_dir, target);
                    acc.events = Some(p.clone());
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Translated,
                        format!("JSONL events stream written to {}", p.display()),
                    );
                } else {
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Blocking,
                        format!(
                            "log_dest `{rest}` is unsupported (use `log_dest file PATH` or `mqttiumd_events PATH`)"
                        ),
                    );
                }
            }
            "log_type" => match rest {
                "error" | "warning" | "notice" | "information" => {
                    if acc.event_level != Some(Level::Audit) {
                        acc.event_level = Some(Level::Ops);
                    }
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Translated,
                        "mapped to JSONL event level `ops`",
                    );
                }
                "debug" | "all" => {
                    acc.event_level = Some(Level::Audit);
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Translated,
                        "mapped to JSONL event level `audit`",
                    );
                }
                _ => acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Blocking,
                    format!("unsupported log_type `{rest}`"),
                ),
            },
            // Explicit native mqttiumd extension directives:
            "mqttiumd_db" => {
                let p = resolve_path(base_dir, rest);
                acc.explicit_db = Some(p.clone());
                acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Equivalent,
                    format!("native SQLite database path: {}", p.display()),
                );
            }
            "mqttiumd_existing_db" => match parse_bool(rest) {
                Some(v) => {
                    acc.require_existing_db = v;
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Equivalent,
                        format!("require_existing_db = {v}"),
                    );
                }
                None => acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Blocking,
                    "mqttiumd_existing_db expects `true` or `false`",
                ),
            },
            "mqttiumd_storage_mode" => match rest {
                "durable" | "log" | "volatile" => {
                    acc.explicit_storage_mode = Some(rest.to_owned());
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Equivalent,
                        format!("explicit storage mode `{rest}`"),
                    );
                }
                _ => acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Blocking,
                    "mqttiumd_storage_mode expects `durable`, `log`, or `volatile`",
                ),
            },
            "mqttiumd_snapshot_interval" => match rest.parse::<u64>() {
                Ok(secs) if secs <= 86_400 => {
                    acc.explicit_snapshot_interval = Some(secs);
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Equivalent,
                        format!("volatile snapshot interval = {secs}s"),
                    );
                }
                _ => acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Blocking,
                    "mqttiumd_snapshot_interval expects 0..=86400 seconds",
                ),
            },
            "mqttiumd_events" => {
                let p = resolve_path(base_dir, rest);
                acc.events = Some(p.clone());
                acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Equivalent,
                    format!("native JSONL events stream: {}", p.display()),
                );
            }
            "mqttiumd_event_level" => match rest.parse::<Level>() {
                Ok(level) => {
                    acc.event_level = Some(level);
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Equivalent,
                        format!("native JSONL event level `{rest}`"),
                    );
                }
                Err(()) => acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Blocking,
                    "mqttiumd_event_level expects `ops` or `audit`",
                ),
            },
            "mqttiumd_identities" => {
                let p = resolve_path(base_dir, rest);
                acc.identities = Some(p.clone());
                acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Equivalent,
                    format!("native mTLS identities/grants policy: {}", p.display()),
                );
            }
            "mqttiumd_io_threads" => match rest.parse::<usize>() {
                Ok(n) if (1..=32).contains(&n) => {
                    acc.io_threads = Some(n);
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Equivalent,
                        format!("I/O reactor threads = {n}"),
                    );
                }
                _ => acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Blocking,
                    "mqttiumd_io_threads expects 1..=32",
                ),
            },
            "mqttiumd_delivery_shards" => match rest.parse::<usize>() {
                Ok(n) if n <= crate::delivery_shard::MAX_DELIVERY_SHARDS => {
                    acc.delivery_shards = Some(n);
                    acc.record(
                        path,
                        line_no,
                        directive,
                        DirectiveStatus::Equivalent,
                        format!("resident delivery shards = {n}"),
                    );
                }
                _ => acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Blocking,
                    format!(
                        "mqttiumd_delivery_shards expects 0..={}",
                        crate::delivery_shard::MAX_DELIVERY_SHARDS
                    ),
                ),
            },
            // Explicitly diagnosed security/routing/bridge directives (CFG-002 / SEC-005):
            "password_file" | "acl_file" | "psk_file" | "psk_hint" | "plugin" | "auth_plugin" => {
                acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Blocking,
                    format!(
                        "CFG-002/SEC-005: `{directive}` is not supported natively; migrate credentials/ACLs to TLS 1.3 mTLS + `mqttiumd_identities`"
                    ),
                );
            }
            "connection" | "address" | "topic" | "bridge_protocol_version" | "bridge_cafile" => {
                acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Blocking,
                    format!("CFG-002: MQTT bridging directive `{directive}` is not supported"),
                );
            }
            "allow_zero_length_clientid" => {
                acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Blocking,
                    "explicit non-empty ClientID is required by mqttiumd",
                );
            }
            // CFG-001: Every unknown directive MUST be refused, never silently ignored.
            _ => {
                acc.record(
                    path,
                    line_no,
                    directive,
                    DirectiveStatus::Blocking,
                    format!("CFG-001: unknown or unsupported directive `{directive}`"),
                );
            }
        }
    }
    Ok(())
}

/// Parses and validates a `mosquitto.conf`-compatible file without opening any
/// socket or database.
pub fn check_file(path: &Path) -> Result<(ConfigReport, Option<ParsedConfig>), String> {
    let mut acc = Accumulator::default();
    if let Err(err) = parse_file(&mut acc, path, 0) {
        acc.errors.push(err);
    }

    // Resolve listener address.
    let listen = if let Some(addr) = acc.listen {
        addr
    } else if acc.legacy_port.is_some() || acc.legacy_bind.is_some() {
        SocketAddr::new(
            acc.legacy_bind.unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            acc.legacy_port.unwrap_or(1883),
        )
    } else {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1883)
    };

    // Resolve TLS & security posture (SEC-005, CFG-002).
    let any_tls = acc.certfile.is_some()
        || acc.keyfile.is_some()
        || acc.cafile.is_some()
        || acc.identities.is_some()
        || acc.require_certificate.is_some();

    let explicit_listener =
        acc.listener_set || acc.legacy_port.is_some() || acc.legacy_bind.is_some();

    let tls = if any_tls {
        if acc.allow_anonymous == Some(true) {
            acc.errors.push(
                "SEC-005: `allow_anonymous true` cannot be combined with TLS / mTLS".to_owned(),
            );
        }
        if acc.require_certificate != Some(true) {
            acc.errors.push(
                "SEC-005: TLS listener requires explicit `require_certificate true`".to_owned(),
            );
        }
        match (
            acc.certfile.clone(),
            acc.keyfile.clone(),
            acc.cafile.clone(),
            acc.identities.clone(),
        ) {
            (Some(cert), Some(key), Some(client_ca), Some(identities)) => Some(TlsFiles {
                cert,
                key,
                client_ca,
                identities,
            }),
            _ => {
                acc.errors.push(
                    "SEC-005: TLS listener requires `certfile`, `keyfile`, `cafile`, `require_certificate true`, and `mqttiumd_identities`"
                        .to_owned(),
                );
                None
            }
        }
    } else {
        if acc.allow_anonymous == Some(false) {
            acc.errors.push(
                "CFG-002: `allow_anonymous false` requires TLS 1.3 mTLS (`certfile`, `keyfile`, `cafile`, `require_certificate true`, `mqttiumd_identities`)"
                    .to_owned(),
            );
        } else if explicit_listener && acc.allow_anonymous.is_none() {
            acc.errors.push(
                "SEC-005/CFG-002: explicit listener defaults to `allow_anonymous false`; set explicit `allow_anonymous true` on loopback or configure TLS 1.3 mTLS (`certfile`, `keyfile`, `cafile`, `require_certificate true`, `mqttiumd_identities`)"
                    .to_owned(),
            );
        }
        if !listen.ip().is_loopback() {
            acc.errors.push(format!(
                "SEC-005: plain listener on non-loopback address {} is refused without TLS 1.3 mTLS",
                listen.ip()
            ));
        }
        None
    };

    // Resolve storage mode (CFG-006, DUR-001, DUR-002).
    let storage_mode = match (acc.explicit_storage_mode.as_deref(), acc.persistence) {
        (Some("durable"), Some(false)) => {
            acc.errors.push(
                "CFG-006: `persistence false` conflicts with `mqttiumd_storage_mode durable`"
                    .to_owned(),
            );
            StorageMode::Durable
        }
        (Some("log"), Some(false)) => {
            acc.errors.push(
                "CFG-006: `persistence false` conflicts with `mqttiumd_storage_mode log`"
                    .to_owned(),
            );
            StorageMode::Log
        }
        (Some("log"), _) => {
            if acc.explicit_snapshot_interval.is_some() {
                acc.errors.push(
                    "mqttiumd_snapshot_interval requires `mqttiumd_storage_mode volatile`"
                        .to_owned(),
                );
            }
            StorageMode::Log
        }
        (Some("durable"), _) | (None, None | Some(true)) => {
            if acc.explicit_snapshot_interval.is_some() {
                acc.errors.push(
                    "mqttiumd_snapshot_interval requires `mqttiumd_storage_mode volatile`"
                        .to_owned(),
                );
            }
            StorageMode::Durable
        }
        (_, Some(false)) => {
            if acc.explicit_snapshot_interval.is_some_and(|secs| secs > 0) {
                acc.errors.push(
                    "CFG-006: `persistence false` disables disk persistence and conflicts with non-zero `mqttiumd_snapshot_interval`"
                        .to_owned(),
                );
            }
            if acc.require_existing_db {
                acc.errors.push(
                    "CFG-006: `persistence false` disables disk persistence and conflicts with `mqttiumd_existing_db true`"
                        .to_owned(),
                );
            }
            StorageMode::Volatile {
                snapshot_interval: None,
            }
        }
        (Some("volatile"), _) => {
            let secs = acc.explicit_snapshot_interval.or(acc.autosave_interval);
            let snapshot_interval = match secs {
                None | Some(0) => None,
                Some(n) => Some(Duration::from_secs(n)),
            };
            StorageMode::Volatile { snapshot_interval }
        }
        (Some(_), _) => StorageMode::Durable,
    };

    // Resolve database path.
    let db = if let Some(explicit) = acc.explicit_db.clone() {
        Some(explicit)
    } else if let Some(dir) = acc.persistence_location.clone() {
        let file = acc.persistence_file.as_deref().unwrap_or("mqttiumd.sqlite");
        Some(dir.join(file))
    } else {
        acc.persistence_file.clone().map(PathBuf::from)
    };

    let ok = !acc.has_blocking();
    let mut report = ConfigReport {
        ok,
        reports: acc.reports.clone(),
        errors: acc.errors.clone(),
        launch_requires: Vec::new(),
    };
    let parsed = if ok {
        let default_limits = if cfg!(feature = "test-hooks") {
            Limits::BOUNDED
        } else {
            Limits::DEFAULT
        };
        let limits = acc
            .max_connections
            .map(Limits::new)
            .unwrap_or(default_limits);
        let default_level = if cfg!(feature = "test-hooks") {
            Level::Audit
        } else {
            Level::Ops
        };
        Some(ParsedConfig {
            db,
            require_existing_db: acc.require_existing_db,
            storage_mode,
            listen,
            events: acc.events,
            event_level: acc.event_level.unwrap_or(default_level),
            limits,
            tls,
            io_threads: acc.io_threads,
            delivery_shards: acc.delivery_shards,
            reports: acc.reports,
        })
    } else {
        None
    };
    if let Some(parsed) = &parsed {
        report.launch_requires = parsed.launch_requires();
    }
    Ok((report, parsed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQ: AtomicU64 = AtomicU64::new(0);

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "mqttiumd-cfg-{name}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn valid_loopback_mosquitto_conf_parses_with_explicit_classifications() {
        let dir = temp_dir("valid-loopback");
        let conf = dir.join("mosquitto.conf");
        fs::write(
            &conf,
            "\
# Loopback Mosquitto profile
listener 1884 127.0.0.1
protocol mqtt
allow_anonymous true
max_connections 512
persistence true
persistence_location ./data
persistence_file broker.sqlite
autosave_interval 30
log_dest file ./events.jsonl
log_type notice
mqttiumd_io_threads 4
mqttiumd_delivery_shards 2
",
        )
        .unwrap();
        let (report, parsed) = check_file(&conf).unwrap();
        assert!(report.ok, "unexpected errors: {:?}", report.errors);
        let parsed = parsed.unwrap();
        assert_eq!(parsed.listen, "127.0.0.1:1884".parse().unwrap());
        assert_eq!(parsed.limits.connections, 512);
        assert_eq!(parsed.storage_mode, StorageMode::Durable);
        assert_eq!(parsed.db.unwrap(), dir.join("./data/broker.sqlite"));
        assert_eq!(parsed.events.unwrap(), dir.join("./events.jsonl"));
        assert_eq!(parsed.event_level, Level::Ops);
        assert_eq!(parsed.io_threads, Some(4));
        assert_eq!(parsed.delivery_shards, Some(2));
        assert!(
            report
                .reports
                .iter()
                .any(|r| r.directive == "persistence" && r.status == DirectiveStatus::Translated)
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn unknown_and_unsafe_directives_block_without_silent_degradation() {
        let dir = temp_dir("blocking");

        // 1. Unknown directive (CFG-001).
        let unknown = dir.join("unknown.conf");
        fs::write(
            &unknown,
            "listener 1883 127.0.0.1\nallow_anonymous true\nfrobnicate 1\n",
        )
        .unwrap();
        let (rep, parsed) = check_file(&unknown).unwrap();
        assert!(!rep.ok);
        assert!(parsed.is_none());
        assert!(
            rep.reports
                .iter()
                .any(|r| r.directive == "frobnicate" && r.status == DirectiveStatus::Blocking)
        );

        // 2. Non-loopback plain listener refused (SEC-005).
        let exposed = dir.join("exposed.conf");
        fs::write(&exposed, "listener 1883 0.0.0.0\nallow_anonymous true\n").unwrap();
        let (rep, parsed) = check_file(&exposed).unwrap();
        assert!(!rep.ok);
        assert!(parsed.is_none());

        // 3. TLS without require_certificate true refused (SEC-005).
        let weak_tls = dir.join("weak_tls.conf");
        fs::write(
            &weak_tls,
            "listener 8883 0.0.0.0\ncertfile s.pem\nkeyfile k.pem\ncafile ca.pem\nrequire_certificate false\n",
        )
        .unwrap();
        let (rep, parsed) = check_file(&weak_tls).unwrap();
        assert!(!rep.ok);
        assert!(parsed.is_none());

        // 4. password_file / acl_file / multi-listener refused (CFG-002).
        let multi = dir.join("multi.conf");
        fs::write(
            &multi,
            "listener 1883 127.0.0.1\nlistener 1884 127.0.0.1\npassword_file /etc/pw\n",
        )
        .unwrap();
        let (rep, parsed) = check_file(&multi).unwrap();
        assert!(!rep.ok);
        assert!(parsed.is_none());
        assert!(
            rep.reports
                .iter()
                .filter(|r| r.status == DirectiveStatus::Blocking)
                .count()
                >= 2
        );

        // 5. Explicit listener without allow_anonymous defaults to anonymous disabled (Mosquitto 2.x / SEC-005).
        let implicit_anon = dir.join("implicit_anon.conf");
        fs::write(&implicit_anon, "listener 1883 127.0.0.1\n").unwrap();
        let (rep, parsed) = check_file(&implicit_anon).unwrap();
        assert!(
            !rep.ok,
            "explicit listener without allow_anonymous must be refused"
        );
        assert!(parsed.is_none());

        // 6. Legacy port without allow_anonymous is likewise refused.
        let legacy_implicit = dir.join("legacy_implicit.conf");
        fs::write(&legacy_implicit, "port 1883\n").unwrap();
        let (rep, parsed) = check_file(&legacy_implicit).unwrap();
        assert!(!rep.ok);
        assert!(parsed.is_none());

        // 7. Contradictions with `persistence false` are refused.
        for (name, body) in [
            (
                "pf_durable.conf",
                "listener 1883 127.0.0.1\nallow_anonymous true\npersistence false\nmqttiumd_storage_mode durable\n",
            ),
            (
                "pf_snap.conf",
                "listener 1883 127.0.0.1\nallow_anonymous true\npersistence false\nmqttiumd_snapshot_interval 15\n",
            ),
            (
                "pf_existing.conf",
                "listener 1883 127.0.0.1\nallow_anonymous true\npersistence false\nmqttiumd_existing_db true\n",
            ),
        ] {
            let p = dir.join(name);
            fs::write(&p, body).unwrap();
            let (rep, parsed) = check_file(&p).unwrap();
            assert!(!rep.ok, "{name} must be refused");
            assert!(parsed.is_none());
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn persistence_false_ignores_autosave_interval_and_requires_no_db_path() {
        let dir = temp_dir("pf-autosave");
        let conf = dir.join("mosquitto.conf");
        fs::write(
            &conf,
            "\
listener 1886 127.0.0.1
allow_anonymous true
persistence false
autosave_interval 30
log_dest file ./events.jsonl
",
        )
        .unwrap();
        let (rep, parsed) = check_file(&conf).unwrap();
        assert!(rep.ok, "errors: {:?}", rep.errors);
        let parsed = parsed.unwrap();
        assert_eq!(
            parsed.storage_mode,
            StorageMode::Volatile {
                snapshot_interval: None
            }
        );
        let runtime_cfg = parsed.into_runtime_config(Hooks::default()).unwrap();
        assert_eq!(
            runtime_cfg.storage_mode,
            StorageMode::Volatile {
                snapshot_interval: None
            }
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn include_dir_loads_conf_files_in_lexicographical_order_and_volatile_mode_translates() {
        let dir = temp_dir("include");
        let inc = dir.join("conf.d");
        fs::create_dir_all(&inc).unwrap();
        fs::write(
            inc.join("01-base.conf"),
            "mqttiumd_storage_mode volatile\nautosave_interval 15\n",
        )
        .unwrap();
        fs::write(
            inc.join("02-net.conf"),
            "listener 1885 127.0.0.1\nallow_anonymous true\n",
        )
        .unwrap();
        fs::write(inc.join("ignored.txt"), "not a conf file\n").unwrap();
        let main = dir.join("main.conf");
        fs::write(
            &main,
            "include_dir conf.d\nmqttiumd_db ./state.sqlite\nmqttiumd_events ./ev.jsonl\n",
        )
        .unwrap();
        let (rep, parsed) = check_file(&main).unwrap();
        assert!(rep.ok, "errors: {:?}", rep.errors);
        let parsed = parsed.unwrap();
        assert_eq!(parsed.listen, "127.0.0.1:1885".parse().unwrap());
        assert_eq!(
            parsed.storage_mode,
            StorageMode::Volatile {
                snapshot_interval: Some(Duration::from_secs(15))
            }
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
