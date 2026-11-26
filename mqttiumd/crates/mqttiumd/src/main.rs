//! Experimental durable hierarchical MQTT TCP broker.
use mqttiumd::{
    config,
    runtime::{Config, Limits, run},
    security::{Security, TlsFiles},
    writer::{self, Hooks, StorageMode},
};
use std::path::{Path, PathBuf};
use std::time::Duration;

fn parse_arguments(args_iter: impl Iterator<Item = String>) -> Result<Config, String> {
    let raw: Vec<String> = args_iter.collect();
    // First pass: check for `-c` / `--config PATH` so explicit CLI flags can
    // override or complete the file configuration cleanly.
    let mut config_path: Option<PathBuf> = None;
    let mut i = 0;
    while i < raw.len() {
        let arg = &raw[i];
        if arg == "-c" || arg == "--config" {
            let value = raw
                .get(i + 1)
                .ok_or_else(|| "missing argument value for --config".to_owned())?;
            if config_path.is_some() {
                return Err("duplicate --config option".to_owned());
            }
            config_path = Some(PathBuf::from(value));
            i += 2;
        } else {
            i += 2;
        }
    }

    let loaded = if let Some(path) = &config_path {
        let (report, parsed) = config::check_file(path)?;
        let Some(parsed) = parsed else {
            let mut details = Vec::new();
            for r in report
                .reports
                .iter()
                .filter(|r| r.status == config::DirectiveStatus::Blocking)
            {
                details.push(format!(
                    "{}:{}: `{}`: {}",
                    r.file.display(),
                    r.line,
                    r.directive,
                    r.reason
                ));
            }
            details.extend(report.errors);
            return Err(format!("configuration refused: {}", details.join("; ")));
        };
        Some(parsed)
    } else {
        None
    };

    let mut db = loaded.as_ref().and_then(|c| c.db.clone());
    let mut require_existing_db = loaded.as_ref().is_some_and(|c| c.require_existing_db);
    let mut storage_mode_from_file = loaded.as_ref().map(|c| c.storage_mode);
    let mut storage_mode_name: Option<String> = None;
    let mut snapshot_interval_secs: Option<u64> = None;
    let mut listen = loaded
        .as_ref()
        .map(|c| c.listen)
        .unwrap_or_else(|| "127.0.0.1:1883".parse().expect("valid default listen"));
    let mut events = loaded.as_ref().and_then(|c| c.events.clone());
    let mut event_level = loaded.as_ref().map(|c| c.event_level).unwrap_or_else(|| {
        // The instrumented build serves receptions that read every record.
        if cfg!(feature = "test-hooks") {
            mqttiumd::events::Level::Audit
        } else {
            mqttiumd::events::Level::Ops
        }
    });
    let mut limits = loaded.as_ref().map(|c| c.limits).unwrap_or_else(|| {
        // The instrumented build keeps the bounded profile its receptions state.
        if cfg!(feature = "test-hooks") {
            Limits::BOUNDED
        } else {
            Limits::DEFAULT
        }
    });
    let mut io_threads = loaded.as_ref().and_then(|c| c.io_threads);
    let mut delivery_shards = loaded.as_ref().and_then(|c| c.delivery_shards).unwrap_or(0);
    let mut hooks = Hooks::default();
    let (mut cert, mut key, mut client_ca, mut identities) = match loaded.and_then(|c| c.tls) {
        Some(tls) => (
            Some(tls.cert),
            Some(tls.key),
            Some(tls.client_ca),
            Some(tls.identities),
        ),
        None => (None, None, None, None),
    };
    let mut db_cli_set = false;
    let mut existing_db_cli_set = false;

    let mut args = raw.into_iter();
    while let Some(arg) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| "missing argument value".to_owned())?;
        match arg.as_str() {
            "-c" | "--config" => {}
            "--db" if !existing_db_cli_set => {
                db = Some(PathBuf::from(value));
                require_existing_db = false;
                db_cli_set = true;
            }
            "--existing-db" if !db_cli_set => {
                db = Some(PathBuf::from(value));
                require_existing_db = true;
                existing_db_cli_set = true;
            }
            "--storage-mode" if storage_mode_name.is_none() => {
                storage_mode_name = Some(value);
                storage_mode_from_file = None;
            }
            "--snapshot-interval" if snapshot_interval_secs.is_none() => {
                let secs: u64 = value
                    .parse()
                    .ok()
                    .filter(|&secs| secs <= 86_400)
                    .ok_or_else(|| "invalid snapshot interval".to_owned())?;
                snapshot_interval_secs = Some(secs);
            }
            "--listen" => {
                listen = value
                    .parse()
                    .map_err(|_| "invalid listen address".to_owned())?;
            }
            "--events" => events = Some(PathBuf::from(value)),
            "--event-level" => {
                event_level = value
                    .parse()
                    .map_err(|()| "invalid event level".to_owned())?;
            }
            "--max-connections" => {
                limits = value
                    .parse()
                    .ok()
                    .filter(|count| (1..=Limits::MAX_CONNECTIONS).contains(count))
                    .map(Limits::new)
                    .ok_or_else(|| "invalid connection limit".to_owned())?;
            }
            "--io-threads" => {
                let threads: usize = value
                    .parse()
                    .ok()
                    .filter(|count| (1..=32).contains(count))
                    .ok_or_else(|| "invalid io thread count".to_owned())?;
                io_threads = Some(threads);
            }
            "--delivery-shards" => {
                delivery_shards = value
                    .parse()
                    .ok()
                    .filter(|count| *count <= mqttiumd::delivery_shard::MAX_DELIVERY_SHARDS)
                    .ok_or_else(|| "invalid delivery shard count".to_owned())?;
            }
            "--tls-cert" if cert.is_none() => cert = Some(PathBuf::from(value)),
            "--tls-key" if key.is_none() => key = Some(PathBuf::from(value)),
            "--client-ca" if client_ca.is_none() => client_ca = Some(PathBuf::from(value)),
            "--identities" if identities.is_none() => identities = Some(PathBuf::from(value)),
            "--test-control" if cfg!(feature = "test-hooks") => {
                hooks.directory = Some(PathBuf::from(value))
            }
            "--test-pause" if cfg!(feature = "test-hooks") => {
                for point in value.split(',') {
                    if ![
                        "admission.prepared",
                        "retained.prepared",
                        "retained.committed",
                        "subscription.committed",
                        "release.prepared",
                        "admission.committed",
                        "ack.receipt",
                        "subscription.prepared",
                        "claim.prepared",
                        "finish.prepared",
                        "claim.committed",
                        "finish.committed",
                        "consume.prepared",
                        "consume.committed",
                        "consume.emission",
                        "publish.emission",
                        "delivery.started.prepared",
                        "delivery.started.committed",
                        "message.delivery.expire.prepared",
                        "message.delivery.expire.committed",
                        "message.expire.prepared",
                        "message.expire.committed",
                        "receipt.close.prepared",
                        "receipt.close.committed",
                        "work.read.before",
                        "tls.frame.sealed",
                        "tls.frame.first_bytes",
                        "will.admission.prepared",
                        "will.admission.committed",
                        "will.end.prepared",
                        "will.end.logged",
                        "log.will.end.before_append",
                        "log.will.end.before_sync",
                        "log.will.end.after_sync",
                        "volatile.will.end.before_apply",
                        "will.end.committed",
                        "session.open.prepared",
                        "session.open.committed",
                        "session.close.prepared",
                        "session.close.committed",
                        "session.detach.prepared",
                        "session.detach.committed",
                        "session.expire.prepared",
                        "session.expire.committed",
                    ]
                    .contains(&point)
                    {
                        return Err("unknown test point".to_owned());
                    }
                    // Only declared unique points enter the bounded control set.
                    if !hooks.pause.iter().any(|known| known == point) {
                        hooks.pause.push(point.to_owned());
                    }
                }
            }
            // Reached only when the guard of the option's own arm refused it.
            "--db" | "--existing-db" => {
                return Err("--db and --existing-db exclude each other".to_owned());
            }
            "--storage-mode"
            | "--snapshot-interval"
            | "--tls-cert"
            | "--tls-key"
            | "--client-ca"
            | "--identities" => {
                return Err(format!("{arg} is given more than once"));
            }
            _ => return Err("unknown or unavailable option".to_owned()),
        }
    }

    if let Some(threads) = io_threads {
        mqttiumd::net::configure_io_threads(threads);
    }

    let storage_mode = if let Some(mode) = storage_mode_from_file {
        match (mode, snapshot_interval_secs) {
            (StorageMode::Durable | StorageMode::Log, Some(_)) => {
                return Err("--snapshot-interval requires --storage-mode volatile".to_owned());
            }
            (StorageMode::Volatile { .. }, Some(0)) => StorageMode::Volatile {
                snapshot_interval: None,
            },
            (StorageMode::Volatile { .. }, Some(secs)) => StorageMode::Volatile {
                snapshot_interval: Some(Duration::from_secs(secs)),
            },
            (other, None) => other,
        }
    } else {
        match storage_mode_name.as_deref() {
            None | Some("durable") => {
                if snapshot_interval_secs.is_some() {
                    return Err("--snapshot-interval requires --storage-mode volatile".to_owned());
                }
                StorageMode::Durable
            }
            Some("log") => {
                if snapshot_interval_secs.is_some() {
                    return Err("--snapshot-interval requires --storage-mode volatile".to_owned());
                }
                StorageMode::Log
            }
            Some("volatile") => {
                let snapshot_interval = match snapshot_interval_secs {
                    None | Some(0) => None,
                    Some(secs) => Some(Duration::from_secs(secs)),
                };
                StorageMode::Volatile { snapshot_interval }
            }
            Some(_) => {
                return Err("invalid storage mode (expected durable, log, or volatile)".to_owned());
            }
        }
    };
    if !hooks.pause.is_empty() && hooks.directory.is_none() {
        return Err("test pause requires --test-control".to_owned());
    }
    if let Some(dir) = &hooks.directory
        && !dir.is_dir()
    {
        return Err("test control must be an existing directory".to_owned());
    }
    let needs_db = matches!(
        storage_mode,
        StorageMode::Durable
            | StorageMode::Log
            | StorageMode::Volatile {
                snapshot_interval: Some(_),
            }
    ) || require_existing_db;
    let db = if needs_db {
        db.ok_or_else(|| "--db or --existing-db is required".to_owned())?
    } else {
        db.unwrap_or_default()
    };
    Ok(Config {
        db,
        require_existing_db,
        storage_mode,
        listen,
        events: events.ok_or_else(|| "--events is required".to_owned())?,
        event_level,
        limits,
        hooks,
        tls: match (cert, key, client_ca, identities) {
            (None, None, None, None) => None,
            (Some(cert), Some(key), Some(client_ca), Some(identities)) => Some(TlsFiles {
                cert,
                key,
                client_ca,
                identities,
            }),
            _ => {
                return Err(
                    "TLS requires --tls-cert, --tls-key, --client-ca and --identities".to_owned(),
                );
            }
        },
        delivery_shards,
    })
}

fn main() {
    let mut args = std::env::args().skip(1).peekable();
    if args.peek().is_some_and(|arg| arg == "check-config") {
        let paths: Vec<String> = args.skip(1).collect();
        let [path] = paths.as_slice() else {
            eprintln!("usage: mqttiumd check-config CONFIG_PATH");
            std::process::exit(2);
        };
        match config::check_file(Path::new(path)) {
            Ok((report, _)) => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&report.to_json())
                        .unwrap_or_else(|_| "{\"ok\":false}".to_owned())
                );
                if !report.ok {
                    std::process::exit(2);
                }
            }
            Err(error) => {
                eprintln!("mqttiumd check-config refused: {error}");
                std::process::exit(2);
            }
        }
        return;
    }
    if args.peek().is_some_and(|arg| arg == "initialize-secure-db") {
        let paths = args.skip(1).map(PathBuf::from).collect::<Vec<_>>();
        let [db, cert, key, client_ca, identities] = paths.as_slice() else {
            eprintln!("usage: mqttiumd initialize-secure-db DB CERT KEY CLIENT_CA IDENTITIES");
            std::process::exit(2);
        };
        let files = TlsFiles {
            cert: cert.clone(),
            key: key.clone(),
            client_ca: client_ca.clone(),
            identities: identities.clone(),
        };
        let result = Security::load(&files)
            .map_err(writer::Failure::plain)
            .and_then(|security| writer::initialize_secured(db, security.policy.storage()));
        if let Err(error) = result {
            eprintln!("{}", error.fields());
            std::process::exit(2);
        }
        return;
    }
    if args.peek().is_some_and(|arg| arg == "check-security") {
        let paths = args.skip(1).map(PathBuf::from).collect::<Vec<_>>();
        let [cert, key, client_ca, identities] = paths.as_slice() else {
            eprintln!("usage: mqttiumd check-security CERT KEY CLIENT_CA IDENTITIES");
            std::process::exit(2);
        };
        let files = TlsFiles {
            cert: cert.clone(),
            key: key.clone(),
            client_ca: client_ca.clone(),
            identities: identities.clone(),
        };
        if let Err(reason) = Security::load(&files) {
            eprintln!("mqttiumd refused: {reason}");
            std::process::exit(2);
        }
        return;
    }
    if args.peek().is_some_and(|arg| arg == "policy") {
        std::process::exit(mqttiumd::policy_transition::run(args.skip(1)));
    }
    let config = match parse_arguments(args) {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "mqttiumd: {e}\nusage: mqttiumd [-c CONFIG_PATH] (--db PATH | --existing-db PATH) --listen ADDRESS --events NEW_JSONL_PATH [--storage-mode durable|log|volatile] [--snapshot-interval SECS] [--io-threads N] [--delivery-shards N] [--max-connections N] [--event-level audit|ops] [--tls-cert PATH --tls-key PATH --client-ca PATH --identities PATH]"
            );
            std::process::exit(2);
        }
    };
    if let Err(failure) = run(config) {
        eprintln!("mqttiumd refused: {}", failure.fields());
        std::process::exit(2);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_points_are_bounded_and_instrumented_only() {
        let points = [
            "session.open.prepared",
            "session.open.committed",
            "session.close.prepared",
            "session.close.committed",
            "session.detach.prepared",
            "session.detach.committed",
            "session.expire.prepared",
            "session.expire.committed",
            "work.read.before",
        ];
        let arguments = |value: String| {
            [
                "--db".to_owned(),
                "unused.sqlite".to_owned(),
                "--events".to_owned(),
                "unused.jsonl".to_owned(),
                "--test-control".to_owned(),
                std::env::temp_dir().to_string_lossy().into_owned(),
                "--test-pause".to_owned(),
                value,
            ]
        };
        let repeated = (0..128).flat_map(|_| points).collect::<Vec<_>>().join(",");
        let result = parse_arguments(arguments(repeated).into_iter());
        if cfg!(feature = "test-hooks") {
            assert_eq!(result.unwrap().hooks.pause, points);
            assert_eq!(
                parse_arguments(arguments("session.open.unknown".to_owned()).into_iter())
                    .err()
                    .as_deref(),
                Some("unknown test point")
            );
        } else {
            assert_eq!(
                result.err().as_deref(),
                Some("unknown or unavailable option")
            );
        }
    }

    #[test]
    fn receipt_closure_points_are_available_only_in_the_instrumented_cli() {
        let args = [
            "--db".to_owned(),
            "unused.sqlite".to_owned(),
            "--events".to_owned(),
            "unused.jsonl".to_owned(),
            "--test-control".to_owned(),
            std::env::temp_dir().to_string_lossy().into_owned(),
            "--test-pause".to_owned(),
            "receipt.close.prepared,receipt.close.committed".to_owned(),
        ];
        let result = parse_arguments(args.into_iter());
        if cfg!(feature = "test-hooks") {
            let config = result.unwrap_or_else(|error| panic!("receipt point refused: {error}"));
            assert_eq!(
                config.hooks.pause,
                ["receipt.close.prepared", "receipt.close.committed"]
            );
        } else {
            assert_eq!(
                result.err().as_deref(),
                Some("unknown or unavailable option")
            );
        }
    }

    /// An option the parser knows but refuses here is named in the refusal,
    /// not reported as unknown.
    #[test]
    fn a_known_option_refused_here_is_named() {
        let refusal = |args: &[&str]| {
            parse_arguments(args.iter().map(|arg| (*arg).to_owned()))
                .err()
                .unwrap_or_default()
        };
        assert_eq!(
            refusal(&["--db", "a.sqlite", "--existing-db", "b.sqlite"]),
            "--db and --existing-db exclude each other"
        );
        assert_eq!(
            refusal(&["--existing-db", "a.sqlite", "--db", "b.sqlite"]),
            "--db and --existing-db exclude each other"
        );
        for option in [
            "--storage-mode",
            "--snapshot-interval",
            "--tls-cert",
            "--tls-key",
            "--client-ca",
            "--identities",
        ] {
            let value = if option == "--snapshot-interval" {
                "5"
            } else {
                "durable"
            };
            assert_eq!(
                refusal(&["--db", "a.sqlite", option, value, option, value]),
                format!("{option} is given more than once")
            );
        }
        assert_eq!(
            refusal(&["--db", "a.sqlite", "--no-such-option", "x"]),
            "unknown or unavailable option"
        );
    }

    #[test]
    fn storage_mode_cli_is_explicit_and_never_degrades_silently() {
        let base = [
            "--db".to_owned(),
            "unused.sqlite".to_owned(),
            "--events".to_owned(),
            "unused.jsonl".to_owned(),
        ];
        // Default is always Durable (DUR-002).
        let default_cfg = parse_arguments(base.clone().into_iter()).unwrap();
        assert_eq!(default_cfg.storage_mode, StorageMode::Durable);

        // --snapshot-interval without --storage-mode volatile is refused.
        let mut bad_interval = base.to_vec();
        bad_interval.extend(["--snapshot-interval".to_owned(), "10".to_owned()]);
        assert!(parse_arguments(bad_interval.into_iter()).is_err());

        // Volatile pure RAM (no snapshots).
        let mut vol_ram = base.to_vec();
        vol_ram.extend(["--storage-mode".to_owned(), "volatile".to_owned()]);
        assert_eq!(
            parse_arguments(vol_ram.into_iter()).unwrap().storage_mode,
            StorageMode::Volatile {
                snapshot_interval: None
            }
        );

        // Volatile with periodic snapshot interval.
        let mut vol_snap = base.to_vec();
        vol_snap.extend([
            "--storage-mode".to_owned(),
            "volatile".to_owned(),
            "--snapshot-interval".to_owned(),
            "30".to_owned(),
        ]);
        assert_eq!(
            parse_arguments(vol_snap.into_iter()).unwrap().storage_mode,
            StorageMode::Volatile {
                snapshot_interval: Some(Duration::from_secs(30))
            }
        );

        // Log-first durable mode (--storage-mode log).
        let mut log_mode = base.to_vec();
        log_mode.extend(["--storage-mode".to_owned(), "log".to_owned()]);
        assert_eq!(
            parse_arguments(log_mode.into_iter()).unwrap().storage_mode,
            StorageMode::Log
        );
        let mut bad_log_interval = base.to_vec();
        bad_log_interval.extend([
            "--storage-mode".to_owned(),
            "log".to_owned(),
            "--snapshot-interval".to_owned(),
            "10".to_owned(),
        ]);
        assert!(parse_arguments(bad_log_interval.into_iter()).is_err());

        // Unknown mode refused.
        let mut bad_mode = base.to_vec();
        bad_mode.extend(["--storage-mode".to_owned(), "memory".to_owned()]);
        assert!(parse_arguments(bad_mode.into_iter()).is_err());

        // Delivery shards 0..=16 accepted, >16 refused.
        assert_eq!(default_cfg.delivery_shards, 0);
        let mut sharded = base.to_vec();
        sharded.extend(["--delivery-shards".to_owned(), "2".to_owned()]);
        assert_eq!(
            parse_arguments(sharded.into_iter())
                .unwrap()
                .delivery_shards,
            2
        );
        let mut bad_shards = base.to_vec();
        bad_shards.extend(["--delivery-shards".to_owned(), "17".to_owned()]);
        assert!(parse_arguments(bad_shards.into_iter()).is_err());
    }
}
