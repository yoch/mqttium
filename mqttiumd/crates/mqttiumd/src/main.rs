//! Experimental incoming-only MQTT 5 TCP broker.
use mqttiumd::{
    runtime::{Config, run},
    writer::Hooks,
};
use std::path::PathBuf;
fn arguments() -> Result<Config, &'static str> {
    let mut db = None;
    let mut listen = "127.0.0.1:1883".parse().map_err(|_| "listen")?;
    let mut events = None;
    let mut hooks = Hooks::default();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let value = args.next().ok_or("missing argument value")?;
        match arg.as_str() {
            "--db" => db = Some(PathBuf::from(value)),
            "--listen" => listen = value.parse().map_err(|_| "invalid listen address")?,
            "--events" => events = Some(PathBuf::from(value)),
            "--test-control" if cfg!(feature = "test-hooks") => {
                hooks.directory = Some(PathBuf::from(value))
            }
            "--test-pause" if cfg!(feature = "test-hooks") => {
                for point in value.split(',') {
                    if ![
                        "admission.prepared",
                        "release.prepared",
                        "admission.committed",
                        "ack.receipt",
                        "subscription.prepared",
                        "claim.prepared",
                        "finish.prepared",
                        "claim.committed",
                        "finish.committed",
                    ]
                    .contains(&point)
                    {
                        return Err("unknown test point");
                    }
                    hooks.pause.push(point.to_owned());
                }
            }
            _ => return Err("unknown or unavailable option"),
        }
    }
    if !hooks.pause.is_empty() && hooks.directory.is_none() {
        return Err("test pause requires --test-control");
    }
    if let Some(dir) = &hooks.directory
        && !dir.is_dir()
    {
        return Err("test control must be an existing directory");
    }
    Ok(Config {
        db: db.ok_or("--db is required")?,
        listen,
        events: events.ok_or("--events is required")?,
        hooks,
    })
}
fn main() {
    let config = match arguments() {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "mqttiumd: {e}\nusage: mqttiumd --db PATH --listen 127.0.0.1:1883 --events NEW_JSONL_PATH"
            );
            std::process::exit(2);
        }
    };
    if let Err(failure) = run(config) {
        eprintln!("mqttiumd refused: {}", failure.fields());
        std::process::exit(2);
    }
}
