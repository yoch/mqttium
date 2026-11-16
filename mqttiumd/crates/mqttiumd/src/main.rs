//! Experimental durable exact-topic MQTT 5 TCP broker.
use mqttiumd::{
    runtime::{Config, run},
    writer::Hooks,
};
use std::path::PathBuf;
fn arguments() -> Result<Config, &'static str> {
    parse_arguments(std::env::args().skip(1))
}
fn parse_arguments(mut args: impl Iterator<Item = String>) -> Result<Config, &'static str> {
    let mut db = None;
    let mut listen = "127.0.0.1:1883".parse().map_err(|_| "listen")?;
    let mut events = None;
    let mut hooks = Hooks::default();
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
                        "consume.prepared",
                        "consume.committed",
                        "consume.emission",
                        "receipt.close.prepared",
                        "receipt.close.committed",
                        "session.open.prepared",
                        "session.open.committed",
                        "session.close.prepared",
                        "session.close.committed",
                    ]
                    .contains(&point)
                    {
                        return Err("unknown test point");
                    }
                    // Only declared unique points enter the bounded control set.
                    if !hooks.pause.iter().any(|known| known == point) {
                        hooks.pause.push(point.to_owned());
                    }
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
            assert!(matches!(
                parse_arguments(arguments("session.open.unknown".to_owned()).into_iter()),
                Err("unknown test point")
            ));
        } else {
            assert!(matches!(result, Err("unknown or unavailable option")));
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
            assert!(matches!(result, Err("unknown or unavailable option")));
        }
    }
}
