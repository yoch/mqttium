//! Operator-only offline subcommand. No runtime, listener, or MQTT input.

use crate::security::{Policy, bounded_file, parse_fingerprint};
use mqttiumd_store_sqlite::{
    PolicyInventory, PolicyMaintenanceError, PolicyTransition, PolicyTransitionPoint,
    SECURE_SCHEMA_VERSION, SqliteStore,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

const PLAN_FORMAT: &str = "mqttiumd.policy-plan.v1";
const PLAN_BYTES: usize = 2 * 1024 * 1024;

fn hex(hash: [u8; 32]) -> String {
    hash.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct State {
    sha256: String,
    incarnation: String,
    revision: String,
    counts: BTreeMap<String, usize>,
}

impl State {
    fn decode(&self) -> Option<PolicyInventory> {
        let incarnation: u64 = self.incarnation.parse().ok()?;
        let revision: u64 = self.revision.parse().ok()?;
        if self.incarnation != incarnation.to_string() || self.revision != revision.to_string() {
            return None;
        }
        Some(PolicyInventory {
            sha256: parse_fingerprint(&self.sha256).ok()?,
            incarnation,
            revision,
            counts: self.counts.clone(),
        })
    }
}

impl From<PolicyInventory> for State {
    fn from(state: PolicyInventory) -> Self {
        Self {
            sha256: hex(state.sha256),
            incarnation: state.incarnation.to_string(),
            revision: state.revision.to_string(),
            counts: state.counts,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Authority {
    authorization_sha256: String,
    configured_credentials_sha256: String,
}

impl From<&Policy> for Authority {
    fn from(policy: &Policy) -> Self {
        Self {
            authorization_sha256: hex(policy.fingerprint()),
            configured_credentials_sha256: hex(policy.credential_fingerprint()),
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct PlanBody {
    sqlite_schema: i64,
    old: Authority,
    new: Authority,
    business: State,
    applicable: bool,
    changes: Value,
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Plan {
    format: String,
    sha256: String,
    body: PlanBody,
}

impl Plan {
    fn new(old: &Policy, new: &Policy, business: State) -> Self {
        let changes = old.changes(new);
        let body = PlanBody {
            sqlite_schema: SECURE_SCHEMA_VERSION,
            old: old.into(),
            new: new.into(),
            business,
            applicable: changes.refusals.is_empty(),
            changes: json!(changes),
        };
        let mut digest = Sha256::new();
        digest.update(PLAN_FORMAT.as_bytes());
        digest.update([0]);
        digest.update(serde_json::to_vec(&body).expect("bounded JSON values"));
        Self {
            format: PLAN_FORMAT.to_owned(),
            sha256: hex(digest.finalize().into()),
            body,
        }
    }

    fn bytes(&self) -> Vec<u8> {
        let mut bytes = serde_json::to_vec_pretty(self).expect("bounded JSON values");
        bytes.push(b'\n');
        bytes
    }

    fn load(path: &Path, old: &Policy, new: &Policy) -> Result<Self, Value> {
        let invalid = || refusal("invalid_plan");
        let raw = bounded_file(path, PLAN_BYTES).map_err(|_| invalid())?;
        let plan: Self = serde_json::from_slice(&raw).map_err(|_| invalid())?;
        if plan != Self::new(old, new, plan.body.business.clone()) || plan.bytes() != raw {
            return Err(invalid());
        }
        Ok(plan)
    }

    /// The answer of `plan` once the plan is written: Planned with its
    /// changes, or Refused with `non_monotone_policy_change` when it cannot
    /// be applied.
    fn planned(self) -> Result<Value, Value> {
        if !self.body.applicable {
            let mut result = self.summary("Refused");
            result["code"] = json!("non_monotone_policy_change");
            result["changes"] = self.body.changes;
            return Err(result);
        }
        let mut result = self.summary("Planned");
        result["changes"] = self.body.changes;
        Ok(result)
    }

    /// The business state `apply` expects to find: refused with its
    /// changes when the plan cannot be applied, `invalid_plan` when it does
    /// not decode.
    fn expected_state(&self) -> Result<PolicyInventory, Value> {
        if !self.body.applicable {
            let mut result = refusal("non_monotone_policy_change");
            result["changes"] = self.body.changes.clone();
            return Err(result);
        }
        self.body
            .business
            .decode()
            .ok_or_else(|| refusal("invalid_plan"))
    }

    fn summary(&self, status: &str) -> Value {
        json!({
            "status": status,
            "plan_sha256": self.sha256,
            "old_policy_sha256": self.body.old.authorization_sha256,
            "new_policy_sha256": self.body.new.authorization_sha256,
            "business": self.body.business,
        })
    }
}

fn refusal(code: &'static str) -> Value {
    json!({"status":"Refused","code":code})
}

fn maintenance_error(error: PolicyMaintenanceError) -> Value {
    let mut outcome = refusal(error.code());
    match error {
        PolicyMaintenanceError::Read(cause) => outcome["cause"] = json!(cause.code()),
        PolicyMaintenanceError::WrongSchema(found) => outcome["schema_found"] = json!(found),
        PolicyMaintenanceError::CommitUnknown | PolicyMaintenanceError::ReadbackFailed => {
            outcome["status"] = json!("Unknown");
        }
        _ => {}
    }
    outcome
}

fn load_policy(path: &Path, invalid: &'static str) -> Result<Policy, Value> {
    Policy::read(path).map_err(|cause| {
        let code = if matches!(cause, "policy_capacity_exceeded" | "security_file_budget") {
            "capacity_exceeded"
        } else {
            invalid
        };
        let mut outcome = refusal(code);
        outcome["cause"] = json!(cause);
        outcome
    })
}

struct Options {
    db: PathBuf,
    old: PathBuf,
    new: PathBuf,
    action: Action,
}

enum Action {
    Plan {
        out: PathBuf,
    },
    Apply {
        plan: PathBuf,
        backup: PathBuf,
        #[cfg(feature = "test-hooks")]
        barrier: Option<(PathBuf, String)>,
    },
}

impl Options {
    fn parse(mut args: impl Iterator<Item = String>) -> Option<Self> {
        let verb = args.next()?;
        let mut flags = BTreeMap::new();
        while let Some(name) = args.next() {
            if flags.insert(name, args.next()?).is_some() {
                return None;
            }
        }
        let db = flags.remove("--db")?.into();
        let old = flags.remove("--old-identities")?.into();
        let new = flags.remove("--new-identities")?.into();
        let action = match verb.as_str() {
            "plan" => Action::Plan {
                out: flags.remove("--out")?.into(),
            },
            "apply" => Action::Apply {
                plan: flags.remove("--plan")?.into(),
                backup: flags.remove("--backup")?.into(),
                #[cfg(feature = "test-hooks")]
                barrier: match (flags.remove("--test-control"), flags.remove("--test-pause")) {
                    (None, None) => None,
                    (Some(dir), Some(point))
                        if Path::new(&dir).is_dir()
                            && matches!(
                                point.as_str(),
                                "policy.fingerprint_staged" | "policy.committed"
                            ) =>
                    {
                        Some((dir.into(), point))
                    }
                    _ => return None,
                },
            },
            _ => return None,
        };
        flags.is_empty().then_some(Self {
            db,
            old,
            new,
            action,
        })
    }
}

#[cfg(feature = "test-hooks")]
fn observe(point: PolicyTransitionPoint, barrier: Option<&(PathBuf, String)>) {
    use std::{
        fs, thread,
        time::{Duration, Instant},
    };
    let name = match point {
        PolicyTransitionPoint::FingerprintStaged => "policy.fingerprint_staged",
        PolicyTransitionPoint::Committed => "policy.committed",
    };
    if let Some((directory, stop)) = barrier
        && name == stop
    {
        let ready = directory.join(format!("ready-{name}"));
        let release = directory.join(format!("release-{name}"));
        if fs::write(ready, b"reached\n").is_err() {
            std::process::exit(3);
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        while !release.is_file() {
            if Instant::now() >= deadline {
                std::process::exit(3);
            }
            thread::sleep(Duration::from_millis(2));
        }
    }
}

fn execute(options: Options) -> Result<Value, Value> {
    let old = load_policy(&options.old, "invalid_old_policy")?;
    let new = load_policy(&options.new, "invalid_new_policy")?;
    match options.action {
        Action::Plan { out } => {
            let state = SqliteStore::inspect_policy(&options.db, &old.storage_policy())
                .map_err(maintenance_error)?;
            let plan = Plan::new(&old, &new, state.into());
            let out = SqliteStore::policy_output_path(&options.db, &out)
                .map_err(|_| refusal("plan_output_failed"))?;
            let bytes = plan.bytes();
            if bytes.len() > PLAN_BYTES {
                return Err(refusal("capacity_exceeded"));
            }
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(out)
                .map_err(|_| refusal("plan_output_failed"))?;
            file.write_all(&bytes)
                .and_then(|()| file.sync_all())
                .map_err(|_| refusal("plan_output_failed"))?;
            plan.planned()
        }
        Action::Apply {
            plan,
            backup,
            #[cfg(feature = "test-hooks")]
            barrier,
        } => {
            let plan = Plan::load(&plan, &old, &new)?;
            let state = plan.expected_state()?;
            let result = SqliteStore::apply_policy(
                &options.db,
                &old.storage_policy(),
                &new.storage_policy(),
                &state,
                &backup,
                |_point: PolicyTransitionPoint| {
                    #[cfg(feature = "test-hooks")]
                    observe(_point, barrier.as_ref());
                },
            )
            .map_err(maintenance_error)?;
            Ok(match result {
                PolicyTransition::Applied { backup_sha256 } => {
                    let mut result = plan.summary("Applied");
                    result["backup_sha256"] = json!(hex(backup_sha256));
                    result
                }
                PolicyTransition::AlreadyApplied => plan.summary("AlreadyApplied"),
                PolicyTransition::Unchanged => plan.summary("Unchanged"),
            })
        }
    }
}

pub fn run(args: impl Iterator<Item = String>) -> i32 {
    let Some(options) = Options::parse(args) else {
        eprintln!(
            "usage: mqttiumd policy plan|apply --db DATA --old-identities OLD.json --new-identities NEW.json --out PLAN.json (plan) | --plan PLAN.json --backup PRIVATE_DIRECTORY/BACKUP.sqlite (apply)"
        );
        println!("{}", refusal("invalid_arguments"));
        return 2;
    };
    let (result, exit) = match execute(options) {
        Ok(value) => (value, 0),
        Err(value) => (value, 2),
    };
    println!("{result}");
    exit
}
