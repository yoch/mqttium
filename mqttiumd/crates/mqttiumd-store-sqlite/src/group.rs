//! Group commit (record 0045): several commands in one transaction.

use mqttiumd_contract::command::Command;
use mqttiumd_store_api::Applied;

use crate::SqliteStore;

/// The largest number of commands one group holds (ARC-020: batches are
/// bounded).
pub const MAX_GROUP: usize = 64;

/// Why a group was not started. Nothing was read and nothing written; the
/// commands stay with the caller, untouched.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum GroupRefused {
    /// More commands than `MAX_GROUP`.
    TooLarge { commands: usize, max: usize },
}

impl SqliteStore {
    /// Not implemented yet: applies nothing and answers nothing.
    pub fn apply_group(&mut self, commands: &[&Command]) -> Result<Vec<Applied>, GroupRefused> {
        let _ = commands;
        Ok(Vec::new())
    }
}
