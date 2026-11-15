//! Bench of mqttiumd.
//!
//! Lot L0 provides contract validation only: strict JSON reading, offline
//! JSON Schema 2020-12 validation, a semantic pass over references, limits
//! and coverage, and the skeleton of a run manifest. The deterministic
//! store, the scheduler and the injections arrive with later lots.

#![forbid(unsafe_code)]

pub mod bench;
pub mod diagnostic;
pub mod files;
pub mod manifest;
pub mod schemas;
pub mod semantic;
pub mod store;
pub mod strict;
pub mod workspace;
