//! Synchronous deterministic core of mqttiumd.
//!
//! This crate is `no_std + alloc` and depends on `mqttiumd-contract` only,
//! so it cannot reach the network, the disk, the system clock, ambient
//! randomness or an async runtime (DEM-003, ADR 0007). The same code runs in
//! simulation and against SQLite; no `cfg` may replace a business decision.
//!
//! Lot L0 creates the crate so that the boundary is enforced from the first
//! commit. Transitions arrive with lot L1, in the plan-then-apply shape of
//! ADR 0006.

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

pub use mqttiumd_contract as contract;

pub mod command;
pub mod credits;
pub mod leases;
