//! Shared contract of mqttiumd: identities, bounded quantities, store
//! outcomes, the synchronous step interface, effects and frozen commands.
//!
//! This crate is `no_std + alloc` and has no dependency, so it cannot reach
//! the network, the disk, the system clock, ambient randomness or an async
//! runtime (DEM-003, ADR 0007). It defines data and ownership rules only; the
//! transitions live in `mqttiumd-core`.
//!
//! Everything here is contract revision 1, strict but not frozen until the S0
//! reception (ADR 0004).

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

pub mod capacity;
pub mod command;
pub mod decimal;
pub mod effects;
pub mod ids;
pub mod outcome;
pub mod packet;
pub mod step;
