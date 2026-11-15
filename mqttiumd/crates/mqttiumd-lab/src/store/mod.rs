//! The deterministic store of the bench (lot L2).
//!
//! - `durable`: the durable state, the commands and the writer interface
//!   `DurableStore`, with its in-memory realisation `MemoryStore`.
//! - `adapter`: `SimStore`, the volatile layer around a writer: commands
//!   in transit, ready results, the named injection points and the
//!   correlation of results (ADR 0003).
//! - `fixture`: a fixture materialised through store commands, never by
//!   writing state.
//! - `evidence`: the store snapshot and the trace records of the adapter,
//!   in the formats of lot L3.
//! - `conformance`: the contract tests, written against `DurableStore`
//!   so that the SQLite store of lot L7 passes them unchanged.

pub mod adapter;
pub mod conformance;
pub mod durable;
pub mod evidence;
pub mod fixture;
