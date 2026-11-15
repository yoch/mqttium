//! The runner of the bench (lot L4).
//!
//! `run` plays one scenario through the real components and leaves a
//! campaign directory that the independent oracle judges:
//!
//! - `backend`: the durable store a campaign runs on;
//! - `scenario`: the scenario as the runner plays it;
//! - `mqtt`: the bytes of the subset, for the peers and the transport;
//! - `collector`: the one writer of the trace, origin by channel;
//! - `process`: the process under test, the real core and the
//!   deterministic store, started by the real path;
//! - `runner`: the supervisor, the peers and the scheduler of choices;
//! - `protocol`: the frames, messages, order and flow between the
//!   supervisor and the child process, tested before any process exists;
//! - `campaign`: the directory, its manifest and its inventory;
//! - `rebuild`: a campaign whose binary is built from the content it
//!   records.
//!
//! Nothing here computes a verdict.

pub mod backend;
pub mod campaign;
pub mod collector;
pub mod mqtt;
pub mod process;
pub mod protocol;
pub mod rebuild;
pub mod runner;
pub mod scenario;
