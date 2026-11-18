//! Product composition of the pure core and the exclusive durable writer.
#![forbid(unsafe_code)]

pub mod codec;

pub mod events;
pub mod net;
pub mod policy_transition;
pub mod runtime;
pub mod session_time;
pub mod writer;

mod receiving;
pub mod security;
