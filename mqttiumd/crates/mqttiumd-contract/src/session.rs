//! Explicit bounded session incarnations and receipted lifecycle changes.
//!
//! ClientID is reusable; SessionId and the opening OperationId are not.
//! A policy/binding change never changes the business revision carried by
//! existing routes, inbound markers and recipient obligations (ADR 0029).

use alloc::vec::Vec;

use crate::command::RecipeError;
use crate::ids::{OperationId, Revision, SessionId};
use crate::packet::SharedBytes;

pub const MAX_SESSION_CLIENT_BYTES: usize = 1024;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SessionExpiry {
    OnDisconnect,
    Never,
    /// Finite seconds, strictly between zero and infinity.
    After(u32),
}

impl SessionExpiry {
    #[must_use]
    pub const fn from_wire(value: u32) -> Option<Self> {
        match value {
            0 => Some(Self::OnDisconnect),
            u32::MAX => Some(Self::Never),
            value => Some(Self::After(value)),
        }
    }

    #[must_use]
    pub const fn to_wire(self) -> u32 {
        match self {
            Self::OnDisconnect => 0,
            Self::Never => u32::MAX,
            Self::After(value) => value,
        }
    }
}

/// Immutable durable detachment occurrence. A missing deadline retains forever.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SessionDetachment {
    pub operation: OperationId,
    pub deadline: Option<u64>,
}

impl SessionDetachment {
    /// Validate the policy-dependent shape before a durable row becomes a
    /// scheduling fact. Logical time starts at zero, so a finite deadline
    /// cannot precede the interval itself, even without the original sample.
    #[must_use]
    pub fn valid_for(self, expiry: SessionExpiry) -> bool {
        match (expiry, self.deadline) {
            (SessionExpiry::Never, None) => true,
            (SessionExpiry::After(_), Some(deadline)) => expiry
                .deadline(0)
                .flatten()
                .is_some_and(|minimum| deadline >= minimum),
            _ => false,
        }
    }
}

impl SessionExpiry {
    /// Logical nanoseconds. Overflow refuses the command, never expires early.
    pub fn deadline(self, observed_at: u64) -> Option<Option<u64>> {
        match self {
            Self::OnDisconnect => Some(Some(observed_at)),
            Self::Never => Some(None),
            Self::After(seconds) if seconds > 0 && seconds < u32::MAX => observed_at
                .checked_add(u64::from(seconds).checked_mul(1_000_000_000)?)
                .map(Some),
            Self::After(_) => None,
        }
    }
}

/// A durable attachment guard, separate from business revision/transport generation.
/// None is reserved for legacy administrative fixture sessions.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SessionGuard {
    pub session: SessionId,
    pub opened_by: Option<OperationId>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SessionEndReason {
    Disconnect,
    TransportLost,
    Replaced,
    Startup,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SessionAction {
    Open {
        client_id: SharedBytes,
        expected: Option<SessionGuard>,
        clean_start: bool,
        expiry: SessionExpiry,
    },
    /// Time-aware Open. Legacy Open has unchanged canonical bytes and cannot
    /// establish a finite policy without an admissible logical observation.
    OpenAt {
        client_id: SharedBytes,
        expected: Option<SessionGuard>,
        clean_start: bool,
        expiry: SessionExpiry,
        now: u64,
    },
    /// Captured transport closure; retries keep this original observation.
    Detach {
        guard: SessionGuard,
        expiry: SessionExpiry,
        observed_at: u64,
    },
    /// Conditional expiry of exactly one durable offline occurrence.
    Expire {
        guard: SessionGuard,
        detached: SessionDetachment,
        now: u64,
    },
    /// Terminate only this exact attachment. Persistent transport detachment
    /// without a policy change does not need a durable lifecycle command.
    Close {
        guard: SessionGuard,
        reason: SessionEndReason,
    },
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SessionLifecycle {
    pub operation: OperationId,
    pub action: SessionAction,
}

/// Cancelled responsibilities are not successful recipient deliveries.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct SessionCancellations {
    pub inbound: u32,
    pub deliveries: u32,
    pub routes: u32,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SessionChange {
    Opened {
        session: SessionId,
        revision: Revision,
        session_present: bool,
        expiry: SessionExpiry,
        retired: Option<SessionId>,
        route_revision: Revision,
        cancelled: SessionCancellations,
    },
    Detached {
        session: SessionId,
        expiry: SessionExpiry,
        detached: SessionDetachment,
    },
    Closed {
        session: SessionId,
        route_revision: Revision,
        cancelled: SessionCancellations,
    },
}

impl SessionLifecycle {
    pub(crate) fn check(&self) -> Result<(), RecipeError> {
        if let SessionAction::Open { client_id, .. } | SessionAction::OpenAt { client_id, .. } =
            &self.action
            && (client_id.is_empty()
                || client_id.len() > MAX_SESSION_CLIENT_BYTES
                || client_id.as_slice().contains(&0)
                || core::str::from_utf8(client_id.as_slice()).is_err())
        {
            return Err(RecipeError::InvalidField("session_client_id"));
        }
        match &self.action {
            SessionAction::Open {
                expiry: SessionExpiry::After(_),
                ..
            } => return Err(RecipeError::InvalidField("legacy_session_expiry")),
            SessionAction::OpenAt {
                expiry: SessionExpiry::After(value),
                ..
            }
            | SessionAction::Detach {
                expiry: SessionExpiry::After(value),
                ..
            } if *value == 0 || *value == u32::MAX => {
                return Err(RecipeError::InvalidField("session_expiry"));
            }
            SessionAction::Detach {
                expiry,
                observed_at,
                ..
            } if expiry.deadline(*observed_at).is_none() => {
                return Err(RecipeError::InvalidField("session_deadline"));
            }
            _ => {}
        }
        Ok(())
    }

    /// Body of canonical format1/kind8. Existing kinds keep their bytes.
    pub(crate) fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.operation.to_be_bytes());
        match &self.action {
            SessionAction::Open {
                client_id,
                expected,
                clean_start,
                expiry,
            } => {
                out.push(0);
                out.push(u8::from(*clean_start));
                out.push(match expiry {
                    SessionExpiry::OnDisconnect => 0,
                    SessionExpiry::Never => 1,
                    SessionExpiry::After(_) => panic!("checked legacy recipe"),
                });
                out.push(u8::from(expected.is_some()));
                if let Some(guard) = expected {
                    encode_guard(out, *guard);
                }
                out.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
                out.extend_from_slice(client_id.as_slice());
            }
            SessionAction::OpenAt {
                client_id,
                expected,
                clean_start,
                expiry,
                now,
            } => {
                out.push(2);
                out.push(u8::from(*clean_start));
                encode_expiry(out, *expiry);
                out.push(u8::from(expected.is_some()));
                if let Some(guard) = expected {
                    encode_guard(out, *guard);
                }
                out.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
                out.extend_from_slice(client_id.as_slice());
                out.extend_from_slice(&now.to_be_bytes());
            }
            SessionAction::Detach {
                guard,
                expiry,
                observed_at,
            } => {
                out.push(3);
                encode_guard(out, *guard);
                encode_expiry(out, *expiry);
                out.extend_from_slice(&observed_at.to_be_bytes());
            }
            SessionAction::Expire {
                guard,
                detached,
                now,
            } => {
                out.push(4);
                encode_guard(out, *guard);
                out.extend_from_slice(&detached.operation.to_be_bytes());
                out.push(u8::from(detached.deadline.is_some()));
                if let Some(deadline) = detached.deadline {
                    out.extend_from_slice(&deadline.to_be_bytes());
                }
                out.extend_from_slice(&now.to_be_bytes());
            }
            SessionAction::Close { guard, reason } => {
                out.push(1);
                encode_guard(out, *guard);
                out.push(match reason {
                    SessionEndReason::Disconnect => 0,
                    SessionEndReason::TransportLost => 1,
                    SessionEndReason::Replaced => 2,
                    SessionEndReason::Startup => 3,
                });
            }
        }
    }

    pub(crate) fn decode(bytes: &mut &[u8]) -> Result<Self, RecipeError> {
        let operation = read_operation(bytes)?;
        let action = match read_byte(bytes)? {
            0 => {
                let clean_start = read_bool(bytes)?;
                let expiry = match read_byte(bytes)? {
                    0 => SessionExpiry::OnDisconnect,
                    1 => SessionExpiry::Never,
                    _ => return Err(RecipeError::InvalidField("session_expiry")),
                };
                let expected = if read_bool(bytes)? {
                    Some(read_guard(bytes)?)
                } else {
                    None
                };
                let raw = take(bytes, 2)?;
                let len = usize::from(u16::from_be_bytes([raw[0], raw[1]]));
                if len > MAX_SESSION_CLIENT_BYTES {
                    return Err(RecipeError::InvalidField("session_client_id"));
                }
                let client_id = SharedBytes::copy_from(take(bytes, len)?, MAX_SESSION_CLIENT_BYTES)
                    .map_err(|_| RecipeError::AllocationFailed)?;
                SessionAction::Open {
                    client_id,
                    expected,
                    clean_start,
                    expiry,
                }
            }
            1 => {
                let guard = read_guard(bytes)?;
                let reason = match read_byte(bytes)? {
                    0 => SessionEndReason::Disconnect,
                    1 => SessionEndReason::TransportLost,
                    2 => SessionEndReason::Replaced,
                    3 => SessionEndReason::Startup,
                    _ => return Err(RecipeError::InvalidField("session_end_reason")),
                };
                SessionAction::Close { guard, reason }
            }
            2 => {
                let clean_start = read_bool(bytes)?;
                let expiry = read_expiry(bytes)?;
                let expected = if read_bool(bytes)? {
                    Some(read_guard(bytes)?)
                } else {
                    None
                };
                let raw = take(bytes, 2)?;
                let len = usize::from(u16::from_be_bytes([raw[0], raw[1]]));
                if len > MAX_SESSION_CLIENT_BYTES {
                    return Err(RecipeError::InvalidField("session_client_id"));
                }
                let client_id = SharedBytes::copy_from(take(bytes, len)?, MAX_SESSION_CLIENT_BYTES)
                    .map_err(|_| RecipeError::AllocationFailed)?;
                let now = read_u64(bytes)?;
                SessionAction::OpenAt {
                    client_id,
                    expected,
                    clean_start,
                    expiry,
                    now,
                }
            }
            3 => {
                let guard = read_guard(bytes)?;
                let expiry = read_expiry(bytes)?;
                let observed_at = read_u64(bytes)?;
                SessionAction::Detach {
                    guard,
                    expiry,
                    observed_at,
                }
            }
            4 => {
                let guard = read_guard(bytes)?;
                let operation = read_operation(bytes)?;
                let deadline = if read_bool(bytes)? {
                    Some(read_u64(bytes)?)
                } else {
                    None
                };
                let now = read_u64(bytes)?;
                SessionAction::Expire {
                    guard,
                    detached: SessionDetachment {
                        operation,
                        deadline,
                    },
                    now,
                }
            }
            _ => return Err(RecipeError::InvalidField("session_action")),
        };
        let result = Self { operation, action };
        result.check()?;
        Ok(result)
    }
}

fn encode_expiry(out: &mut Vec<u8>, expiry: SessionExpiry) {
    match expiry {
        SessionExpiry::OnDisconnect => out.push(0),
        SessionExpiry::Never => out.push(1),
        SessionExpiry::After(value) => {
            out.push(2);
            out.extend_from_slice(&value.to_be_bytes());
        }
    }
}
fn read_expiry(bytes: &mut &[u8]) -> Result<SessionExpiry, RecipeError> {
    match read_byte(bytes)? {
        0 => Ok(SessionExpiry::OnDisconnect),
        1 => Ok(SessionExpiry::Never),
        2 => {
            let value = u32::from_be_bytes(
                take(bytes, 4)?
                    .try_into()
                    .map_err(|_| RecipeError::Truncated)?,
            );
            if value == 0 || value == u32::MAX {
                return Err(RecipeError::InvalidField("session_expiry"));
            }
            Ok(SessionExpiry::After(value))
        }
        _ => Err(RecipeError::InvalidField("session_expiry")),
    }
}
fn read_u64(bytes: &mut &[u8]) -> Result<u64, RecipeError> {
    Ok(u64::from_be_bytes(
        take(bytes, 8)?
            .try_into()
            .map_err(|_| RecipeError::Truncated)?,
    ))
}

fn encode_guard(out: &mut Vec<u8>, guard: SessionGuard) {
    out.extend_from_slice(&guard.session.to_be_bytes());
    out.push(u8::from(guard.opened_by.is_some()));
    if let Some(operation) = guard.opened_by {
        out.extend_from_slice(&operation.to_be_bytes());
    }
}

fn take<'a>(bytes: &mut &'a [u8], length: usize) -> Result<&'a [u8], RecipeError> {
    if bytes.len() < length {
        return Err(RecipeError::Truncated);
    }
    let (head, tail) = bytes.split_at(length);
    *bytes = tail;
    Ok(head)
}

fn read_byte(bytes: &mut &[u8]) -> Result<u8, RecipeError> {
    Ok(take(bytes, 1)?[0])
}

fn read_bool(bytes: &mut &[u8]) -> Result<bool, RecipeError> {
    match read_byte(bytes)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(RecipeError::InvalidField("session_flag")),
    }
}

fn read_operation(bytes: &mut &[u8]) -> Result<OperationId, RecipeError> {
    OperationId::from_be_slice(take(bytes, 32)?).map_err(|_| RecipeError::Truncated)
}

fn read_guard(bytes: &mut &[u8]) -> Result<SessionGuard, RecipeError> {
    let raw = take(bytes, 8)?;
    let session = SessionId::new(u64::from_be_bytes(
        raw.try_into().map_err(|_| RecipeError::Truncated)?,
    ));
    let opened_by = if read_bool(bytes)? {
        Some(read_operation(bytes)?)
    } else {
        None
    };
    Ok(SessionGuard { session, opened_by })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::Recipe;
    use crate::ids::{LogicalOwnerId, OperationSeq, RunEpoch, StoreIncarnation};
    use crate::receipts::{RECEIPT_DATA_WINDOW, RECEIPT_WINDOW, ReceiptClass};

    fn operation(n: u64) -> OperationId {
        OperationId::new(
            StoreIncarnation::new(11),
            RunEpoch::new(2),
            LogicalOwnerId::new(0),
            OperationSeq::new(n),
        )
    }

    fn roundtrip(action: SessionAction) -> Vec<u8> {
        let recipe = Recipe::SessionLifecycle(SessionLifecycle {
            operation: operation(8),
            action,
        });
        let mut bytes = Vec::new();
        recipe.encode_canonical(&mut bytes).unwrap();
        assert_eq!(bytes[6], 8);
        assert_eq!(Recipe::decode_canonical(&bytes, 16, 1024).unwrap(), recipe);
        for length in 0..bytes.len() {
            assert!(Recipe::decode_canonical(&bytes[..length], 16, 1024).is_err());
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert_eq!(
            Recipe::decode_canonical(&trailing, 16, 1024),
            Err(RecipeError::TrailingBytes)
        );
        bytes
    }

    #[test]
    fn the_lifecycle_encoding_keeps_policy_clean_start_and_each_attachment_guard() {
        for expected in [
            None,
            Some(SessionGuard {
                session: SessionId::new(3),
                opened_by: None,
            }),
            Some(SessionGuard {
                session: SessionId::new(3),
                opened_by: Some(operation(7)),
            }),
        ] {
            for clean_start in [false, true] {
                for expiry in [SessionExpiry::OnDisconnect, SessionExpiry::Never] {
                    roundtrip(SessionAction::Open {
                        client_id: SharedBytes::copy_from(b"client", 1024).unwrap(),
                        expected,
                        clean_start,
                        expiry,
                    });
                }
            }
        }
        for reason in [
            SessionEndReason::Disconnect,
            SessionEndReason::TransportLost,
            SessionEndReason::Replaced,
            SessionEndReason::Startup,
        ] {
            roundtrip(SessionAction::Close {
                guard: SessionGuard {
                    session: SessionId::new(3),
                    opened_by: Some(operation(7)),
                },
                reason,
            });
        }
    }

    #[test]
    fn an_invalid_policy_or_flag_has_no_canonical_alias() {
        let bytes = roundtrip(SessionAction::Open {
            client_id: SharedBytes::copy_from(b"client", 1024).unwrap(),
            expected: None,
            clean_start: false,
            expiry: SessionExpiry::Never,
        });
        for index in [40, 41, 42] {
            let mut wrong = bytes.clone();
            wrong[index] = 2;
            assert!(matches!(
                Recipe::decode_canonical(&wrong, 16, 1024),
                Err(RecipeError::InvalidField(_))
            ));
        }
    }

    #[test]
    fn close_has_reserved_capacity_and_open_cannot_make_a_control_chain() {
        let close = Recipe::SessionLifecycle(SessionLifecycle {
            operation: operation(8),
            action: SessionAction::Close {
                guard: SessionGuard {
                    session: SessionId::new(3),
                    opened_by: None,
                },
                reason: SessionEndReason::Disconnect,
            },
        });
        assert_eq!(ReceiptClass::of(&close), ReceiptClass::Control);
        let open = Recipe::SessionLifecycle(SessionLifecycle {
            operation: operation(8),
            action: SessionAction::Open {
                client_id: SharedBytes::copy_from(b"x", 1024).unwrap(),
                expected: None,
                clean_start: false,
                expiry: SessionExpiry::Never,
            },
        });
        assert_eq!(ReceiptClass::of(&open), ReceiptClass::Business);
        assert_eq!(RECEIPT_WINDOW - RECEIPT_DATA_WINDOW, 128 + 128 + 256);
    }

    #[test]
    fn finite_wire_intervals_are_supported_without_reclassifying_zero_or_infinity() {
        for value in [1, 2, u32::MAX - 1] {
            assert!(
                SessionExpiry::from_wire(value).is_some(),
                "finite_interval_rejected"
            );
            assert_eq!(SessionExpiry::from_wire(value).unwrap().to_wire(), value);
        }
        assert_eq!(
            RECEIPT_WINDOW - RECEIPT_DATA_WINDOW,
            128 + 128 + 256,
            "finite_detach_and_expire_have_insufficient_continuation_reserve"
        );
    }

    #[test]
    fn wire_values_have_exact_retention_policies() {
        assert_eq!(
            SessionExpiry::from_wire(0),
            Some(SessionExpiry::OnDisconnect)
        );
        assert_eq!(
            SessionExpiry::from_wire(u32::MAX),
            Some(SessionExpiry::Never)
        );
        assert_eq!(SessionExpiry::from_wire(1), Some(SessionExpiry::After(1)));
        assert_eq!(
            SessionExpiry::from_wire(u32::MAX - 1),
            Some(SessionExpiry::After(u32::MAX - 1))
        );
    }

    #[test]
    fn timed_actions_preserve_the_original_observation_and_detachment() {
        let guard = SessionGuard {
            session: SessionId::new(3),
            opened_by: Some(operation(2)),
        };
        for expiry in [
            SessionExpiry::OnDisconnect,
            SessionExpiry::Never,
            SessionExpiry::After(1),
            SessionExpiry::After(u32::MAX - 1),
        ] {
            roundtrip(SessionAction::OpenAt {
                client_id: SharedBytes::copy_from(b"timed", 1024).unwrap(),
                expected: Some(guard),
                clean_start: false,
                expiry,
                now: 1_234_567_890,
            });
            roundtrip(SessionAction::Detach {
                guard,
                expiry,
                observed_at: 1_234_567_890,
            });
        }
        for deadline in [None, Some(0), Some(u64::MAX)] {
            roundtrip(SessionAction::Expire {
                guard,
                detached: SessionDetachment {
                    operation: operation(7),
                    deadline,
                },
                now: u64::MAX,
            });
        }
    }

    #[test]
    fn finite_deadline_math_refuses_aliases_and_overflow() {
        assert_eq!(
            SessionExpiry::After(1).deadline(999_999_999),
            Some(Some(1_999_999_999))
        );
        assert_eq!(
            SessionExpiry::After(u32::MAX - 1).deadline(0),
            Some(Some(u64::from(u32::MAX - 1) * 1_000_000_000))
        );
        assert_eq!(SessionExpiry::Never.deadline(u64::MAX), Some(None));
        assert_eq!(SessionExpiry::After(1).deadline(u64::MAX), None);
        for seconds in [0, u32::MAX] {
            let recipe = Recipe::SessionLifecycle(SessionLifecycle {
                operation: operation(8),
                action: SessionAction::OpenAt {
                    client_id: SharedBytes::copy_from(b"invalid", 1024).unwrap(),
                    expected: None,
                    clean_start: false,
                    expiry: SessionExpiry::After(seconds),
                    now: 0,
                },
            });
            assert!(matches!(
                recipe.encode_canonical(&mut Vec::new()),
                Err(RecipeError::InvalidField("session_expiry"))
            ));
        }
        let recipe = Recipe::SessionLifecycle(SessionLifecycle {
            operation: operation(8),
            action: SessionAction::Open {
                client_id: SharedBytes::copy_from(b"legacy", 1024).unwrap(),
                expected: None,
                clean_start: false,
                expiry: SessionExpiry::After(1),
            },
        });
        assert!(matches!(
            recipe.encode_canonical(&mut Vec::new()),
            Err(RecipeError::InvalidField("legacy_session_expiry"))
        ));
    }

    #[test]
    fn a_detachment_deadline_must_have_a_possible_nonnegative_observation() {
        for expiry in [
            SessionExpiry::OnDisconnect,
            SessionExpiry::Never,
            SessionExpiry::After(0),
            SessionExpiry::After(1),
            SessionExpiry::After(u32::MAX - 1),
            SessionExpiry::After(u32::MAX),
        ] {
            let minimum = expiry.deadline(0).flatten();
            for deadline in [
                None,
                Some(0),
                Some(999_999_999),
                Some(1_000_000_000),
                minimum,
                Some(u64::MAX),
            ] {
                let detached = SessionDetachment {
                    operation: operation(7),
                    deadline,
                };
                let expected = match (expiry, deadline) {
                    (SessionExpiry::Never, None) => true,
                    (SessionExpiry::After(seconds), Some(deadline))
                        if seconds > 0 && seconds < u32::MAX =>
                    {
                        deadline >= u64::from(seconds) * 1_000_000_000
                    }
                    _ => false,
                };
                assert_eq!(detached.valid_for(expiry), expected);
            }
        }
    }
}
