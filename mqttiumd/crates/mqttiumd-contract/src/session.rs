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
}

impl SessionExpiry {
    #[must_use]
    pub const fn from_wire(value: u32) -> Option<Self> {
        match value {
            0 => Some(Self::OnDisconnect),
            u32::MAX => Some(Self::Never),
            _ => None,
        }
    }

    #[must_use]
    pub const fn to_wire(self) -> u32 {
        match self {
            Self::OnDisconnect => 0,
            Self::Never => u32::MAX,
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
    Closed {
        session: SessionId,
        route_revision: Revision,
        cancelled: SessionCancellations,
    },
}

impl SessionLifecycle {
    pub(crate) fn check(&self) -> Result<(), RecipeError> {
        if let SessionAction::Open { client_id, .. } = &self.action
            && (client_id.is_empty()
                || client_id.len() > MAX_SESSION_CLIENT_BYTES
                || client_id.as_slice().contains(&0)
                || core::str::from_utf8(client_id.as_slice()).is_err())
        {
            return Err(RecipeError::InvalidField("session_client_id"));
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
                });
                out.push(u8::from(expected.is_some()));
                if let Some(guard) = expected {
                    encode_guard(out, *guard);
                }
                out.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
                out.extend_from_slice(client_id.as_slice());
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
            _ => return Err(RecipeError::InvalidField("session_action")),
        };
        let result = Self { operation, action };
        result.check()?;
        Ok(result)
    }
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
        assert_eq!(RECEIPT_WINDOW - RECEIPT_DATA_WINDOW, 128 + 128 + 128);
    }

    #[test]
    fn only_zero_and_infinity_are_retention_policies() {
        assert_eq!(
            SessionExpiry::from_wire(0),
            Some(SessionExpiry::OnDisconnect)
        );
        assert_eq!(
            SessionExpiry::from_wire(u32::MAX),
            Some(SessionExpiry::Never)
        );
        assert_eq!(SessionExpiry::from_wire(1), None);
        assert_eq!(SessionExpiry::from_wire(u32::MAX - 1), None);
    }
}
