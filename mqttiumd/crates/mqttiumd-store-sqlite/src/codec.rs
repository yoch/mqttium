//! Durable encodings (AGENTS rule 7). A 64-bit value or an identity is a
//! BLOB of a checked length, big-endian, so that the order of the BLOBs
//! is the order of the values; a small enumeration is an INTEGER with an
//! explicit code. Nothing is computed on an encoded form: it is decoded,
//! changed in Rust with checked arithmetic, and encoded again.

use mqttiumd_contract::capacity::Resource;
use mqttiumd_contract::command::{DeliveryCause, OpeningAttempt};
use mqttiumd_contract::effects::InboundPhase;
use mqttiumd_contract::ids::{ExchangeGeneration, ExchangeId, SessionId};
use mqttiumd_contract::outcome::RejectReason;
use mqttiumd_contract::packet::Qos;
use mqttiumd_store_api::{ObligationPhase, SessionState};

/// A durable value that does not decode: the file is not one this store
/// wrote, or it was damaged. Nothing is inferred from it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Corrupt;

pub fn u64_blob(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

pub fn blob_u64(bytes: &[u8]) -> Result<u64, Corrupt> {
    let array: [u8; 8] = bytes.try_into().map_err(|_| Corrupt)?;
    Ok(u64::from_be_bytes(array))
}

/// A `usize` of this crate's targets fits 64 bits; refused otherwise.
pub fn usize_blob(value: usize) -> Result<[u8; 8], Corrupt> {
    Ok(u64_blob(u64::try_from(value).map_err(|_| Corrupt)?))
}

pub fn blob_usize(bytes: &[u8]) -> Result<usize, Corrupt> {
    usize::try_from(blob_u64(bytes)?).map_err(|_| Corrupt)
}

pub fn exchange_blob(exchange: ExchangeId) -> [u8; 16] {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&exchange.session.to_be_bytes());
    bytes[8..].copy_from_slice(&exchange.generation.to_be_bytes());
    bytes
}

pub fn blob_exchange(bytes: &[u8]) -> Result<ExchangeId, Corrupt> {
    if bytes.len() != 16 {
        return Err(Corrupt);
    }
    Ok(ExchangeId {
        session: SessionId::from_be_slice(&bytes[..8]).map_err(|_| Corrupt)?,
        generation: ExchangeGeneration::from_be_slice(&bytes[8..]).map_err(|_| Corrupt)?,
    })
}

pub fn attempt_blob(attempt: OpeningAttempt) -> [u8; 24] {
    attempt.to_be_bytes()
}

pub fn blob_attempt(bytes: &[u8]) -> Result<OpeningAttempt, Corrupt> {
    OpeningAttempt::from_be_slice(bytes).map_err(|_| Corrupt)
}

pub const fn qos_code(qos: Qos) -> i64 {
    match qos {
        Qos::AtMostOnce => 0,
        Qos::AtLeastOnce => 1,
        Qos::ExactlyOnce => 2,
    }
}

pub const fn code_qos(code: i64) -> Result<Qos, Corrupt> {
    match code {
        0 => Ok(Qos::AtMostOnce),
        1 => Ok(Qos::AtLeastOnce),
        2 => Ok(Qos::ExactlyOnce),
        _ => Err(Corrupt),
    }
}

pub const fn session_state_code(state: SessionState) -> i64 {
    match state {
        SessionState::Active => 1,
        SessionState::Terminal => 2,
    }
}

pub const fn code_session_state(code: i64) -> Result<SessionState, Corrupt> {
    match code {
        1 => Ok(SessionState::Active),
        2 => Ok(SessionState::Terminal),
        _ => Err(Corrupt),
    }
}

pub const fn cause_code(cause: DeliveryCause) -> i64 {
    match cause {
        DeliveryCause::LiveExact => 1,
    }
}

pub const fn code_cause(code: i64) -> Result<DeliveryCause, Corrupt> {
    match code {
        1 => Ok(DeliveryCause::LiveExact),
        _ => Err(Corrupt),
    }
}

pub const fn obligation_phase_code(phase: ObligationPhase) -> i64 {
    match phase {
        ObligationPhase::Pending => 1,
    }
}

pub const fn code_obligation_phase(code: i64) -> Result<ObligationPhase, Corrupt> {
    match code {
        1 => Ok(ObligationPhase::Pending),
        _ => Err(Corrupt),
    }
}

pub const fn inbound_phase_code(phase: InboundPhase) -> i64 {
    match phase {
        InboundPhase::AwaitPubrel => 1,
    }
}

pub const fn code_inbound_phase(code: i64) -> Result<InboundPhase, Corrupt> {
    match code {
        1 => Ok(InboundPhase::AwaitPubrel),
        _ => Err(Corrupt),
    }
}

const fn resource_code(resource: Resource) -> i64 {
    match resource {
        Resource::PayloadBytes => 1,
        Resource::References => 2,
        Resource::ExchangeSlots => 3,
        Resource::EffectSlots => 4,
        Resource::ResultSlots => 5,
        Resource::ReceiptSlots => 6,
        Resource::Continuations => 7,
        Resource::Reservations => 8,
        Resource::Work => 9,
    }
}

const fn code_resource(code: i64) -> Result<Resource, Corrupt> {
    match code {
        1 => Ok(Resource::PayloadBytes),
        2 => Ok(Resource::References),
        3 => Ok(Resource::ExchangeSlots),
        4 => Ok(Resource::EffectSlots),
        5 => Ok(Resource::ResultSlots),
        6 => Ok(Resource::ReceiptSlots),
        7 => Ok(Resource::Continuations),
        8 => Ok(Resource::Reservations),
        9 => Ok(Resource::Work),
        _ => Err(Corrupt),
    }
}

/// A rejection reason as two columns: the reason, and the resource of a
/// capacity that was short (NULL otherwise).
pub const fn reason_codes(reason: RejectReason) -> (i64, Option<i64>) {
    match reason {
        RejectReason::PreconditionFailed => (1, None),
        RejectReason::CapacityExceeded(resource) => (2, Some(resource_code(resource))),
        RejectReason::Fenced => (3, None),
        RejectReason::EpochClosed => (4, None),
    }
}

pub const fn codes_reason(reason: i64, resource: Option<i64>) -> Result<RejectReason, Corrupt> {
    match (reason, resource) {
        (1, None) => Ok(RejectReason::PreconditionFailed),
        (2, Some(resource)) => match code_resource(resource) {
            Ok(resource) => Ok(RejectReason::CapacityExceeded(resource)),
            Err(corrupt) => Err(corrupt),
        },
        (3, None) => Ok(RejectReason::Fenced),
        (4, None) => Ok(RejectReason::EpochClosed),
        _ => Err(Corrupt),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_order_of_blobs_is_the_order_of_values() {
        let values = [0u64, 1, 255, 256, 1 << 32, u64::MAX];
        for pair in values.windows(2) {
            assert!(u64_blob(pair[0]) < u64_blob(pair[1]));
            assert_eq!(blob_u64(&u64_blob(pair[0])), Ok(pair[0]));
        }
        assert_eq!(blob_u64(&[0; 7]), Err(Corrupt));
        assert_eq!(blob_u64(&[0; 9]), Err(Corrupt));
    }

    #[test]
    fn every_code_decodes_to_its_value() {
        for qos in [Qos::AtMostOnce, Qos::AtLeastOnce, Qos::ExactlyOnce] {
            assert_eq!(code_qos(qos_code(qos)), Ok(qos));
        }
        for state in [SessionState::Active, SessionState::Terminal] {
            assert_eq!(code_session_state(session_state_code(state)), Ok(state));
        }
        let resources = [
            Resource::PayloadBytes,
            Resource::References,
            Resource::ExchangeSlots,
            Resource::EffectSlots,
            Resource::ResultSlots,
            Resource::ReceiptSlots,
            Resource::Continuations,
            Resource::Reservations,
            Resource::Work,
        ];
        let mut reasons = vec![
            RejectReason::PreconditionFailed,
            RejectReason::Fenced,
            RejectReason::EpochClosed,
        ];
        reasons.extend(resources.map(RejectReason::CapacityExceeded));
        for reason in reasons {
            let (code, resource) = reason_codes(reason);
            assert_eq!(codes_reason(code, resource), Ok(reason));
        }
        assert_eq!(codes_reason(1, Some(1)), Err(Corrupt));
        assert_eq!(codes_reason(2, None), Err(Corrupt));
        assert_eq!(code_qos(3), Err(Corrupt));
    }
}
