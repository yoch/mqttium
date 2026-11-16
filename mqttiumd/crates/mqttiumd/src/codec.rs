//! The bounded MQTT 5 codec of the experimental incoming QoS 2 profile.

use mqttiumd_contract::packet::{InboundPacket, OutboundFrame, SharedBytes};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bounds {
    pub max_packet: usize,
    pub max_client_id: usize,
    pub max_topic: usize,
    pub max_payload: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    pub first: u8,
    pub remaining: usize,
    /// Total fixed-header bytes, including the packet-type byte.
    pub encoded_len: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Connect {
    pub client_id: SharedBytes,
    pub keep_alive: u16,
    pub max_packet_size: u32,
    pub receive_maximum: u16,
    pub topic_alias_maximum: u16,
    pub request_problem_information: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Packet {
    Connect(Connect),
    Core(InboundPacket),
    Disconnect,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    Malformed,
    Protocol,
    Unsupported,
    Oversized,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Error {
    pub kind: ErrorKind,
    pub code: &'static str,
    pub reason: u8,
}

impl Error {
    pub const fn kind(self) -> ErrorKind {
        self.kind
    }
    pub const fn code(self) -> &'static str {
        self.code
    }
    pub const fn reason_code(self) -> u8 {
        self.reason
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(out, "{:?}: {}", self.kind, self.code)
    }
}

impl std::error::Error for Error {}

fn pending() -> Error {
    Error {
        kind: ErrorKind::Unsupported,
        code: "CodecNotImplemented",
        reason: 0x83,
    }
}

const fn malformed(code: &'static str) -> Error {
    Error {
        kind: ErrorKind::Malformed,
        code,
        reason: 0x81,
    }
}

const fn protocol(code: &'static str) -> Error {
    Error {
        kind: ErrorKind::Protocol,
        code,
        reason: 0x82,
    }
}

const fn unsupported(code: &'static str) -> Error {
    Error {
        kind: ErrorKind::Unsupported,
        code,
        reason: 0x83,
    }
}

const fn oversized(code: &'static str) -> Error {
    Error {
        kind: ErrorKind::Oversized,
        code,
        reason: 0x95,
    }
}

fn flags(first: u8) -> Result<(), Error> {
    let low = first & 0x0f;
    match first >> 4 {
        0 => Err(malformed("ReservedPacketType")),
        3 if (first >> 1) & 3 == 3 => Err(malformed("InvalidQos")),
        3 if (first >> 1) & 3 == 0 && first & 8 != 0 => Err(malformed("DupAtQosZero")),
        3 => Ok(()),
        6 | 8 | 10 if low == 2 => Ok(()),
        6 | 8 | 10 => Err(malformed("InvalidFixedFlags")),
        _ if low == 0 => Ok(()),
        _ => Err(malformed("InvalidFixedFlags")),
    }
}

/// Inspect at most five bytes. A returned size is checked before the
/// transport reserves or reads an application-owned body buffer.
pub fn header(bytes: &[u8], max_packet: usize) -> Result<Option<Header>, Error> {
    let Some(&first) = bytes.first() else {
        return Ok(None);
    };
    flags(first)?;
    let mut remaining = 0usize;
    for digit in 0..4 {
        let Some(&byte) = bytes.get(digit + 1) else {
            return Ok(None);
        };
        remaining = remaining
            .checked_add(usize::from(byte & 0x7f) << (digit * 7))
            .ok_or_else(|| oversized("RemainingLengthOverflow"))?;
        if byte & 0x80 == 0 {
            if digit > 0 && byte == 0 {
                return Err(malformed("NonCanonicalRemainingLength"));
            }
            let encoded_len = digit + 2;
            if remaining
                .checked_add(encoded_len)
                .is_none_or(|n| n > max_packet)
            {
                return Err(oversized("PacketTooLarge"));
            }
            return Ok(Some(Header {
                first,
                remaining,
                encoded_len,
            }));
        }
    }
    Err(malformed("RemainingLengthTooLong"))
}

pub fn decode(_: u8, _: &[u8], _: Bounds) -> Result<Packet, Error> {
    Err(pending())
}

pub fn encode(_: OutboundFrame) -> Result<Vec<u8>, Error> {
    Err(pending())
}

pub fn refusal(error: Error, before_connack: bool) -> Vec<u8> {
    if before_connack {
        vec![0x20, 3, 0, error.reason, 0]
    } else {
        vec![0xe0, 2, error.reason, 0]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mqttiumd_contract::ids::InboundPacketId;
    use mqttiumd_contract::packet::{AckReason, Qos};

    const BOUNDS: Bounds = Bounds {
        max_packet: 65_536,
        max_client_id: 1024,
        max_topic: 1024,
        max_payload: 32_768,
    };

    fn connect(properties: &[u8], client: &[u8]) -> Vec<u8> {
        assert!(properties.len() < 128);
        let mut body = b"\0\x04MQTT\x05\0\0\x1e".to_vec();
        body.push(properties.len() as u8);
        body.extend_from_slice(properties);
        body.extend_from_slice(&(client.len() as u16).to_be_bytes());
        body.extend_from_slice(client);
        body
    }

    const EXPIRY: &[u8] = &[0x11, 0xff, 0xff, 0xff, 0xff];

    fn kind(result: Result<Packet, Error>, expected: ErrorKind) {
        assert_eq!(result.expect_err("must refuse").kind, expected);
    }

    #[test]
    fn a_header_is_complete_before_its_body_and_keeps_a_bounded_size() {
        assert_eq!(header(&[], 65_536).unwrap(), None);
        assert_eq!(header(&[0x34], 65_536).unwrap(), None);
        assert_eq!(header(&[0x34, 0x80], 65_536).unwrap(), None);
        assert_eq!(
            header(&[0x34, 0x80, 1], 131).unwrap(),
            Some(Header {
                first: 0x34,
                remaining: 128,
                encoded_len: 3
            })
        );
        assert_eq!(
            header(&[0x34, 0x80, 1], 130).unwrap_err().kind,
            ErrorKind::Oversized
        );
    }

    #[test]
    fn remaining_length_is_canonical_and_at_most_four_bytes() {
        for bytes in [vec![0x34, 0x80, 0], vec![0x34, 0xff, 0xff, 0xff, 0x80]] {
            assert_eq!(
                header(&bytes, 65_536).unwrap_err().kind,
                ErrorKind::Malformed
            );
        }
    }

    #[test]
    fn the_profile_connect_accepts_expiry_and_applies_common_limits() {
        let mut props = EXPIRY.to_vec();
        props.extend_from_slice(&[0x21, 0, 7, 0x22, 0, 6, 0x27, 0, 1, 0, 0, 0x17, 0]);
        let Packet::Connect(c) =
            decode(0x10, &connect(&props, b"external-client"), BOUNDS).unwrap()
        else {
            panic!("CONNECT")
        };
        assert_eq!(c.client_id.as_slice(), b"external-client");
        assert_eq!(c.keep_alive, 30);
        assert_eq!(c.receive_maximum, 7);
        assert_eq!(c.topic_alias_maximum, 6);
        assert_eq!(c.max_packet_size, 65_536);
        assert!(!c.request_problem_information);
    }

    #[test]
    fn a_tiny_nonzero_peer_limit_is_retained_for_the_writer() {
        let mut props = EXPIRY.to_vec();
        props.extend_from_slice(&[0x27, 0, 0, 0, 1]);
        let Packet::Connect(c) = decode(0x10, &connect(&props, b"small"), BOUNDS).unwrap() else {
            panic!("CONNECT")
        };
        assert_eq!(c.max_packet_size, 1);
    }

    #[test]
    fn profile_refusals_differ_from_malformed_or_contradictory_properties() {
        kind(
            decode(0x10, &connect(&[], b"client"), BOUNDS),
            ErrorKind::Unsupported,
        );
        let mut duplicate = EXPIRY.to_vec();
        duplicate.extend_from_slice(EXPIRY);
        kind(
            decode(0x10, &connect(&duplicate, b"client"), BOUNDS),
            ErrorKind::Protocol,
        );
        let mut zero = EXPIRY.to_vec();
        zero.extend_from_slice(&[0x21, 0, 0]);
        kind(
            decode(0x10, &connect(&zero, b"client"), BOUNDS),
            ErrorKind::Protocol,
        );
        let mut bad = EXPIRY.to_vec();
        bad.extend_from_slice(&[0x27, 1]);
        kind(
            decode(0x10, &connect(&bad, b"client"), BOUNDS),
            ErrorKind::Malformed,
        );
    }

    #[test]
    fn utf8_and_client_boundaries_are_checked_before_owned_packets() {
        for client in [&b"bad\0id"[..], &b"\xc0\x80"[..], &b"bad\x1fid"[..]] {
            kind(
                decode(0x10, &connect(EXPIRY, client), BOUNDS),
                ErrorKind::Malformed,
            );
        }
        let client = vec![b'x'; 1025];
        kind(
            decode(0x10, &connect(EXPIRY, &client), BOUNDS),
            ErrorKind::Oversized,
        );
    }

    #[test]
    fn qos2_publish_and_pubrel_enter_the_existing_core() {
        let body = b"\0\x03a/b\0\x07\0payload";
        let Packet::Core(InboundPacket::Publish {
            topic,
            qos,
            retain,
            dup,
            packet_id,
            payload,
        }) = decode(0x3c, body, BOUNDS).unwrap()
        else {
            panic!("PUBLISH")
        };
        assert_eq!(topic.as_slice(), b"a/b");
        assert_eq!(qos, Qos::ExactlyOnce);
        assert!(!retain);
        assert!(dup);
        assert_eq!(packet_id.unwrap().get(), 7);
        assert_eq!(payload.as_slice(), b"payload");
        for body in [&[0, 7][..], &[0, 7, 0][..], &[0, 7, 0, 0][..]] {
            assert_eq!(
                decode(0x62, body, BOUNDS).unwrap(),
                Packet::Core(InboundPacket::Pubrel {
                    packet_id: InboundPacketId::new(7).unwrap()
                })
            );
        }
    }

    #[test]
    fn publish_restrictions_do_not_hide_invalid_fields() {
        kind(
            decode(0x32, b"\0\x03a/b\0\x07\0payload", BOUNDS),
            ErrorKind::Unsupported,
        );
        kind(
            decode(0x35, b"\0\x03a/b\0\x07\0payload", BOUNDS),
            ErrorKind::Unsupported,
        );
        kind(
            decode(0x35, b"\0\x03a/b\0\0\0payload", BOUNDS),
            ErrorKind::Protocol,
        );
        kind(
            decode(0x34, b"\0\x03a/+\0\x07\0payload", BOUNDS),
            ErrorKind::Protocol,
        );
        kind(
            decode(0x34, b"\0\x03a/b\0\x07\x02\x0b\x01payload", BOUNDS),
            ErrorKind::Protocol,
        );
    }

    #[test]
    fn duplicate_unsupported_properties_are_still_a_protocol_error() {
        let body = b"\0\x03a/b\0\x07\x08\x03\0\x01x\x03\0\x01ypayload";
        kind(decode(0x34, body, BOUNDS), ErrorKind::Protocol);
    }

    #[test]
    fn ping_and_disconnect_do_not_fabricate_store_packets() {
        assert_eq!(
            decode(0xc0, &[], BOUNDS).unwrap(),
            Packet::Core(InboundPacket::Pingreq)
        );
        assert_eq!(decode(0xe0, &[], BOUNDS).unwrap(), Packet::Disconnect);
        kind(decode(0xc0, &[0], BOUNDS), ErrorKind::Malformed);
        kind(decode(0x60, &[0, 7], BOUNDS), ErrorKind::Malformed);
    }

    #[test]
    fn valid_subscribe_is_unsupported_but_a_truncated_one_is_malformed() {
        kind(
            decode(0x82, b"\0\x01\0\0\x03a/#\x02", BOUNDS),
            ErrorKind::Unsupported,
        );
        kind(
            decode(0x82, b"\0\x01\0\0\x03a/#", BOUNDS),
            ErrorKind::Malformed,
        );
    }

    #[test]
    fn outbound_frames_are_mqtt5_and_advertise_only_implemented_features() {
        let packet = encode(OutboundFrame::Connack {
            session_present: false,
        })
        .unwrap();
        assert_eq!(&packet[..5], &[0x20, 22, 0, 0, 19]);
        assert_eq!(
            &packet[5..],
            &[
                0x21, 0, 8, 0x27, 0, 1, 0, 0, 0x25, 0, 0x22, 0, 0, 0x28, 0, 0x29, 0, 0x2a, 0
            ]
        );
        assert_eq!(encode(OutboundFrame::Pingresp).unwrap(), [0xd0, 0]);
        assert_eq!(
            encode(OutboundFrame::Pubrec {
                packet_id: InboundPacketId::new(7).unwrap(),
                reason: AckReason::Success
            })
            .unwrap(),
            [0x50, 2, 0, 7]
        );
    }
}
