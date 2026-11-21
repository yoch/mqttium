//! The bytes of the MQTT 5 subset of P1, on both sides of the wire.
//!
//! Two readers live here and share nothing but the constants of the
//! protocol: `peer` encodes what a simulated client sends and decodes
//! what it observes; `transport` decodes what the broker receives into
//! the validated packets of the core and encodes the frames the core
//! made eligible. The oracle decodes the same bytes a third time, with
//! code of its own (record 0016).

/// The packets of the subset, by their MQTT type.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    Connect,
    Connack,
    Publish,
    Puback,
    Pubrec,
    Pubrel,
    Pubcomp,
    Suback,
    Unsuback,
    Pingresp,
}

impl Kind {
    /// The name the trace format uses.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Connect => "CONNECT",
            Self::Connack => "CONNACK",
            Self::Publish => "PUBLISH",
            Self::Puback => "PUBACK",
            Self::Pubrec => "PUBREC",
            Self::Pubrel => "PUBREL",
            Self::Pubcomp => "PUBCOMP",
            Self::Suback => "SUBACK",
            Self::Unsuback => "UNSUBACK",
            Self::Pingresp => "PINGRESP",
        }
    }

    /// A kind from its trace name.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        [
            Self::Connect,
            Self::Connack,
            Self::Publish,
            Self::Puback,
            Self::Pubrec,
            Self::Pubrel,
            Self::Pubcomp,
            Self::Suback,
            Self::Unsuback,
            Self::Pingresp,
        ]
        .into_iter()
        .find(|kind| kind.name() == name)
    }
}

/// Appends the variable byte integer of MQTT (at most four bytes).
fn remaining_length(out: &mut Vec<u8>, mut length: usize) -> Result<(), &'static str> {
    if length > 268_435_455 {
        return Err("remaining length beyond MQTT");
    }
    loop {
        let mut byte = (length % 128) as u8;
        length /= 128;
        if length > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if length == 0 {
            return Ok(());
        }
    }
}

fn string(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), &'static str> {
    let length = u16::try_from(bytes.len()).map_err(|_| "string longer than 65535 bytes")?;
    out.extend_from_slice(&length.to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn packet(first: u8, body: &[u8]) -> Result<Vec<u8>, &'static str> {
    let mut out = vec![first];
    remaining_length(&mut out, body.len())?;
    out.extend_from_slice(body);
    Ok(out)
}

/// The fixed header of one whole packet: first byte, and the body.
/// Refuses bytes that are not exactly one packet.
fn split(bytes: &[u8]) -> Result<(u8, &[u8]), &'static str> {
    let (&first, rest) = bytes.split_first().ok_or("no fixed header")?;
    let mut length = 0usize;
    let mut multiplier = 1usize;
    let mut used = 0usize;
    loop {
        let byte = *rest.get(used).ok_or("truncated remaining length")?;
        used += 1;
        length += usize::from(byte & 0x7f) * multiplier;
        if byte & 0x80 == 0 {
            break;
        }
        if used == 4 {
            return Err("remaining length longer than four bytes");
        }
        multiplier *= 128;
    }
    let body = rest.get(used..).ok_or("truncated packet")?;
    if body.len() != length {
        return Err("the bytes are not exactly one packet");
    }
    Ok((first, body))
}

fn take_u16(body: &[u8], at: usize) -> Result<u16, &'static str> {
    let pair = body.get(at..at + 2).ok_or("truncated two-byte integer")?;
    Ok(u16::from_be_bytes([pair[0], pair[1]]))
}

fn take_string(body: &[u8], at: usize) -> Result<(&[u8], usize), &'static str> {
    let length = usize::from(take_u16(body, at)?);
    let bytes = body
        .get(at + 2..at + 2 + length)
        .ok_or("truncated string")?;
    Ok((bytes, at + 2 + length))
}

/// A property length that is zero; the subset carries no property.
fn no_properties(body: &[u8], at: usize) -> Result<usize, &'static str> {
    match body.get(at) {
        Some(0) => Ok(at + 1),
        Some(_) => Err("properties are outside the subset"),
        None => Err("truncated property length"),
    }
}

/// The simulated clients' side.
pub mod peer {
    use super::{Kind, no_properties, packet, split, string, take_u16};

    /// CONNECT of MQTT 5 that resumes a persistent session: Clean Start
    /// is 0, keep alive 10 seconds, no property, no Will, no credential.
    pub fn connect(client_id: &[u8]) -> Result<Vec<u8>, &'static str> {
        let mut body = Vec::new();
        string(&mut body, b"MQTT")?;
        body.push(5);
        body.push(0x00);
        body.extend_from_slice(&10u16.to_be_bytes());
        body.push(0);
        string(&mut body, client_id)?;
        packet(0x10, &body)
    }

    /// PUBLISH with no property.
    pub fn publish(
        topic: &[u8],
        qos: u8,
        retain: bool,
        dup: bool,
        packet_id: Option<u16>,
        payload: &[u8],
    ) -> Result<Vec<u8>, &'static str> {
        if qos > 2 || (qos == 0) != packet_id.is_none() {
            return Err("a Packet Identifier exactly when QoS is above 0");
        }
        let first = 0x30 | (u8::from(dup) << 3) | (qos << 1) | u8::from(retain);
        let mut body = Vec::new();
        string(&mut body, topic)?;
        if let Some(packet_id) = packet_id {
            body.extend_from_slice(&packet_id.to_be_bytes());
        }
        body.push(0);
        body.extend_from_slice(payload);
        packet(first, &body)
    }

    /// PUBREL with reason Success and no property: the short form.
    pub fn pubrel(packet_id: u16) -> Result<Vec<u8>, &'static str> {
        packet(0x62, &packet_id.to_be_bytes())
    }

    /// What a client decoded from bytes the broker sent.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub struct Observed {
        pub kind: Kind,
        pub packet_id: Option<u16>,
        pub reason_code: u8,
        pub session_present: Option<bool>,
    }

    /// Decodes one whole packet the broker sent.
    pub fn observe(bytes: &[u8]) -> Result<Observed, &'static str> {
        let (first, body) = split(bytes)?;
        match first {
            0x20 => {
                let flags = *body.first().ok_or("CONNACK without flags")?;
                if flags & 0xfe != 0 {
                    return Err("CONNACK with reserved flags");
                }
                let reason_code = *body.get(1).ok_or("CONNACK without reason")?;
                no_properties(body, 2)?;
                if body.len() != 3 {
                    return Err("CONNACK longer than its fields");
                }
                Ok(Observed {
                    kind: Kind::Connack,
                    packet_id: None,
                    reason_code,
                    session_present: Some(flags & 1 == 1),
                })
            }
            0x40 | 0x50 | 0x70 => {
                let kind = if first == 0x40 {
                    Kind::Puback
                } else if first == 0x50 {
                    Kind::Pubrec
                } else {
                    Kind::Pubcomp
                };
                let packet_id = take_u16(body, 0)?;
                let reason_code = match body.len() {
                    2 => 0,
                    3 => body[2],
                    _ => {
                        no_properties(body, 3)?;
                        if body.len() != 4 {
                            return Err("acknowledgement longer than its fields");
                        }
                        body[2]
                    }
                };
                Ok(Observed {
                    kind,
                    packet_id: Some(packet_id),
                    reason_code,
                    session_present: None,
                })
            }
            _ => Err("a packet a client of the subset does not expect"),
        }
    }
}

/// The broker's side: the connection adapter of the bench.
pub mod transport {
    use mqttiumd_contract::ids::InboundPacketId;
    use mqttiumd_contract::packet::{AckReason, InboundPacket, OutboundFrame, Qos, SharedBytes};

    use super::{Kind, no_properties, packet, split, take_string, take_u16};

    /// The bounds a decoded packet is copied within.
    #[derive(Clone, Copy, Debug)]
    pub struct Bounds {
        pub max_client_id: usize,
        pub max_topic: usize,
        pub max_payload: usize,
    }

    /// Decodes one whole packet into the validated packet of the core,
    /// with its kind and Packet Identifier for the trace.
    pub fn decode(
        bytes: &[u8],
        bounds: Bounds,
    ) -> Result<(InboundPacket, Kind, Option<u16>), &'static str> {
        let (first, body) = split(bytes)?;
        match first >> 4 {
            1 => {
                if first & 0x0f != 0 {
                    return Err("CONNECT with flags");
                }
                let (name, at) = take_string(body, 0)?;
                if name != b"MQTT" || body.get(at) != Some(&5) {
                    return Err("not MQTT 5");
                }
                let flags = *body.get(at + 1).ok_or("truncated CONNECT")?;
                if flags != 0 {
                    return Err("CONNECT outside the subset: clean start, Will or credentials");
                }
                let at = no_properties(body, at + 4)?;
                let (client_id, end) = take_string(body, at)?;
                if end != body.len() {
                    return Err("CONNECT longer than its fields");
                }
                let client_id = SharedBytes::copy_from(client_id, bounds.max_client_id)
                    .map_err(|_| "ClientID beyond its bound")?;
                Ok((
                    InboundPacket::ConnectResume { client_id },
                    Kind::Connect,
                    None,
                ))
            }
            3 => {
                let dup = first & 0x08 != 0;
                let qos = Qos::from_wire((first >> 1) & 0x03).ok_or("QoS 3")?;
                let retain = first & 0x01 != 0;
                let (topic, mut at) = take_string(body, 0)?;
                let packet_id = if qos == Qos::AtMostOnce {
                    None
                } else {
                    let raw = take_u16(body, at)?;
                    at += 2;
                    Some(InboundPacketId::new(raw).ok_or("Packet Identifier 0")?)
                };
                let at = no_properties(body, at)?;
                let payload = body.get(at..).ok_or("truncated PUBLISH")?;
                let topic = SharedBytes::copy_from(topic, bounds.max_topic)
                    .map_err(|_| "topic beyond its bound")?;
                let payload = SharedBytes::copy_from(payload, bounds.max_payload)
                    .map_err(|_| "payload beyond its bound")?;
                let raw = packet_id.map(InboundPacketId::get);
                Ok((
                    InboundPacket::Publish {
                        expiry: None,
                        topic,
                        qos,
                        retain,
                        dup,
                        packet_id,
                        payload,
                    },
                    Kind::Publish,
                    raw,
                ))
            }
            6 => {
                if first & 0x0f != 0x02 || body.len() != 2 {
                    return Err("PUBREL outside the subset");
                }
                let raw = take_u16(body, 0)?;
                let packet_id = InboundPacketId::new(raw).ok_or("Packet Identifier 0")?;
                Ok((InboundPacket::Pubrel { packet_id }, Kind::Pubrel, Some(raw)))
            }
            _ => Err("a packet outside the subset"),
        }
    }

    /// The reason code of an acknowledgement on the wire.
    #[must_use]
    pub const fn reason_code(reason: AckReason) -> u8 {
        match reason {
            AckReason::Success => 0x00,
            AckReason::PacketIdentifierNotFound => 0x92,
            AckReason::QuotaExceeded => 0x97,
            AckReason::ImplementationSpecificError => 0x83,
        }
    }

    /// The bytes of a frame: the short form when the reason is Success.
    pub fn encode(frame: OutboundFrame) -> Result<Vec<u8>, &'static str> {
        match frame {
            OutboundFrame::Suback { .. } | OutboundFrame::Unsuback { .. } => {
                Err("subscription acknowledgements outside the fixture profile")
            }
            OutboundFrame::Puback { packet_id, reason } => {
                let mut body = packet_id.get().to_be_bytes().to_vec();
                if reason.to_wire() != 0 {
                    body.push(reason.to_wire());
                }
                packet(0x40, &body)
            }
            OutboundFrame::Pingresp => packet(0xd0, &[]),
            OutboundFrame::Connack { session_present } => {
                packet(0x20, &[u8::from(session_present), 0x00, 0x00])
            }
            OutboundFrame::Pubrec { packet_id, reason } => ack(0x50, packet_id, reason),
            OutboundFrame::Pubcomp { packet_id, reason } => ack(0x70, packet_id, reason),
            OutboundFrame::Pubrel { packet_id } => {
                let [high, low] = packet_id.get().to_be_bytes();
                Ok(vec![0x62, 2, high, low])
            }
        }
    }

    fn ack(
        first: u8,
        packet_id: InboundPacketId,
        reason: AckReason,
    ) -> Result<Vec<u8>, &'static str> {
        let mut body = packet_id.get().to_be_bytes().to_vec();
        if reason != AckReason::Success {
            body.push(reason_code(reason));
        }
        packet(first, &body)
    }

    /// The trace name of a frame's kind and its Packet Identifier.
    #[must_use]
    pub fn describe(frame: OutboundFrame) -> (Kind, Option<u16>) {
        match frame {
            OutboundFrame::Suback { packet_id, .. } => (Kind::Suback, Some(packet_id.get())),
            OutboundFrame::Unsuback { packet_id, .. } => (Kind::Unsuback, Some(packet_id.get())),
            OutboundFrame::Puback { packet_id, .. } => (Kind::Puback, Some(packet_id.get())),
            OutboundFrame::Pingresp => (Kind::Pingresp, None),
            OutboundFrame::Connack { .. } => (Kind::Connack, None),
            OutboundFrame::Pubrec { packet_id, .. } => (Kind::Pubrec, Some(packet_id.get())),
            OutboundFrame::Pubcomp { packet_id, .. } => (Kind::Pubcomp, Some(packet_id.get())),
            OutboundFrame::Pubrel { packet_id } => (Kind::Pubrel, Some(packet_id.get())),
        }
    }
}

#[cfg(test)]
mod tests {
    use mqttiumd_contract::ids::InboundPacketId;
    use mqttiumd_contract::packet::{AckReason, InboundPacket, OutboundFrame};

    use super::{Kind, peer, transport};

    const BOUNDS: transport::Bounds = transport::Bounds {
        max_client_id: 64,
        max_topic: 64,
        max_payload: 64,
    };

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn the_bytes_of_p1_are_those_of_the_corpus() {
        assert_eq!(
            hex(&peer::connect(b"p1-producer").unwrap()),
            "101800044d5154540500000a00000b70312d70726f6475636572"
        );
        assert_eq!(
            hex(&peer::publish(b"p1/example", 2, false, false, Some(7), b"payload").unwrap()),
            "3416000a70312f6578616d706c650007007061796c6f6164"
        );
        assert_eq!(
            hex(&peer::publish(b"p1/example", 2, false, true, Some(7), b"payload").unwrap()),
            "3c16000a70312f6578616d706c650007007061796c6f6164"
        );
        assert_eq!(hex(&peer::pubrel(7).unwrap()), "62020007");
        let id = InboundPacketId::new(7).unwrap();
        let pubrec = OutboundFrame::Pubrec {
            packet_id: id,
            reason: AckReason::Success,
        };
        assert_eq!(hex(&transport::encode(pubrec).unwrap()), "50020007");
        let connack = OutboundFrame::Connack {
            session_present: true,
        };
        assert_eq!(hex(&transport::encode(connack).unwrap()), "2003010000");
    }

    #[test]
    fn each_side_reads_what_the_other_writes() {
        let bytes = peer::publish(b"t", 2, false, true, Some(9), b"x").unwrap();
        let (packet, kind, id) = transport::decode(&bytes, BOUNDS).unwrap();
        assert_eq!((kind, id), (Kind::Publish, Some(9)));
        assert!(matches!(packet, InboundPacket::Publish { dup: true, .. }));
        let refusal = OutboundFrame::Pubrec {
            packet_id: InboundPacketId::new(9).unwrap(),
            reason: AckReason::ImplementationSpecificError,
        };
        let observed = peer::observe(&transport::encode(refusal).unwrap()).unwrap();
        assert_eq!(observed.reason_code, 0x83);
        assert_eq!(observed.packet_id, Some(9));
    }

    #[test]
    fn bytes_that_are_not_one_whole_packet_are_refused() {
        let mut bytes = peer::pubrel(7).unwrap();
        bytes.push(0);
        assert!(transport::decode(&bytes, BOUNDS).is_err());
        assert!(transport::decode(&bytes[..3], BOUNDS).is_err());
        assert!(peer::observe(&[0x50, 0x02, 0x00]).is_err());
    }
}
