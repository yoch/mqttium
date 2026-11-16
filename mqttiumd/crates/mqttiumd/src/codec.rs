//! The bounded MQTT 5 codec of the experimental incoming QoS 2 profile.

use mqttiumd_contract::ids::InboundPacketId;
use mqttiumd_contract::packet::{AckReason, InboundPacket, OutboundFrame, Qos, SharedBytes};

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

struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }
    fn take(&mut self, len: usize) -> Result<&'a [u8], Error> {
        let end = self
            .at
            .checked_add(len)
            .ok_or_else(|| malformed("FieldLengthOverflow"))?;
        let result = self
            .bytes
            .get(self.at..end)
            .ok_or_else(|| malformed("TruncatedField"))?;
        self.at = end;
        Ok(result)
    }
    fn byte(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, Error> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }
    fn u32(&mut self) -> Result<u32, Error> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn variable(&mut self) -> Result<u32, Error> {
        let mut value = 0u32;
        for digit in 0..4 {
            let byte = self.byte()?;
            value |= u32::from(byte & 0x7f) << (digit * 7);
            if byte & 0x80 == 0 {
                return if digit > 0 && byte == 0 {
                    Err(malformed("NonCanonicalVariableInteger"))
                } else {
                    Ok(value)
                };
            }
        }
        Err(malformed("VariableIntegerTooLong"))
    }
    fn binary(&mut self) -> Result<&'a [u8], Error> {
        let len = usize::from(self.u16()?);
        self.take(len)
    }
    fn string(&mut self) -> Result<&'a [u8], Error> {
        let b = self.binary()?;
        utf8(b)?;
        Ok(b)
    }
    fn end(&self) -> Result<(), Error> {
        if self.at == self.bytes.len() {
            Ok(())
        } else {
            Err(malformed("TrailingFields"))
        }
    }
    fn is_empty(&self) -> bool {
        self.at == self.bytes.len()
    }
}

fn utf8(bytes: &[u8]) -> Result<(), Error> {
    let text = std::str::from_utf8(bytes).map_err(|_| malformed("InvalidUtf8"))?;
    // MQTT forbids NUL; the experimental profile also rejects the disallowed
    // controls and noncharacters of MQTT 5 section 1.5.4. BOM is preserved.
    if text.chars().any(|c| {
        let n = u32::from(c);
        n <= 0x1f
            || (0x7f..=0x9f).contains(&n)
            || (0xfdd0..=0xfdef).contains(&n)
            || n & 0xfffe == 0xfffe
    }) {
        return Err(malformed("DisallowedUtf8"));
    }
    Ok(())
}

fn topic(bytes: &[u8]) -> Result<(), Error> {
    if bytes.is_empty() || bytes.iter().any(|b| matches!(b, b'+' | b'#')) {
        return Err(Error {
            reason: 0x90,
            ..protocol("InvalidTopicName")
        });
    }
    Ok(())
}

fn filter(bytes: &[u8]) -> Result<bool, Error> {
    if bytes.is_empty() {
        return Err(protocol("EmptyTopicFilter"));
    }
    let (shared, bytes) = if let Some(rest) = bytes.strip_prefix(b"$share/") {
        let slash = rest
            .iter()
            .position(|b| *b == b'/')
            .ok_or_else(|| protocol("InvalidSharedFilter"))?;
        let group = &rest[..slash];
        if group.is_empty()
            || group.iter().any(|b| matches!(b, b'+' | b'#'))
            || slash + 1 == rest.len()
        {
            return Err(protocol("InvalidSharedFilter"));
        }
        (true, &rest[slash + 1..])
    } else {
        (false, bytes)
    };
    for (index, b) in bytes.iter().enumerate() {
        let boundary = index == 0 || bytes[index - 1] == b'/';
        if (*b == b'#' && (!boundary || index + 1 != bytes.len()))
            || (*b == b'+' && (!boundary || (index + 1 != bytes.len() && bytes[index + 1] != b'/')))
        {
            return Err(protocol("InvalidTopicFilter"));
        }
    }
    Ok(shared)
}

#[derive(Clone, Copy)]
enum Context {
    Connect,
    Will,
    Publish,
    Acknowledge,
    Disconnect,
    Subscribe,
    Unsubscribe,
    Auth,
}

impl Context {
    fn allows(self, id: u32) -> bool {
        match self {
            Self::Connect => matches!(
                id,
                0x11 | 0x15 | 0x16 | 0x17 | 0x19 | 0x21 | 0x22 | 0x26 | 0x27
            ),
            Self::Will => matches!(id, 0x01 | 0x02 | 0x03 | 0x08 | 0x09 | 0x18 | 0x26),
            Self::Publish => matches!(id, 0x01 | 0x02 | 0x03 | 0x08 | 0x09 | 0x23 | 0x26),
            Self::Acknowledge => matches!(id, 0x1f | 0x26),
            Self::Disconnect => matches!(id, 0x11 | 0x1f | 0x26),
            Self::Subscribe => matches!(id, 0x0b | 0x26),
            Self::Unsubscribe => id == 0x26,
            Self::Auth => matches!(id, 0x15 | 0x16 | 0x1f | 0x26),
        }
    }
}

enum Property<'a> {
    Byte(u8),
    Two(u16),
    Four(u32),
    Text(&'a [u8]),
    Other,
}

fn property<'a>(id: u32, input: &mut Cursor<'a>) -> Result<Property<'a>, Error> {
    let value = match id {
        0x01 | 0x17 | 0x19 | 0x24 | 0x25 | 0x28 | 0x29 | 0x2a => {
            let value = input.byte()?;
            if value > 1 {
                return Err(protocol("InvalidBooleanProperty"));
            }
            Property::Byte(value)
        }
        0x13 | 0x21 | 0x22 | 0x23 => {
            let value = input.u16()?;
            if id == 0x21 && value == 0 {
                return Err(protocol("ZeroReceiveMaximum"));
            }
            if id == 0x23 && value == 0 {
                return Err(Error {
                    reason: 0x94,
                    ..protocol("ZeroTopicAlias")
                });
            }
            Property::Two(value)
        }
        0x02 | 0x11 | 0x18 | 0x27 => {
            let value = input.u32()?;
            if id == 0x27 && value == 0 {
                return Err(protocol("ZeroMaximumPacketSize"));
            }
            Property::Four(value)
        }
        0x0b => {
            if input.variable()? == 0 {
                return Err(protocol("ZeroSubscriptionIdentifier"));
            }
            Property::Other
        }
        0x03 | 0x08 | 0x12 | 0x15 | 0x1a | 0x1c | 0x1f => Property::Text(input.string()?),
        0x09 | 0x16 => {
            input.binary()?;
            Property::Other
        }
        0x26 => {
            input.string()?;
            input.string()?;
            Property::Other
        }
        _ => return Err(malformed("UnknownProperty")),
    };
    if id == 0x08
        && let Property::Text(bytes) = value
    {
        topic(bytes)?;
    }
    Ok(value)
}

#[derive(Default)]
struct Properties {
    expiry: Option<u32>,
    max_packet: Option<u32>,
    receive_maximum: Option<u16>,
    alias_maximum: Option<u16>,
    problem_information: Option<bool>,
    unsupported: bool,
}

fn properties(input: &mut Cursor<'_>, context: Context) -> Result<Properties, Error> {
    let length =
        usize::try_from(input.variable()?).map_err(|_| oversized("PropertyLengthTooLarge"))?;
    let mut cursor = Cursor::new(input.take(length)?);
    let mut seen = 0u64;
    let mut result = Properties::default();
    let mut auth_method = false;
    let mut auth_data = false;
    while !cursor.is_empty() {
        let id = cursor.variable()?;
        let value = property(id, &mut cursor)?;
        if !context.allows(id) {
            return Err(protocol("PropertyNotAllowed"));
        }
        // Every known property identifier fits in this fixed bitmap. User
        // Property alone may repeat in the supported inbound packet roles.
        let mask = 1u64
            .checked_shl(id)
            .ok_or_else(|| malformed("UnknownProperty"))?;
        if id != 0x26 && seen & mask != 0 {
            return Err(protocol("DuplicateProperty"));
        }
        seen |= mask;
        match (context, id, value) {
            (Context::Connect, 0x11, Property::Four(n)) => result.expiry = Some(n),
            (Context::Connect, 0x27, Property::Four(n)) => result.max_packet = Some(n),
            (Context::Connect, 0x21, Property::Two(n)) => result.receive_maximum = Some(n),
            (Context::Connect, 0x22, Property::Two(n)) => result.alias_maximum = Some(n),
            (Context::Connect, 0x17, Property::Byte(n)) => {
                result.problem_information = Some(n != 0)
            }
            (Context::Connect, 0x19, Property::Byte(0)) => (),
            (Context::Disconnect, 0x11, Property::Four(n)) => result.expiry = Some(n),
            (Context::Publish, 0x23, _) => {
                return Err(Error {
                    reason: 0x94,
                    ..protocol("TopicAliasesDisabled")
                });
            }
            (_, 0x15, _) => {
                auth_method = true;
                result.unsupported = true;
            }
            (_, 0x16, _) => {
                auth_data = true;
                result.unsupported = true;
            }
            _ => result.unsupported = true,
        }
    }
    if auth_data && !auth_method {
        return Err(protocol("AuthenticationDataWithoutMethod"));
    }
    Ok(result)
}

fn client_connect(body: &[u8], bounds: Bounds) -> Result<Packet, Error> {
    let mut input = Cursor::new(body);
    if input.string()? != b"MQTT" {
        return Err(malformed("InvalidProtocolName"));
    }
    if input.byte()? != 5 {
        return Err(Error {
            reason: 0x84,
            ..unsupported("UnsupportedProtocolVersion")
        });
    }
    let flags = input.byte()?;
    if flags & 1 != 0 {
        return Err(malformed("ReservedConnectFlag"));
    }
    let will = flags & 4 != 0;
    if (!will && flags & 0x38 != 0) || (will && (flags >> 3) & 3 == 3) {
        return Err(protocol("InvalidWillFlags"));
    }
    let keep_alive = input.u16()?;
    let props = properties(&mut input, Context::Connect)?;
    let client_id = input.string()?;
    if will {
        properties(&mut input, Context::Will)?;
        topic(input.string()?)?;
        input.binary()?;
    }
    if flags & 0x80 != 0 {
        input.string()?;
    }
    if flags & 0x40 != 0 {
        input.binary()?;
    }
    input.end()?;
    if client_id.is_empty() {
        return Err(Error {
            reason: 0x85,
            ..unsupported("ExplicitClientIdRequired")
        });
    }
    if flags != 0 || props.expiry != Some(u32::MAX) || props.unsupported {
        return Err(unsupported("ConnectOutsideProfile"));
    }
    let client_id = SharedBytes::copy_from(client_id, bounds.max_client_id)
        .map_err(|_| oversized("ClientIdTooLarge"))?;
    Ok(Packet::Connect(Connect {
        client_id,
        keep_alive,
        max_packet_size: props.max_packet.unwrap_or(u32::MAX),
        receive_maximum: props.receive_maximum.unwrap_or(u16::MAX),
        topic_alias_maximum: props.alias_maximum.unwrap_or(0),
        request_problem_information: props.problem_information.unwrap_or(true),
    }))
}

fn identifier(input: &mut Cursor<'_>) -> Result<InboundPacketId, Error> {
    InboundPacketId::new(input.u16()?).ok_or_else(|| protocol("ZeroPacketIdentifier"))
}

fn publish(first: u8, body: &[u8], bounds: Bounds) -> Result<Packet, Error> {
    let mut input = Cursor::new(body);
    let topic_bytes = input.string()?;
    let qos = Qos::from_wire((first >> 1) & 3).ok_or_else(|| malformed("InvalidQos"))?;
    let packet_id = if qos == Qos::AtMostOnce {
        None
    } else {
        Some(identifier(&mut input)?)
    };
    let props = properties(&mut input, Context::Publish)?;
    topic(topic_bytes)?;
    let payload = input.take(body.len() - input.at)?;
    if props.unsupported {
        return Err(unsupported("PublishPropertiesOutsideProfile"));
    }
    if first & 1 != 0 {
        return Err(Error {
            reason: 0x9a,
            ..unsupported("RetainNotSupported")
        });
    }
    if qos != Qos::ExactlyOnce {
        return Err(unsupported("OnlyInboundQos2"));
    }
    let topic = SharedBytes::copy_from(topic_bytes, bounds.max_topic)
        .map_err(|_| oversized("TopicTooLarge"))?;
    let payload = SharedBytes::copy_from(payload, bounds.max_payload)
        .map_err(|_| oversized("PayloadTooLarge"))?;
    Ok(Packet::Core(InboundPacket::Publish {
        topic,
        qos,
        retain: false,
        dup: first & 8 != 0,
        packet_id,
        payload,
    }))
}

fn pubrel(body: &[u8]) -> Result<Packet, Error> {
    let mut input = Cursor::new(body);
    let packet_id = identifier(&mut input)?;
    let reason = if input.is_empty() { 0 } else { input.byte()? };
    if !matches!(reason, 0 | 0x92) {
        return Err(protocol("InvalidPubrelReason"));
    }
    let props = if input.is_empty() {
        Properties::default()
    } else {
        properties(&mut input, Context::Acknowledge)?
    };
    input.end()?;
    if reason != 0 || props.unsupported {
        return Err(unsupported("PubrelOutsideProfile"));
    }
    Ok(Packet::Core(InboundPacket::Pubrel { packet_id }))
}

fn disconnect(body: &[u8]) -> Result<Packet, Error> {
    let mut input = Cursor::new(body);
    let reason = if input.is_empty() { 0 } else { input.byte()? };
    if !matches!(
        reason,
        0 | 4
            | 0x80
            | 0x81
            | 0x82
            | 0x83
            | 0x87
            | 0x90
            | 0x93
            | 0x94
            | 0x95
            | 0x96
            | 0x97
            | 0x98
            | 0x99
    ) {
        return Err(protocol("InvalidDisconnectReason"));
    }
    let props = if input.is_empty() {
        Properties::default()
    } else {
        properties(&mut input, Context::Disconnect)?
    };
    input.end()?;
    if reason != 0 || props.unsupported || props.expiry.is_some_and(|n| n != u32::MAX) {
        return Err(unsupported("DisconnectOutsideProfile"));
    }
    Ok(Packet::Disconnect)
}

fn subscribe(body: &[u8], unsubscribe: bool) -> Result<Packet, Error> {
    let mut input = Cursor::new(body);
    identifier(&mut input)?;
    properties(
        &mut input,
        if unsubscribe {
            Context::Unsubscribe
        } else {
            Context::Subscribe
        },
    )?;
    if input.is_empty() {
        return Err(protocol("EmptySubscriptionList"));
    }
    while !input.is_empty() {
        let shared = filter(input.string()?)?;
        if !unsubscribe {
            let options = input.byte()?;
            if options & 0xc0 != 0
                || options & 3 == 3
                || (options >> 4) & 3 == 3
                || (shared && options & 4 != 0)
            {
                return Err(protocol("InvalidSubscriptionOptions"));
            }
        }
    }
    Err(unsupported("SubscriptionsOutsideProfile"))
}

/// Decode a complete packet body. No application-owned bytes are copied
/// until field and profile validation have completed.
pub fn decode(first: u8, body: &[u8], bounds: Bounds) -> Result<Packet, Error> {
    flags(first)?;
    if body.len() > 268_435_455 {
        return Err(oversized("PacketTooLarge"));
    }
    let variable = if body.len() < 128 {
        1
    } else if body.len() < 16_384 {
        2
    } else if body.len() < 2_097_152 {
        3
    } else {
        4
    };
    if body
        .len()
        .checked_add(variable + 1)
        .is_none_or(|n| n > bounds.max_packet)
    {
        return Err(oversized("PacketTooLarge"));
    }
    match first >> 4 {
        1 => client_connect(body, bounds),
        3 => publish(first, body, bounds),
        6 => pubrel(body),
        8 => subscribe(body, false),
        10 => subscribe(body, true),
        12 if body.is_empty() => Ok(Packet::Core(InboundPacket::Pingreq)),
        12 => Err(malformed("PingreqPayload")),
        14 => disconnect(body),
        15 => {
            let mut input = Cursor::new(body);
            let reason = if input.is_empty() { 0 } else { input.byte()? };
            if !matches!(reason, 0 | 0x18 | 0x19) {
                return Err(protocol("InvalidAuthReason"));
            }
            if !input.is_empty() {
                properties(&mut input, Context::Auth)?;
            }
            input.end()?;
            Err(protocol("AuthenticationNotNegotiated"))
        }
        _ => Err(protocol("UnexpectedClientPacketType")),
    }
}

/// Encode only frames the pure core can authorize. The transport must still
/// compare the result with the peer's Maximum Packet Size before writing.
pub fn encode(frame: OutboundFrame) -> Result<Vec<u8>, Error> {
    let ack = |first: u8, packet_id: InboundPacketId, reason: AckReason| {
        let reason = match reason {
            AckReason::Success => 0,
            AckReason::PacketIdentifierNotFound => 0x92,
            AckReason::QuotaExceeded => 0x97,
            AckReason::ImplementationSpecificError => 0x83,
        };
        if (first == 0x70 && !matches!(reason, 0 | 0x92)) || (first == 0x50 && reason == 0x92) {
            return Err(protocol("InvalidOutboundAckReason"));
        }
        let id = packet_id.get().to_be_bytes();
        Ok(if reason == 0 {
            vec![first, 2, id[0], id[1]]
        } else {
            vec![first, 3, id[0], id[1], reason]
        })
    };
    match frame {
        OutboundFrame::Connack { session_present } => Ok(vec![
            0x20,
            22,
            u8::from(session_present),
            0,
            19,
            0x21,
            0,
            8,
            0x27,
            0,
            1,
            0,
            0,
            0x25,
            0,
            0x22,
            0,
            0,
            0x28,
            0,
            0x29,
            0,
            0x2a,
            0,
        ]),
        OutboundFrame::Pubrec { packet_id, reason } => ack(0x50, packet_id, reason),
        OutboundFrame::Pubcomp { packet_id, reason } => ack(0x70, packet_id, reason),
        OutboundFrame::Pingresp => Ok(vec![0xd0, 0]),
    }
}

pub fn refusal(error: Error, before_connack: bool) -> Vec<u8> {
    let reason = if error.reason >= 0x80 {
        error.reason
    } else {
        0x83
    };
    if before_connack {
        // Do not emit a reason reserved to another packet role in CONNACK.
        let reason = if matches!(reason,0x80..=0x8a|0x8c|0x90|0x95|0x97|0x99..=0x9d|0x9f) {
            reason
        } else {
            0x83
        };
        vec![0x20, 3, 0, reason, 0]
    } else {
        vec![0xe0, 2, reason, 0]
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
