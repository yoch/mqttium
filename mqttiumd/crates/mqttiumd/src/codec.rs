//! Bounded MQTT framing and validation for the documented product profiles.

use std::sync::Arc;

use mqttiumd_contract::ids::{InboundPacketId, OutboundPacketId};
use mqttiumd_contract::packet::{
    AckReason, InboundPacket, OutboundFrame, PubackReason, Qos, Qos2Ack, SharedBytes,
};
use mqttiumd_contract::properties::{
    MAX_PROPERTY_BYTES, MAX_USER_PROPERTIES, PublishProperties, varint_len,
};
use mqttiumd_contract::pubsub::{FilterList, MAX_FILTERS, SubscriptionFilter, SubscriptionOptions};
use mqttiumd_contract::session::SessionExpiry;
use mqttiumd_contract::will::{MAX_WILL_PAYLOAD_BYTES, MAX_WILL_TOPIC_BYTES, WillSpec};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Version {
    V311 = 4,
    #[default]
    V5 = 5,
}

impl Version {
    pub const fn level(self) -> u8 {
        self as u8
    }
}

/// The common protocol name identifies the CONNECT envelope. A level other
/// than 4 or 5 uses the legacy Unsupported Protocol Version return code.
pub fn connect_response_version(body: &[u8]) -> Option<Version> {
    let level = *body.strip_prefix(b"\0\x04MQTT")?.first()?;
    Some(if level == 5 {
        Version::V5
    } else {
        Version::V311
    })
}

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
    pub version: Version,
    pub will: Option<WillSpec>,
    pub client_id: SharedBytes,
    pub clean_start: bool,
    pub expiry: SessionExpiry,
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
    /// A validated interval awaiting its receipt-time observation in the runtime.
    TimedPublish {
        packet: InboundPacket,
        interval: u32,
    },
    Disconnect {
        expiry: Option<SessionExpiry>,
        publish_will: bool,
    },
    /// A valid SUBSCRIBE or UNSUBSCRIBE the core does not take as one
    /// request: more filters than its request holds, or a filter longer
    /// than the profile stores. It is served as the unit requests `parts`,
    /// in order, and acknowledged once. `answers` has one entry per filter
    /// of the request: the code of a filter the profile cannot store, or
    /// `None` for a filter given to the core. The size of the packet is the
    /// only bound on the number of filters.
    Requests {
        packet_id: InboundPacketId,
        unsubscribe: bool,
        parts: Vec<InboundPacket>,
        answers: Vec<Option<u8>>,
    },
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
    peer_maximum: Option<u32>,
    disconnect_expiry: Option<u32>,
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
    /// Peer response limit established by a fully validated CONNECT.
    pub const fn peer_maximum(self) -> Option<u32> {
        self.peer_maximum
    }
    /// The packet was structurally validated before profile refusal. MQTT's
    /// CONNECT0→DISCONNECTnonzero context rule also applies to an otherwise
    /// unsupported DISCONNECT feature.
    pub fn in_session(self, connect_expiry: SessionExpiry) -> Self {
        if connect_expiry == SessionExpiry::OnDisconnect
            && self.disconnect_expiry.is_some_and(|value| value != 0)
        {
            protocol("SessionExpiryIncreaseForbidden")
        } else {
            self
        }
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
        peer_maximum: None,
        disconnect_expiry: None,
    }
}

const fn protocol(code: &'static str) -> Error {
    Error {
        kind: ErrorKind::Protocol,
        code,
        reason: 0x82,
        peer_maximum: None,
        disconnect_expiry: None,
    }
}

const fn unsupported(code: &'static str) -> Error {
    Error {
        kind: ErrorKind::Unsupported,
        code,
        reason: 0x83,
        peer_maximum: None,
        disconnect_expiry: None,
    }
}

const fn oversized(code: &'static str) -> Error {
    Error {
        kind: ErrorKind::Oversized,
        code,
        reason: 0x95,
        peer_maximum: None,
        disconnect_expiry: None,
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

pub(crate) fn utf8(bytes: &[u8]) -> Result<(), Error> {
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

pub(crate) fn topic(bytes: &[u8]) -> Result<(), Error> {
    if bytes.is_empty() || bytes.iter().any(|b| matches!(b, b'+' | b'#')) {
        return Err(Error {
            reason: 0x90,
            ..protocol("InvalidTopicName")
        });
    }
    Ok(())
}

/// Validates a topic filter and says whether it is a shared subscription.
/// The `$share/` form belongs to MQTT 5: in 3.1.1 such a filter is an
/// ordinary one.
fn filter(version: Version, bytes: &[u8]) -> Result<bool, Error> {
    if bytes.is_empty() {
        return Err(protocol("EmptyTopicFilter"));
    }
    let shared_form = if version == Version::V5 {
        bytes.strip_prefix(b"$share/")
    } else {
        None
    };
    let (shared, bytes) = if let Some(rest) = shared_form {
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
    Binary(&'a [u8]),
    Pair(&'a [u8], &'a [u8]),
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
        0x09 | 0x16 => Property::Binary(input.binary()?),
        0x26 => Property::Pair(input.string()?, input.string()?),
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
struct Properties<'a> {
    expiry: Option<u32>,
    message_expiry: Option<u32>,
    will_delay: Option<u32>,
    max_packet: Option<u32>,
    receive_maximum: Option<u16>,
    alias_maximum: Option<u16>,
    problem_information: Option<bool>,
    payload_format_indicator: Option<u8>,
    content_type: Option<&'a [u8]>,
    response_topic: Option<&'a [u8]>,
    correlation_data: Option<&'a [u8]>,
    user_properties: Vec<(&'a [u8], &'a [u8])>,
    unsupported: bool,
    /// An Authentication Method was given: no method is supported.
    authentication: bool,
}

fn properties<'a>(input: &mut Cursor<'a>, context: Context) -> Result<Properties<'a>, Error> {
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
            // The server may return Response Information when asked; it
            // returns none.
            (Context::Connect, 0x19, Property::Byte(_)) => (),
            // Outside a publication and a Will, which must forward them,
            // the protocol gives a server nothing to do with a User
            // Property; nor with the Reason String of an acknowledgement
            // or of a DISCONNECT. They are validated and left unused.
            (
                Context::Connect
                | Context::Subscribe
                | Context::Unsubscribe
                | Context::Acknowledge
                | Context::Disconnect,
                0x26,
                _,
            )
            | (Context::Acknowledge | Context::Disconnect, 0x1f, _) => (),
            (Context::Disconnect, 0x11, Property::Four(n)) => result.expiry = Some(n),
            (Context::Publish, 0x01, Property::Byte(n)) => {
                result.payload_format_indicator = Some(n)
            }
            (Context::Publish, 0x02, Property::Four(n)) => result.message_expiry = Some(n),
            (Context::Publish, 0x03, Property::Text(s)) => result.content_type = Some(s),
            (Context::Publish, 0x08, Property::Text(s)) => result.response_topic = Some(s),
            (Context::Publish, 0x09, Property::Binary(b)) => result.correlation_data = Some(b),
            (Context::Publish, 0x26, Property::Pair(k, v)) => {
                if result.user_properties.len() >= MAX_USER_PROPERTIES {
                    return Err(oversized("TooManyUserProperties"));
                }
                result.user_properties.push((k, v));
            }
            (Context::Will, 0x18, Property::Four(n)) => result.will_delay = Some(n),
            (Context::Publish, 0x23, _) => {
                return Err(Error {
                    reason: 0x94,
                    ..protocol("TopicAliasesDisabled")
                });
            }
            (Context::Subscribe, 0x0b, _) => {
                // Subscription Identifiers Available 0 was announced.
                return Err(Error {
                    reason: 0xa1,
                    ..protocol("SubscriptionIdentifiersNotSupported")
                });
            }
            (_, 0x15, _) => {
                auth_method = true;
                result.authentication = true;
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
    let version = match input.byte()? {
        4 => Version::V311,
        5 => Version::V5,
        _ => {
            return Err(Error {
                reason: 0x84,
                peer_maximum: Some(u32::MAX),
                ..unsupported("UnsupportedProtocolVersion")
            });
        }
    };
    let flags = input.byte()?;
    if flags & 1 != 0 {
        return Err(malformed("ReservedConnectFlag"));
    }
    if version == Version::V311 && flags & 0xc0 == 0x40 {
        return Err(malformed("PasswordWithoutUsername"));
    }
    let will = flags & 4 != 0;
    if (!will && flags & 0x38 != 0) || (will && (flags >> 3) & 3 == 3) {
        return Err(protocol("InvalidWillFlags"));
    }
    let keep_alive = input.u16()?;
    let props = versioned_properties(version, &mut input, Context::Connect)?;
    let client_id = input.string()?;
    let will_fields = if will {
        let will_properties = versioned_properties(version, &mut input, Context::Will)?;
        let will_topic = input.string()?;
        topic(will_topic)?;
        let will_payload = input.binary()?;
        Some((will_topic, will_payload, will_properties))
    } else {
        None
    };
    if flags & 0x80 != 0 {
        input.string()?;
    }
    if flags & 0x40 != 0 {
        input.binary()?;
    }
    input.end()?;
    // These response limits are trusted only after every field has been
    // validated. A malformed or truncated CONNECT supplies no such context.
    let peer_maximum = Some(props.max_packet.unwrap_or(u32::MAX));
    if client_id.is_empty() {
        return Err(Error {
            reason: 0x85,
            peer_maximum,
            ..unsupported("ExplicitClientIdRequired")
        });
    }
    let clean_start = flags & 2 != 0;
    let expiry = if version == Version::V311 && !clean_start {
        Some(SessionExpiry::Never)
    } else {
        SessionExpiry::from_wire(props.expiry.unwrap_or(0))
    };
    if version == Version::V311 && flags & 0xc0 != 0 {
        return Err(Error {
            reason: 0x86,
            peer_maximum,
            ..unsupported("CredentialsOutsideProfile")
        });
    }
    let will_outside_profile = will_fields.as_ref().is_some_and(|(_, _, properties)| {
        ((flags >> 3) & 3) != 1
            || properties.will_delay.is_some_and(|delay| delay != 0)
            || properties.unsupported
    });
    if props.authentication {
        return Err(Error {
            peer_maximum,
            reason: 0x8c,
            ..unsupported("AuthenticationMethodNotSupported")
        });
    }
    if flags & !(2 | 0x0c | 0x20) != 0
        || expiry.is_none()
        || props.unsupported
        || will_outside_profile
    {
        return Err(Error {
            peer_maximum,
            ..unsupported("ConnectOutsideProfile")
        });
    }
    // Validate every owned field against both its negotiated/product budget
    // and the durable Will format before copying any Will bytes.
    if will_fields
        .as_ref()
        .is_some_and(|(topic, _, _)| topic.len() > bounds.max_topic.min(MAX_WILL_TOPIC_BYTES))
    {
        return Err(Error {
            peer_maximum,
            ..oversized("WillTopicTooLarge")
        });
    }
    if will_fields.as_ref().is_some_and(|(_, payload, _)| {
        payload.len() > bounds.max_payload.min(MAX_WILL_PAYLOAD_BYTES)
    }) {
        return Err(Error {
            peer_maximum,
            ..oversized("WillPayloadTooLarge")
        });
    }
    let client_id = SharedBytes::copy_from(client_id, bounds.max_client_id).map_err(|_| Error {
        peer_maximum,
        reason: if version == Version::V311 { 0x85 } else { 0x95 },
        ..oversized("ClientIdTooLarge")
    })?;
    let will = will_fields
        .map(|(topic, payload, _)| {
            Ok(WillSpec {
                retain: flags & 0x20 != 0,
                topic: SharedBytes::copy_from(topic, bounds.max_topic.min(MAX_WILL_TOPIC_BYTES))
                    .map_err(|_| Error {
                        peer_maximum,
                        ..oversized("WillTopicTooLarge")
                    })?,
                payload: SharedBytes::copy_from(
                    payload,
                    bounds.max_payload.min(MAX_WILL_PAYLOAD_BYTES),
                )
                .map_err(|_| Error {
                    peer_maximum,
                    ..oversized("WillPayloadTooLarge")
                })?,
            })
        })
        .transpose()?;
    Ok(Packet::Connect(Connect {
        version,
        will,
        client_id,
        clean_start,
        expiry: expiry.ok_or_else(|| unsupported("SessionExpiryOutsideProfile"))?,
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

fn versioned_properties<'a>(
    version: Version,
    input: &mut Cursor<'a>,
    context: Context,
) -> Result<Properties<'a>, Error> {
    if version == Version::V5 {
        properties(input, context)
    } else {
        Ok(Properties::default())
    }
}

fn publish(version: Version, first: u8, body: &[u8], bounds: Bounds) -> Result<Packet, Error> {
    let mut input = Cursor::new(body);
    let topic_bytes = input.string()?;
    let qos = Qos::from_wire((first >> 1) & 3).ok_or_else(|| malformed("InvalidQos"))?;
    let packet_id = if qos == Qos::AtMostOnce {
        None
    } else {
        Some(identifier(&mut input)?)
    };
    let props = versioned_properties(version, &mut input, Context::Publish)?;
    topic(topic_bytes)?;
    let payload = input.take(body.len() - input.at)?;
    if props.unsupported {
        return Err(unsupported("PublishPropertiesOutsideProfile"));
    }
    if props.payload_format_indicator == Some(1) && std::str::from_utf8(payload).is_err() {
        return Err(Error {
            kind: ErrorKind::Malformed,
            code: "PayloadFormatInvalid",
            reason: 0x99,
            peer_maximum: None,
            disconnect_expiry: None,
        });
    }
    let has_props = props.payload_format_indicator.is_some()
        || props.content_type.is_some()
        || props.response_topic.is_some()
        || props.correlation_data.is_some()
        || !props.user_properties.is_empty();
    if has_props {
        let logical_bytes = usize::from(props.payload_format_indicator.is_some())
            .saturating_add(props.content_type.map_or(0, <[u8]>::len))
            .saturating_add(props.response_topic.map_or(0, <[u8]>::len))
            .saturating_add(props.correlation_data.map_or(0, <[u8]>::len))
            .saturating_add(
                props
                    .user_properties
                    .iter()
                    .map(|(k, v)| k.len().saturating_add(v.len()))
                    .sum::<usize>(),
            );
        if logical_bytes > MAX_PROPERTY_BYTES {
            return Err(oversized("PublishPropertiesTooLarge"));
        }
    }
    // Check every field before making any application-owned copy.
    if topic_bytes.len() > bounds.max_topic {
        return Err(oversized("TopicTooLarge"));
    }
    if payload.len() > bounds.max_payload {
        return Err(oversized("PayloadTooLarge"));
    }
    let publish_props = if has_props {
        let content_type = props
            .content_type
            .map(|s| SharedBytes::copy_from(s, MAX_PROPERTY_BYTES))
            .transpose()
            .map_err(|_| oversized("PublishPropertiesTooLarge"))?;
        let response_topic = props
            .response_topic
            .map(|s| SharedBytes::copy_from(s, MAX_PROPERTY_BYTES))
            .transpose()
            .map_err(|_| oversized("PublishPropertiesTooLarge"))?;
        let correlation_data = props
            .correlation_data
            .map(|b| SharedBytes::copy_from(b, MAX_PROPERTY_BYTES))
            .transpose()
            .map_err(|_| oversized("PublishPropertiesTooLarge"))?;
        let mut pairs = Vec::with_capacity(props.user_properties.len());
        for (k, v) in props.user_properties {
            let key = SharedBytes::copy_from(k, MAX_PROPERTY_BYTES)
                .map_err(|_| oversized("PublishPropertiesTooLarge"))?;
            let val = SharedBytes::copy_from(v, MAX_PROPERTY_BYTES)
                .map_err(|_| oversized("PublishPropertiesTooLarge"))?;
            pairs.push((key, val));
        }
        Some(Arc::new(PublishProperties {
            payload_format_indicator: props.payload_format_indicator,
            content_type,
            response_topic,
            correlation_data,
            user_properties: Arc::from(pairs),
        }))
    } else {
        None
    };
    let topic = SharedBytes::copy_from(topic_bytes, bounds.max_topic)
        .map_err(|_| oversized("TopicTooLarge"))?;
    let payload = SharedBytes::copy_from(payload, bounds.max_payload)
        .map_err(|_| oversized("PayloadTooLarge"))?;
    let packet = InboundPacket::Publish {
        topic,
        qos,
        retain: first & 1 != 0,
        dup: first & 8 != 0,
        packet_id,
        expiry: None,
        properties: publish_props,
        payload,
    };
    Ok(match props.message_expiry {
        Some(interval) => Packet::TimedPublish { packet, interval },
        None => Packet::Core(packet),
    })
}

fn pubrel(version: Version, body: &[u8]) -> Result<Packet, Error> {
    let mut input = Cursor::new(body);
    let packet_id = identifier(&mut input)?;
    if version == Version::V311 {
        input.end()?;
        return Ok(Packet::Core(InboundPacket::Pubrel { packet_id }));
    }
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
        0 | 4 | 0x80 | 0x81 | 0x82 | 0x83 | 0x90 | 0x93 | 0x94 | 0x95 | 0x96 | 0x97 | 0x98 | 0x99
    ) {
        return Err(protocol("InvalidDisconnectReason"));
    }
    let props = if input.is_empty() {
        Properties::default()
    } else {
        properties(&mut input, Context::Disconnect)?
    };
    input.end()?;
    if !matches!(reason, 0 | 4)
        || props.unsupported
        || props
            .expiry
            .is_some_and(|n| SessionExpiry::from_wire(n).is_none())
    {
        return Err(Error {
            disconnect_expiry: props.expiry,
            ..unsupported("DisconnectOutsideProfile")
        });
    }
    Ok(Packet::Disconnect {
        expiry: props.expiry.and_then(SessionExpiry::from_wire),
        publish_will: reason == 4,
    })
}

/// A refused establishment has no session key and never advertises SP1.
pub fn session_refusal(reason: u8) -> Result<Vec<u8>, Error> {
    session_refusal_for(Version::V5, reason)
}

pub fn session_refusal_for(version: Version, reason: u8) -> Result<Vec<u8>, Error> {
    if !matches!(reason, 0x80 | 0x82 | 0x83 | 0x87 | 0x89 | 0x97) {
        return Err(protocol("InvalidConnackRefusal"));
    }
    Ok(match version {
        Version::V5 => vec![0x20, 3, 0, reason, 0],
        Version::V311 => legacy_connack_refusal(reason),
    })
}

fn legacy_connack_refusal(reason: u8) -> Vec<u8> {
    let code = match reason {
        0x84 => 1, // Unacceptable protocol version.
        0x85 => 2, // Identifier rejected.
        0x86 => 4, // Bad username or password.
        0x87 => 5, // Not authorized.
        _ => 3,    // Server unavailable.
    };
    vec![0x20, 2, 0, code]
}

/// The lifecycle runtime uses this bounded control frame before physical close.
/// The Maximum Packet Size the CONNACK announces. It is a promise that no
/// packet within it is refused for its size, so it follows the payload
/// limit, not the larger frame the transport reads: the PUBLISH with the
/// least overhead (one-byte topic, QoS 0, no property) spends eight bytes
/// around its payload once the Remaining Length takes three. A client that
/// honours it never sends a payload over the limit. The transport still
/// reads frames up to its own bound, which a CONNECT with a large Will
/// needs before any announcement; the runtime refuses every later MQTT 5
/// packet larger than the announcement.
pub const ANNOUNCED_PACKET_BYTES: u32 = 1_048_576 + 8;
const ANNOUNCED: [u8; 4] = ANNOUNCED_PACKET_BYTES.to_be_bytes();

/// The size of a packet on the wire: its first byte, the bytes of its
/// Remaining Length and its body.
#[must_use]
pub const fn wire_size(remaining: usize) -> usize {
    let length = if remaining < 128 {
        1
    } else if remaining < 16_384 {
        2
    } else if remaining < 2_097_152 {
        3
    } else {
        4
    };
    1 + length + remaining
}

pub fn disconnect_reason(reason: u8) -> Vec<u8> {
    vec![0xe0, 2, reason, 0]
}

fn subscribe(
    version: Version,
    body: &[u8],
    unsubscribe: bool,
    bounds: Bounds,
) -> Result<Packet, Error> {
    let mut input = Cursor::new(body);
    let packet_id = identifier(&mut input)?;
    let props = versioned_properties(
        version,
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
    let mut parsed = Vec::with_capacity(MAX_FILTERS);
    let outside = props.unsupported;
    // What a filter longer than the profile stores is answered: it holds
    // no subscription and can hold none.
    let unstorable = match (unsubscribe, version) {
        (false, Version::V5) => 0x97,
        (false, Version::V311) => 0x80,
        (true, _) => 0x11,
    };
    let mut count = 0usize;
    let mut answers = Vec::with_capacity(MAX_FILTERS);
    while !input.is_empty() {
        let text = input.string()?;
        let shared = filter(version, text)?;
        let mut qos = Qos::AtLeastOnce;
        let mut subscription_options = SubscriptionOptions::DEFAULT;
        if !unsubscribe {
            let options = input.byte()?;
            let reserved = if version == Version::V311 {
                options & !3
            } else {
                options & 0xc0
            };
            if reserved != 0 {
                return Err(malformed("ReservedSubscriptionOptionBits"));
            }
            if options & 3 == 3 || (options >> 4) & 3 == 3 || (shared && options & 4 != 0) {
                return Err(protocol("InvalidSubscriptionOptions"));
            }
            if shared {
                // Shared Subscription Available 0 was announced.
                return Err(Error {
                    reason: 0x9e,
                    ..protocol("SharedSubscriptionsNotSupported")
                });
            }
            qos = Qos::from_wire(options & 3).ok_or_else(|| protocol("InvalidSubscriptionQos"))?;
            subscription_options = SubscriptionOptions::from_bits(options & !3)
                .ok_or_else(|| protocol("InvalidSubscriptionOptions"))?;
        }
        count += 1;
        if text.len() > bounds.max_topic {
            answers.push(Some(unstorable));
        } else {
            answers.push(None);
            parsed.push((text, qos, subscription_options));
        }
    }
    if outside {
        return Err(unsupported("SubscriptionOptionsOutsideProfile"));
    }
    let whole = parsed.len() == count && count <= MAX_FILTERS;
    let mut parts = Vec::with_capacity(parsed.len().div_ceil(MAX_FILTERS));
    for unit in parsed.chunks(MAX_FILTERS) {
        let part = if unsubscribe {
            let mut filters = Vec::with_capacity(unit.len());
            for (text, _, _) in unit {
                filters.push(
                    SharedBytes::copy_from(text, bounds.max_topic)
                        .map_err(|_| oversized("FilterTooLarge"))?,
                );
            }
            InboundPacket::Unsubscribe {
                packet_id,
                filters: FilterList::copy_from(&filters)
                    .map_err(|_| oversized("TooManyFilters"))?,
            }
        } else {
            let mut filters = Vec::with_capacity(unit.len());
            for (text, qos, options) in unit {
                filters.push(SubscriptionFilter {
                    filter: SharedBytes::copy_from(text, bounds.max_topic)
                        .map_err(|_| oversized("FilterTooLarge"))?,
                    qos: *qos,
                    options: *options,
                });
            }
            InboundPacket::Subscribe {
                packet_id,
                filters: FilterList::copy_from(&filters)
                    .map_err(|_| oversized("TooManyFilters"))?,
                denied: 0,
            }
        };
        parts.push(part);
    }
    if whole {
        // The ordinary case: one request the core takes as it is.
        return Ok(Packet::Core(parts.remove(0)));
    }
    Ok(Packet::Requests {
        packet_id,
        unsubscribe,
        parts,
        answers,
    })
}

fn puback(version: Version, body: &[u8]) -> Result<Packet, Error> {
    let mut input = Cursor::new(body);
    let packet_id =
        OutboundPacketId::new(input.u16()?).ok_or_else(|| protocol("ZeroPacketIdentifier"))?;
    if version == Version::V311 {
        input.end()?;
        return Ok(Packet::Core(InboundPacket::Puback {
            packet_id,
            reason: 0,
        }));
    }
    let reason = if input.is_empty() { 0 } else { input.byte()? };
    if !matches!(
        reason,
        0 | 0x10 | 0x80 | 0x83 | 0x87 | 0x90 | 0x91 | 0x97 | 0x99
    ) {
        return Err(protocol("InvalidPubackReason"));
    }
    if !input.is_empty() {
        properties(&mut input, Context::Acknowledge)?;
    }
    input.end()?;
    Ok(Packet::Core(InboundPacket::Puback { packet_id, reason }))
}

fn qos2_ack(version: Version, kind: Qos2Ack, body: &[u8]) -> Result<Packet, Error> {
    let mut input = Cursor::new(body);
    let packet_id =
        OutboundPacketId::new(input.u16()?).ok_or_else(|| protocol("ZeroPacketIdentifier"))?;
    let reason = if version == Version::V311 || input.is_empty() {
        0
    } else {
        input.byte()?
    };
    if !kind.accepts(reason) {
        return Err(protocol("InvalidQos2AckReason"));
    }
    if version == Version::V5 && !input.is_empty() {
        properties(&mut input, Context::Acknowledge)?;
    }
    input.end()?;
    Ok(Packet::Core(InboundPacket::Qos2Ack {
        kind,
        packet_id,
        reason,
    }))
}

/// Decode a complete packet body. No application-owned bytes are copied
/// until field and profile validation have completed.
pub fn decode(first: u8, body: &[u8], bounds: Bounds) -> Result<Packet, Error> {
    decode_for(Version::V5, first, body, bounds)
}

pub fn decode_for(
    version: Version,
    first: u8,
    body: &[u8],
    bounds: Bounds,
) -> Result<Packet, Error> {
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
        3 => publish(version, first, body, bounds),
        4 => puback(version, body),
        5 => qos2_ack(version, Qos2Ack::Pubrec, body),
        6 => pubrel(version, body),
        7 => qos2_ack(version, Qos2Ack::Pubcomp, body),
        8 => subscribe(version, body, false, bounds),
        10 => subscribe(version, body, true, bounds),
        12 if body.is_empty() => Ok(Packet::Core(InboundPacket::Pingreq)),
        12 => Err(malformed("PingreqPayload")),
        14 if version == Version::V311 && !body.is_empty() => Err(malformed("DisconnectPayload")),
        14 => disconnect(body),
        15 if version == Version::V5 => {
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
        OutboundFrame::Suback { packet_id, results }
        | OutboundFrame::Unsuback { packet_id, results } => {
            let codes = results
                .as_slice()
                .filter(|v| !v.is_empty())
                .ok_or_else(|| protocol("InvalidFilterResults"))?;
            let sub = matches!(frame, OutboundFrame::Suback { .. });
            if codes.iter().any(|code| {
                if sub {
                    !matches!(
                        code,
                        0 | 1 | 2 | 0x80 | 0x83 | 0x87 | 0x8f | 0x91 | 0x97 | 0x9e | 0xa1 | 0xa2
                    )
                } else {
                    !matches!(code, 0 | 0x11 | 0x80 | 0x83 | 0x87 | 0x8f | 0x91)
                }
            }) {
                return Err(protocol("InvalidFilterReason"));
            }
            let [high, low] = packet_id.get().to_be_bytes();
            let mut bytes = vec![
                if sub { 0x90 } else { 0xb0 },
                (3 + codes.len()) as u8,
                high,
                low,
                0,
            ];
            bytes.extend_from_slice(codes);
            Ok(bytes)
        }
        OutboundFrame::Puback { packet_id, reason } => {
            let [high, low] = packet_id.get().to_be_bytes();
            if reason.to_wire() == 0 {
                Ok(vec![0x40, 2, high, low])
            } else {
                Ok(vec![0x40, 3, high, low, reason.to_wire()])
            }
        }
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
            ANNOUNCED[0],
            ANNOUNCED[1],
            ANNOUNCED[2],
            ANNOUNCED[3],
            0x25,
            1,
            0x22,
            0,
            0,
            0x28,
            1,
            0x29,
            0,
            0x2a,
            0,
        ]),
        OutboundFrame::Pubrec { packet_id, reason } => ack(0x50, packet_id, reason),
        OutboundFrame::Pubcomp { packet_id, reason } => ack(0x70, packet_id, reason),
        OutboundFrame::Pubrel { packet_id } => {
            let [high, low] = packet_id.get().to_be_bytes();
            Ok(vec![0x62, 2, high, low])
        }
        OutboundFrame::Pingresp => Ok(vec![0xd0, 0]),
    }
}

pub fn encode_for(version: Version, frame: OutboundFrame) -> Result<Vec<u8>, Error> {
    if version == Version::V5 {
        return encode(frame);
    }
    match frame {
        OutboundFrame::Connack { session_present } => {
            Ok(vec![0x20, 2, u8::from(session_present), 0])
        }
        OutboundFrame::Puback {
            reason: PubackReason::Success,
            ..
        }
        | OutboundFrame::Pubrec {
            reason: AckReason::Success,
            ..
        }
        | OutboundFrame::Pubcomp {
            reason: AckReason::Success,
            ..
        }
        | OutboundFrame::Pubrel { .. }
        | OutboundFrame::Pingresp => encode(frame),
        // MQTT 3.1.1 has no reason code: a publication accepted without a
        // matching subscription is acknowledged like any other (record
        // 0049).
        OutboundFrame::Puback {
            packet_id,
            reason: PubackReason::NoMatchingSubscribers,
        } => encode(OutboundFrame::Puback {
            packet_id,
            reason: PubackReason::Success,
        }),
        OutboundFrame::Pubcomp {
            packet_id,
            reason: AckReason::PacketIdentifierNotFound,
        } => {
            // MQTT 3.1.1 ends a replayed PUBREL with the same identifier.
            let [high, low] = packet_id.get().to_be_bytes();
            Ok(vec![0x70, 2, high, low])
        }
        OutboundFrame::Suback { packet_id, results } => {
            let codes = results
                .as_slice()
                .filter(|codes| !codes.is_empty())
                .ok_or_else(|| protocol("InvalidFilterResults"))?;
            let [high, low] = packet_id.get().to_be_bytes();
            let mut bytes = vec![0x90, (2 + codes.len()) as u8, high, low];
            bytes.extend(
                codes
                    .iter()
                    .map(|&code| if code <= 2 { code } else { 0x80 }),
            );
            Ok(bytes)
        }
        // 3.1.1 acknowledges the request, also where nothing is removed.
        // A filter the broker refuses as invalid holds no subscription;
        // any other failure could leave one and is not acknowledged.
        OutboundFrame::Unsuback { packet_id, results }
            if results.as_slice().is_some_and(|codes| {
                !codes.is_empty() && codes.iter().all(|code| matches!(code, 0 | 0x11 | 0x8f))
            }) =>
        {
            let [high, low] = packet_id.get().to_be_bytes();
            Ok(vec![0xb0, 2, high, low])
        }
        _ => Err(unsupported("AckNotEncodableInMqtt311")),
    }
}

/// One SUBACK or UNSUBACK with a code per filter, of any length. An
/// UNSUBACK of MQTT 3.1.1 has no payload: its codes are not written.
pub fn encode_filter_ack(
    version: Version,
    packet_id: InboundPacketId,
    unsubscribe: bool,
    codes: &[u8],
) -> Result<Vec<u8>, Error> {
    let properties = usize::from(version == Version::V5);
    let codes = if unsubscribe && version == Version::V311 {
        &[][..]
    } else {
        codes
    };
    let remaining = 2usize
        .checked_add(properties)
        .and_then(|n| n.checked_add(codes.len()))
        .filter(|n| *n <= 268_435_455)
        .ok_or_else(|| oversized("AckTooLarge"))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(remaining + 5)
        .map_err(|_| oversized("AckTooLarge"))?;
    bytes.push(if unsubscribe { 0xb0 } else { 0x90 });
    let mut length = remaining;
    loop {
        let mut byte = (length % 128) as u8;
        length /= 128;
        if length > 0 {
            byte |= 0x80;
        }
        bytes.push(byte);
        if length == 0 {
            break;
        }
    }
    bytes.extend_from_slice(&packet_id.get().to_be_bytes());
    if properties == 1 {
        bytes.push(0);
    }
    bytes.extend_from_slice(codes);
    Ok(bytes)
}

/// The codes of a SUBACK or UNSUBACK that [`encode_for`] built for one
/// request of the core (at most [`MAX_FILTERS`] codes, so one length
/// byte): what follows the Packet Identifier and, in MQTT 5, the empty
/// property list. None for an UNSUBACK of MQTT 3.1.1.
#[must_use]
pub fn filter_ack_codes(version: Version, bytes: &[u8]) -> &[u8] {
    let head = 4 + usize::from(version == Version::V5);
    bytes.get(head..).unwrap_or(&[])
}

pub fn refusal_for(version: Version, error: Error, before_connack: bool) -> Option<Vec<u8>> {
    match version {
        Version::V5 => Some(refusal(error, before_connack)),
        Version::V311 if before_connack => Some(legacy_connack_refusal(error.reason)),
        Version::V311 => None,
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
        // Reasons such as Unsupported Protocol Version belong to CONNACK,
        // and cannot be reused in a server DISCONNECT.
        let reason = if matches!(reason,0x80..=0x83|0x87|0x89|0x8b|0x8d..=0x90|0x93..=0xa2) {
            reason
        } else {
            0x83
        };
        vec![0xe0, 2, reason, 0]
    }
}

/// Encode the core-authorized outgoing publication, preserving frozen RETAIN.
#[cfg(any(test, feature = "test-hooks"))]
pub fn encode_qos0(topic: &SharedBytes, payload: &SharedBytes) -> Result<Vec<u8>, Error> {
    encode_qos0_retained(topic, payload, false)
}

pub fn encode_qos0_retained(
    topic: &SharedBytes,
    payload: &SharedBytes,
    retain: bool,
) -> Result<Vec<u8>, Error> {
    encode_qos0_expiring(topic, payload, retain, None).map(|encoded| encoded.bytes)
}

/// Encoded MQTT publication and the exact fixed-width value the writer may
/// update before its first positive write. Updating it never changes the size.
#[derive(Debug, PartialEq, Eq)]
pub struct EncodedPublish {
    pub bytes: Vec<u8>,
    pub expiry_value_offset: Option<usize>,
}

pub fn encode_qos0_expiring(
    topic: &SharedBytes,
    payload: &SharedBytes,
    retain: bool,
    remaining: Option<u32>,
) -> Result<EncodedPublish, Error> {
    encode_qos0_for(Version::V5, topic, payload, retain, remaining)
}

pub fn encode_qos0_for(
    version: Version,
    topic: &SharedBytes,
    payload: &SharedBytes,
    retain: bool,
    remaining: Option<u32>,
) -> Result<EncodedPublish, Error> {
    encode_publication(
        version, None, topic, payload, false, retain, remaining, None,
    )
}

pub fn encode_qos0_with_properties(
    version: Version,
    topic: &SharedBytes,
    payload: &SharedBytes,
    retain: bool,
    remaining: Option<u32>,
    properties: Option<&PublishProperties>,
) -> Result<EncodedPublish, Error> {
    encode_publication(
        version, None, topic, payload, false, retain, remaining, properties,
    )
}

#[cfg(any(test, feature = "test-hooks"))]
pub fn encode_publish(
    packet_id: OutboundPacketId,
    topic_name: &SharedBytes,
    payload: &SharedBytes,
    dup: bool,
) -> Result<Vec<u8>, Error> {
    encode_publish_retained(packet_id, topic_name, payload, dup, false)
}
pub fn encode_publish_retained(
    packet_id: OutboundPacketId,
    topic_name: &SharedBytes,
    payload: &SharedBytes,
    dup: bool,
    retain: bool,
) -> Result<Vec<u8>, Error> {
    encode_publish_expiring(packet_id, topic_name, payload, dup, retain, None)
        .map(|encoded| encoded.bytes)
}

pub fn encode_publish_expiring(
    packet_id: OutboundPacketId,
    topic_name: &SharedBytes,
    payload: &SharedBytes,
    dup: bool,
    retain: bool,
    remaining: Option<u32>,
) -> Result<EncodedPublish, Error> {
    encode_publish_for(
        Version::V5,
        packet_id,
        topic_name,
        payload,
        dup,
        retain,
        remaining,
    )
}

pub fn encode_publish_for(
    version: Version,
    packet_id: OutboundPacketId,
    topic_name: &SharedBytes,
    payload: &SharedBytes,
    dup: bool,
    retain: bool,
    remaining: Option<u32>,
) -> Result<EncodedPublish, Error> {
    encode_delivery_for(
        version,
        (packet_id, Qos::AtLeastOnce),
        topic_name,
        payload,
        dup,
        retain,
        remaining,
    )
}

pub(crate) fn encode_delivery_for(
    version: Version,
    packet: (OutboundPacketId, Qos),
    topic_name: &SharedBytes,
    payload: &SharedBytes,
    dup: bool,
    retain: bool,
    remaining: Option<u32>,
) -> Result<EncodedPublish, Error> {
    encode_publication(
        version,
        Some(packet),
        topic_name,
        payload,
        dup,
        retain,
        remaining,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn encode_delivery_with_properties(
    version: Version,
    packet: (OutboundPacketId, Qos),
    topic_name: &SharedBytes,
    payload: &SharedBytes,
    dup: bool,
    retain: bool,
    remaining: Option<u32>,
    properties: Option<&PublishProperties>,
) -> Result<EncodedPublish, Error> {
    encode_publication(
        version,
        Some(packet),
        topic_name,
        payload,
        dup,
        retain,
        remaining,
        properties,
    )
}

fn push_varint(bytes: &mut Vec<u8>, mut value: usize) {
    for _ in 0..4 {
        let mut part = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            part |= 0x80;
        }
        bytes.push(part);
        if value == 0 {
            break;
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn encode_publication(
    version: Version,
    packet: Option<(OutboundPacketId, Qos)>,
    topic_name: &SharedBytes,
    payload: &SharedBytes,
    dup: bool,
    retain: bool,
    remaining: Option<u32>,
    properties: Option<&PublishProperties>,
) -> Result<EncodedPublish, Error> {
    topic(topic_name.as_slice())?;
    if topic_name.len() > crate::writer::MAX_TOPIC || payload.len() > crate::writer::MAX_PAYLOAD {
        return Err(oversized("OutgoingFieldTooLarge"));
    }
    let remaining = if version == Version::V5 {
        remaining
    } else {
        None
    };
    let props = if version == Version::V5 {
        properties.filter(|p| !p.is_empty())
    } else {
        None
    };
    if let Some(p) = props {
        p.check()
            .map_err(|_| protocol("InvalidOutgoingProperties"))?;
    }
    let prop_bytes = (if remaining.is_some() { 5usize } else { 0usize })
        .checked_add(props.map_or(0, PublishProperties::wire_len))
        .ok_or_else(|| oversized("PacketTooLarge"))?;
    let prop_section = if version == Version::V5 {
        varint_len(prop_bytes)
            .checked_add(prop_bytes)
            .ok_or_else(|| oversized("PacketTooLarge"))?
    } else {
        0
    };
    let body = topic_name
        .len()
        .checked_add(payload.len())
        .and_then(|n| n.checked_add(2 + if packet.is_some() { 2 } else { 0 }))
        .and_then(|n| n.checked_add(prop_section))
        .ok_or_else(|| oversized("PacketTooLarge"))?;
    let length_bytes = varint_len(body);
    let total = body
        .checked_add(1 + length_bytes)
        .filter(|n| *n <= crate::net::MAX_PACKET_BYTES)
        .ok_or_else(|| oversized("PacketTooLarge"))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(total)
        .map_err(|_| protocol("OutgoingAllocationFailed"))?;
    bytes.push(
        (if let Some((_, qos)) = packet {
            0x30 | (qos.to_wire() << 1) | if dup { 8 } else { 0 }
        } else {
            0x30
        }) | u8::from(retain),
    );
    push_varint(&mut bytes, body);
    bytes.extend_from_slice(&(topic_name.len() as u16).to_be_bytes());
    bytes.extend_from_slice(topic_name.as_slice());
    if let Some((packet_id, _)) = packet {
        bytes.extend_from_slice(&packet_id.get().to_be_bytes());
    }
    let expiry_value_offset = if version == Version::V5 {
        push_varint(&mut bytes, prop_bytes);
        let offset = if let Some(remaining) = remaining {
            bytes.push(0x02);
            let at = bytes.len();
            bytes.extend_from_slice(&remaining.to_be_bytes());
            Some(at)
        } else {
            None
        };
        if let Some(p) = props {
            if let Some(pfi) = p.payload_format_indicator {
                bytes.push(0x01);
                bytes.push(pfi);
            }
            if let Some(ct) = &p.content_type {
                bytes.push(0x03);
                bytes.extend_from_slice(&(ct.len() as u16).to_be_bytes());
                bytes.extend_from_slice(ct.as_slice());
            }
            if let Some(rt) = &p.response_topic {
                bytes.push(0x08);
                bytes.extend_from_slice(&(rt.len() as u16).to_be_bytes());
                bytes.extend_from_slice(rt.as_slice());
            }
            if let Some(cd) = &p.correlation_data {
                bytes.push(0x09);
                bytes.extend_from_slice(&(cd.len() as u16).to_be_bytes());
                bytes.extend_from_slice(cd.as_slice());
            }
            for (k, v) in p.user_properties.iter() {
                bytes.push(0x26);
                bytes.extend_from_slice(&(k.len() as u16).to_be_bytes());
                bytes.extend_from_slice(k.as_slice());
                bytes.extend_from_slice(&(v.len() as u16).to_be_bytes());
                bytes.extend_from_slice(v.as_slice());
            }
        }
        offset
    } else {
        None
    };
    bytes.extend_from_slice(payload.as_slice());
    debug_assert_eq!(bytes.len(), total);
    Ok(EncodedPublish {
        bytes,
        expiry_value_offset,
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn message_expiry_accepts_absent_zero_finite_and_the_maximum_interval() {
        for interval in [0u32, 1, u32::MAX] {
            let mut body = b"\0\x01t\x05\x02".to_vec();
            body.extend_from_slice(&interval.to_be_bytes());
            body.extend_from_slice(b"payload");
            let decoded = decode(0x30, &body, BOUNDS);
            assert!(decoded.is_ok(), "interval {interval}: {decoded:?}");
        }
        assert!(decode(0x30, b"\0\x01t\0payload", BOUNDS).is_ok());
    }

    #[test]
    fn message_expiry_preserves_wire_values_without_fabricating_a_clock_observation() {
        for qos in [Qos::AtMostOnce, Qos::AtLeastOnce, Qos::ExactlyOnce] {
            for interval in [0u32, 1, u32::MAX] {
                let mut body = b"\0\x01t".to_vec();
                if qos != Qos::AtMostOnce {
                    body.extend_from_slice(&7u16.to_be_bytes());
                }
                body.extend_from_slice(&[5, 0x02]);
                body.extend_from_slice(&interval.to_be_bytes());
                body.extend_from_slice(b"payload");
                let Packet::TimedPublish {
                    packet:
                        InboundPacket::Publish {
                            qos: decoded_qos,
                            topic,
                            payload,
                            retain,
                            expiry,
                            ..
                        },
                    interval: decoded_interval,
                } = decode(0x31 | (qos.to_wire() << 1), &body, BOUNDS).unwrap()
                else {
                    panic!("timed PUBLISH");
                };
                assert_eq!(decoded_interval, interval);
                assert_eq!(decoded_qos, qos);
                assert_eq!(expiry, None);
                assert_eq!(topic.as_slice(), b"t");
                assert_eq!(payload.as_slice(), b"payload");
                assert!(retain);
            }
        }
        assert!(matches!(
            decode(0x30, b"\0\x01t\0p", BOUNDS),
            Ok(Packet::Core(InboundPacket::Publish { expiry: None, .. }))
        ));
    }

    #[test]
    fn message_expiry_keeps_duplicate_truncation_and_other_property_refusals() {
        let duplicate = b"\0\x01t\x0a\x02\0\0\0\x01\x02\0\0\0\x02p";
        assert_eq!(
            decode(0x30, duplicate, BOUNDS).unwrap_err().code(),
            "DuplicateProperty"
        );
        for value_bytes in 0..4 {
            let mut truncated = b"\0\x01t\x05\x02".to_vec();
            truncated.resize(truncated.len() + value_bytes, 0);
            kind(decode(0x30, &truncated, BOUNDS), ErrorKind::Malformed);
        }
        assert_eq!(
            decode(0x10, &connect(&[0x02, 0, 0, 0, 1], b"c"), BOUNDS)
                .unwrap_err()
                .code(),
            "PropertyNotAllowed"
        );
        let Packet::TimedPublish {
            packet:
                InboundPacket::Publish {
                    properties: Some(props),
                    ..
                },
            interval,
        } = decode(0x30, b"\0\x01t\x09\x02\0\0\0\x01\x03\0\x01xp", BOUNDS).unwrap()
        else {
            panic!("Message Expiry + Content Type must be accepted");
        };
        assert_eq!(interval, 1);
        assert_eq!(
            props.content_type.as_ref().map(|b| b.as_slice()),
            Some(&b"x"[..])
        );
        assert_eq!(
            decode(0x30, b"\0\x01t\x08\x02\0\0\0\x01\x23\0\x01p", BOUNDS)
                .unwrap_err()
                .reason_code(),
            0x94
        );
    }

    #[test]
    fn publication_expiry_encoding_keeps_zero_present_and_exposes_only_its_value() {
        let topic = SharedBytes::copy_from(b"t", 1).unwrap();
        let payload = SharedBytes::copy_from(b"p", 1).unwrap();
        for qos1 in [false, true] {
            for remaining in [None, Some(0), Some(1), Some(u32::MAX)] {
                let encoded = if qos1 {
                    encode_publish_expiring(
                        OutboundPacketId::new(7).unwrap(),
                        &topic,
                        &payload,
                        true,
                        true,
                        remaining,
                    )
                } else {
                    encode_qos0_expiring(&topic, &payload, true, remaining)
                }
                .unwrap();
                assert_eq!(encoded.bytes[0], if qos1 { 0x3b } else { 0x31 });
                let header = header(&encoded.bytes, encoded.bytes.len())
                    .unwrap()
                    .unwrap();
                kind(
                    header_decode(&encoded.bytes, encoded.bytes.len() - 1),
                    ErrorKind::Oversized,
                );
                let decoded =
                    decode(header.first, &encoded.bytes[header.encoded_len..], BOUNDS).unwrap();
                match (remaining, encoded.expiry_value_offset, decoded) {
                    (Some(value), Some(offset), Packet::TimedPublish { interval, .. }) => {
                        assert_eq!(interval, value, "outgoing_message_expiry_remaining_wrong");
                        assert_eq!(&encoded.bytes[offset - 2..offset], &[5, 0x02]);
                        assert_eq!(&encoded.bytes[offset..offset + 4], &value.to_be_bytes());
                        assert_eq!(&encoded.bytes[offset + 4..], b"p");
                    }
                    (None, None, Packet::Core(InboundPacket::Publish { expiry: None, .. })) => {}
                    other => panic!("lost expiry presence: {other:?}"),
                }
            }
        }
    }

    fn header_decode(bytes: &[u8], maximum: usize) -> Result<Packet, Error> {
        let header = header(bytes, maximum)?.unwrap();
        decode(header.first, &bytes[header.encoded_len..], BOUNDS)
    }

    #[test]
    fn expiry_overhead_counts_remaining_length_boundary_growth() {
        let topic = SharedBytes::copy_from(b"t", 1).unwrap();
        for base_body in [122usize, 123, 127, 128, 16_378, 16_379, 16_383, 16_384] {
            let payload = SharedBytes::copy_from(&vec![b'x'; base_body - 4], 32768).unwrap();
            let plain = encode_qos0_expiring(&topic, &payload, false, None).unwrap();
            let expiring = encode_qos0_expiring(&topic, &payload, false, Some(0)).unwrap();
            let plain_header = header(&plain.bytes, 65536).unwrap().unwrap();
            let timed_header = header(&expiring.bytes, 65536).unwrap().unwrap();
            assert_eq!(plain_header.remaining, base_body);
            assert_eq!(timed_header.remaining, base_body + 5);
            assert_eq!(
                expiring.bytes.len() - plain.bytes.len(),
                5 + timed_header.encoded_len - plain_header.encoded_len
            );
            assert_eq!(
                header(&expiring.bytes, expiring.bytes.len() - 1)
                    .unwrap_err()
                    .kind(),
                ErrorKind::Oversized
            );
            assert!(header(&expiring.bytes, expiring.bytes.len()).is_ok());
        }
    }

    #[test]
    fn qos0_has_no_mqtt_identifier_ack_or_duplicate_flag() {
        use mqttiumd_contract::packet::{InboundPacket, Qos, SharedBytes};
        assert!(matches!(
            super::decode(0x30, b"\0\x01t\0p", BOUNDS),
            Ok(super::Packet::Core(InboundPacket::Publish {
                qos: Qos::AtMostOnce,
                packet_id: None,
                dup: false,
                ..
            }))
        ));
        assert_eq!(
            super::encode_qos0(
                &SharedBytes::copy_from(b"t", 1).unwrap(),
                &SharedBytes::copy_from(b"p", 1).unwrap()
            )
            .unwrap(),
            [0x30, 5, 0, 1, b't', 0, b'p']
        );
        assert!(super::decode(0x38, b"\0\x01t\0p", BOUNDS).is_err());
    }
    #[test]
    fn pubsub_reasons_frames_and_filter_counts_have_packet_specific_bounds() {
        use mqttiumd_contract::{ids::OutboundPacketId, pubsub::FilterResults};
        assert_eq!(
            super::encode(OutboundFrame::Suback {
                packet_id: InboundPacketId::new(7).unwrap(),
                results: FilterResults::new(&[1, 0x83, 0xa2]).unwrap()
            })
            .unwrap(),
            [0x90, 6, 0, 7, 0, 1, 0x83, 0xa2]
        );
        assert!(
            super::encode(OutboundFrame::Unsuback {
                packet_id: InboundPacketId::new(7).unwrap(),
                results: FilterResults::new(&[1]).unwrap()
            })
            .is_err()
        );
        for reason in [0, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99] {
            assert!(super::decode(0x40, &[0, 1, reason, 0], BOUNDS).is_ok());
        }
        assert!(super::decode(0x40, &[0, 1, 0x92], BOUNDS).is_err());
        let mut too_many = vec![0, 1, 0];
        for _ in 0..9 {
            too_many.extend_from_slice(&[0, 1, b't', 1]);
        }
        // A valid request the core does not take as one is served as unit
        // requests, in order, and answered once.
        let id = InboundPacketId::new(1).unwrap();
        let split = |packet: Packet| -> (bool, Vec<usize>, Vec<Option<u8>>) {
            let Packet::Requests {
                packet_id,
                unsubscribe,
                parts,
                answers,
            } = packet
            else {
                panic!("unit requests expected, got {packet:?}");
            };
            assert_eq!(packet_id, id);
            let sizes = parts
                .iter()
                .map(|part| match part {
                    InboundPacket::Subscribe {
                        packet_id, filters, ..
                    } if !unsubscribe => {
                        assert_eq!(*packet_id, id);
                        filters.as_slice().len()
                    }
                    InboundPacket::Unsubscribe { packet_id, filters } if unsubscribe => {
                        assert_eq!(*packet_id, id);
                        filters.as_slice().len()
                    }
                    other => panic!("a unit request of another kind: {other:?}"),
                })
                .collect();
            (unsubscribe, sizes, answers)
        };
        assert_eq!(
            split(super::decode(0x82, &too_many, BOUNDS).unwrap()),
            (false, vec![8, 1], vec![None; 9])
        );
        let mut unsubscribed = vec![0, 1, 0];
        for _ in 0..9 {
            unsubscribed.extend_from_slice(&[0, 1, b't']);
        }
        assert_eq!(
            split(super::decode(0xa2, &unsubscribed, BOUNDS).unwrap()),
            (true, vec![8, 1], vec![None; 9])
        );
        let legacy = [&[0u8, 1][..], &[0, 1, b't'].repeat(9)].concat();
        assert_eq!(
            split(super::decode_for(Version::V311, 0xa2, &legacy, BOUNDS).unwrap()),
            (true, vec![8, 1], vec![None; 9])
        );
        // The one acknowledgement, of any length, in both versions.
        let codes = [0, 1, 2, 0x80, 0, 0, 0, 0, 0x97];
        assert_eq!(
            super::encode_filter_ack(Version::V5, id, false, &codes).unwrap(),
            [&[0x90, 12, 0, 1, 0][..], &codes].concat()
        );
        assert_eq!(
            super::encode_filter_ack(Version::V311, id, false, &codes).unwrap(),
            [&[0x90, 11, 0, 1][..], &codes].concat()
        );
        assert_eq!(
            super::encode_filter_ack(Version::V5, id, true, &[0x11; 9]).unwrap(),
            [&[0xb0, 12, 0, 1, 0][..], &[0x11; 9]].concat()
        );
        assert_eq!(
            super::encode_filter_ack(Version::V311, id, true, &[0x11; 9]).unwrap(),
            [0xb0, 2, 0, 1]
        );
        let long = super::encode_filter_ack(Version::V5, id, false, &[0; 200]).unwrap();
        assert_eq!(&long[..6], &[0x90, 0xcb, 1, 0, 1, 0]);
        // The codes of the answer to one unit request.
        for version in [Version::V5, Version::V311] {
            let part = super::encode_filter_ack(version, id, false, &[1, 0x80]).unwrap();
            assert_eq!(super::filter_ack_codes(version, &part), [1, 0x80]);
        }
        assert!(super::filter_ack_codes(Version::V311, &[0xb0, 2, 0, 1]).is_empty());
        // A filter longer than the profile stores is answered for itself;
        // its neighbours are served.
        let long = [
            &[0u8, 1, 0][..],
            &2_000u16.to_be_bytes(),
            &[b'f'; 2_000],
            &[1],
        ]
        .concat();
        assert_eq!(
            split(super::decode(0x82, &long, BOUNDS).unwrap()),
            (false, vec![], vec![Some(0x97)])
        );
        let both = [&long[..], &[0, 1, b't', 1]].concat();
        assert_eq!(
            split(super::decode(0x82, &both, BOUNDS).unwrap()),
            (false, vec![1], vec![Some(0x97), None])
        );
        let gone = [&[0u8, 1, 0][..], &2_000u16.to_be_bytes(), &[b'f'; 2_000]].concat();
        assert_eq!(
            split(super::decode(0xa2, &gone, BOUNDS).unwrap()),
            (true, vec![], vec![Some(0x11)])
        );
        // The size of the packet is the only bound: sixty-five filters are
        // nine unit requests.
        let many = [&[0u8, 1, 0][..], &[0, 1, b't', 1].repeat(65)].concat();
        assert_eq!(
            split(super::decode(0x82, &many, BOUNDS).unwrap()),
            (false, vec![8, 8, 8, 8, 8, 8, 8, 8, 1], vec![None; 65])
        );
        let legacy = [&[0u8, 1][..], &[0, 1, b't'].repeat(65)].concat();
        assert_eq!(
            split(super::decode_for(Version::V311, 0xa2, &legacy, BOUNDS).unwrap())
                .1
                .len(),
            9
        );
        let bytes = super::encode_publish(
            OutboundPacketId::new(4).unwrap(),
            &SharedBytes::copy_from(b"t", 1).unwrap(),
            &SharedBytes::copy_from(b"p", 1).unwrap(),
            true,
        )
        .unwrap();
        assert_eq!(bytes, [0x3a, 7, 0, 1, b't', 0, 4, 0, b'p']);
    }
    #[test]
    fn exact_subscriptions_and_recipient_puback_enter_the_pure_core() {
        use mqttiumd_contract::packet::InboundPacket;
        assert!(matches!(
            super::decode(0x82, &[0, 7, 0, 0, 1, b't', 1], BOUNDS),
            Ok(super::Packet::Core(InboundPacket::Subscribe { .. }))
        ));
        assert!(matches!(
            super::decode(0xa2, &[0, 8, 0, 0, 1, b't'], BOUNDS),
            Ok(super::Packet::Core(InboundPacket::Unsubscribe { .. }))
        ));
        assert!(matches!(
            super::decode(0x40, &[0, 1, 0x97], BOUNDS),
            Ok(super::Packet::Core(InboundPacket::Puback {
                reason: 0x97,
                ..
            }))
        ));
    }
    use super::*;
    use mqttiumd_contract::ids::InboundPacketId;
    use mqttiumd_contract::packet::{AckReason, PubackReason, Qos};

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

    /// The same CONNECT with the User Name flag and a user name: valid,
    /// and outside the profile.
    fn with_user_name(mut body: Vec<u8>) -> Vec<u8> {
        body[7] |= 0x80;
        body.extend_from_slice(&[0, 1, b'u']);
        body
    }

    fn will_connect(
        properties: &[u8],
        will_properties: &[u8],
        flags: u8,
        topic: &[u8],
        payload: &[u8],
    ) -> Vec<u8> {
        let mut body = connect(properties, b"will-client");
        body[7] = flags;
        assert!(will_properties.len() < 128);
        body.push(will_properties.len() as u8);
        body.extend_from_slice(will_properties);
        for value in [topic, payload] {
            body.extend_from_slice(&(value.len() as u16).to_be_bytes());
            body.extend_from_slice(value);
        }
        body
    }

    #[test]
    fn qos1_nonretained_zero_delay_will_and_explicit_will_disconnect_are_accepted() {
        for delay in [&[][..], &[0x18, 0, 0, 0, 0][..]] {
            let body = will_connect(EXPIRY, delay, 0x0c, b"atelier/capteur/status", b"offline");
            assert!(
                matches!(decode(0x10, &body, BOUNDS), Ok(Packet::Connect(_))),
                "supported Will CONNECT was refused"
            );
        }
        assert!(
            matches!(decode(0xe0, &[4], BOUNDS), Ok(Packet::Disconnect { .. })),
            "DISCONNECT4 must request the Will"
        );
    }

    #[test]
    fn supported_will_preserves_owned_bytes_and_redacts_debug() {
        let payload = b"sensitive-offline\0\xff";
        let Packet::Connect(value) = decode(
            0x10,
            &will_connect(
                EXPIRY,
                &[0x18, 0, 0, 0, 0],
                0x2e,
                b"atelier/sensor/status",
                payload,
            ),
            BOUNDS,
        )
        .unwrap() else {
            panic!("CONNECT")
        };
        let will = value.will.unwrap();
        assert!(will.retain);
        assert_eq!(will.topic.as_slice(), b"atelier/sensor/status");
        assert_eq!(will.payload.as_slice(), payload);
        let diagnostic = format!("{will:?}");
        assert!(!diagnostic.contains("sensitive"));
        assert!(!diagnostic.contains("atelier"));
        assert!(matches!(
            decode(0x10, &connect(EXPIRY, b"plain"), BOUNDS),
            Ok(Packet::Connect(Connect { will: None, .. }))
        ));
    }

    #[test]
    fn will_profile_refusals_do_not_accept_ignored_properties() {
        for flags in [0x04, 0x14, 0x24, 0x34] {
            kind(
                decode(
                    0x10,
                    &will_connect(EXPIRY, &[], flags, b"a/b", b"offline"),
                    BOUNDS,
                ),
                ErrorKind::Unsupported,
            );
        }
        for properties in [
            &[0x18, 0, 0, 0, 1][..],             // nonzero delay
            &[0x02, 0, 0, 0, 1][..], // Message Expiry is explicitly out of this Will profile
            &[0x01, 0][..],          // Payload Format Indicator
            &[0x03, 0, 1, b'x'][..], // Content Type
            &[0x08, 0, 1, b'x'][..], // Response Topic
            &[0x09, 0, 1, b'x'][..], // Correlation Data
            &[0x26, 0, 1, b'k', 0, 1, b'v'][..], // User Property
        ] {
            let error = decode(
                0x10,
                &will_connect(EXPIRY, properties, 0x0c, b"a/b", b"offline"),
                BOUNDS,
            )
            .unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Unsupported);
            assert_eq!(error.peer_maximum(), Some(u32::MAX));
        }
    }

    #[test]
    fn will_malformed_and_duplicate_properties_keep_their_protocol_causes() {
        for properties in [
            &[0x18, 0, 0, 0, 0, 0x18, 0, 0, 0, 0][..],
            &[0x02, 0, 0, 0, 1, 0x02, 0, 0, 0, 2][..],
        ] {
            assert_eq!(
                decode(
                    0x10,
                    &will_connect(EXPIRY, properties, 0x0c, b"a/b", b"offline"),
                    BOUNDS
                )
                .unwrap_err()
                .code(),
                "DuplicateProperty"
            );
        }
        kind(
            decode(
                0x10,
                &will_connect(EXPIRY, &[0x18, 0], 0x0c, b"a/b", b"offline"),
                BOUNDS,
            ),
            ErrorKind::Malformed,
        );
        kind(
            decode(
                0x10,
                &will_connect(EXPIRY, &[0x11, 0, 0, 0, 0], 0x0c, b"a/b", b"offline"),
                BOUNDS,
            ),
            ErrorKind::Protocol,
        );
        for name in [b"".as_slice(), b"a/+", b"a/#"] {
            assert_eq!(
                decode(
                    0x10,
                    &will_connect(EXPIRY, &[], 0x0c, name, b"offline"),
                    BOUNDS
                )
                .unwrap_err()
                .code(),
                "InvalidTopicName"
            );
        }
        for flags in [0x10, 0x20, 0x1c] {
            assert_eq!(
                decode(
                    0x10,
                    &will_connect(EXPIRY, &[], flags, b"a/b", b"offline"),
                    BOUNDS
                )
                .unwrap_err()
                .code(),
                "InvalidWillFlags"
            );
        }
    }

    #[test]
    fn will_boundaries_are_checked_before_owned_field_copy() {
        let topic = vec![b't'; BOUNDS.max_topic];
        let payload = vec![b'p'; BOUNDS.max_payload];
        assert!(
            decode(
                0x10,
                &will_connect(EXPIRY, &[], 0x0c, &topic, &payload),
                BOUNDS
            )
            .is_ok()
        );
        for (name, body) in [
            (
                "WillTopicTooLarge",
                will_connect(EXPIRY, &[], 0x0c, &vec![b't'; BOUNDS.max_topic + 1], b"p"),
            ),
            (
                "WillPayloadTooLarge",
                will_connect(EXPIRY, &[], 0x0c, b"t", &vec![b'p'; BOUNDS.max_payload + 1]),
            ),
        ] {
            let error = decode(0x10, &body, BOUNDS).unwrap_err();
            assert_eq!(error.code(), name);
            assert_eq!(error.kind(), ErrorKind::Oversized);
        }
    }

    #[test]
    fn disconnect_will_disposition_and_session_policy_are_independent() {
        assert_eq!(
            decode(0xe0, &[4], BOUNDS).unwrap(),
            Packet::Disconnect {
                expiry: None,
                publish_will: true
            }
        );
        assert_eq!(
            decode(0xe0, &[4, 5, 0x11, 0, 0, 0, 0], BOUNDS).unwrap(),
            Packet::Disconnect {
                expiry: Some(SessionExpiry::OnDisconnect),
                publish_will: true
            }
        );
        assert_eq!(
            decode(0xe0, &[0, 5, 0x11, 0, 0, 0, 5], BOUNDS).unwrap(),
            Packet::Disconnect {
                expiry: Some(SessionExpiry::After(5)),
                publish_will: false
            }
        );
    }

    #[test]
    fn session_connect_matrix_keeps_absent_zero_infinite_and_clean_start_explicit() {
        for clean_start in [false, true] {
            for (properties, expected) in [
                (&[][..], SessionExpiry::OnDisconnect),
                (&[0x11, 0, 0, 0, 0][..], SessionExpiry::OnDisconnect),
                (EXPIRY, SessionExpiry::Never),
                (&[0x11, 0, 0, 0, 1][..], SessionExpiry::After(1)),
                (
                    &[0x11, 255, 255, 255, 254][..],
                    SessionExpiry::After(u32::MAX - 1),
                ),
            ] {
                let mut body = connect(properties, b"lifecycle-client");
                body[7] = if clean_start { 2 } else { 0 };
                let Packet::Connect(value) = decode(0x10, &body, BOUNDS).unwrap() else {
                    panic!("CONNECT");
                };
                assert_eq!(value.clean_start, clean_start);
                assert_eq!(value.expiry, expected);
            }
        }
    }

    #[test]
    fn disconnect_retains_override_and_checks_its_domain_before_runtime_policy() {
        assert_eq!(
            decode(0xe0, &[], BOUNDS).unwrap(),
            Packet::Disconnect {
                expiry: None,
                publish_will: false
            }
        );
        assert_eq!(
            decode(0xe0, &[0, 5, 0x11, 0, 0, 0, 0], BOUNDS).unwrap(),
            Packet::Disconnect {
                expiry: Some(SessionExpiry::OnDisconnect),
                publish_will: false
            }
        );
        assert_eq!(
            decode(0xe0, &[0, 5, 0x11, 255, 255, 255, 255], BOUNDS).unwrap(),
            Packet::Disconnect {
                expiry: Some(SessionExpiry::Never),
                publish_will: false
            }
        );
        assert_eq!(
            decode(0xe0, &[0, 5, 0x11, 0, 0, 0, 7], BOUNDS).unwrap(),
            Packet::Disconnect {
                expiry: Some(SessionExpiry::After(7)),
                publish_will: false
            }
        );
        assert_eq!(
            decode(0xe0, &[0, 10, 0x11, 0, 0, 0, 0, 0x11, 0, 0, 0, 0], BOUNDS)
                .unwrap_err()
                .code(),
            "DuplicateProperty"
        );
        assert_eq!(
            decode(0xe0, &[0, 5, 0x11, 0], BOUNDS).unwrap_err().kind(),
            ErrorKind::Malformed
        );
    }

    #[test]
    fn session_refusal_never_invents_a_session_or_a_success_reason() {
        assert_eq!(session_refusal(0x97).unwrap(), [0x20, 3, 0, 0x97, 0]);
        assert!(session_refusal(0).is_err());
        assert!(session_refusal(0x92).is_err());
        assert_eq!(disconnect_reason(0x82), [0xe0, 2, 0x82, 0]);
    }

    #[test]
    fn zero_connect_context_rejects_any_nonzero_disconnect_before_profile_refusal() {
        let error = decode(0xe0, &[0x80, 5, 0x11, 0, 0, 0, 7], BOUNDS).unwrap_err();
        assert_eq!(
            error.in_session(SessionExpiry::OnDisconnect).code(),
            "SessionExpiryIncreaseForbidden"
        );
        assert_eq!(
            error.in_session(SessionExpiry::OnDisconnect).reason_code(),
            0x82
        );
        assert_eq!(
            error.in_session(SessionExpiry::Never).kind(),
            ErrorKind::Unsupported
        );
        let malformed = decode(0xe0, &[0, 5, 0x11, 0], BOUNDS).unwrap_err();
        assert_eq!(
            malformed.in_session(SessionExpiry::OnDisconnect).kind(),
            ErrorKind::Malformed
        );
    }

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
    fn an_unsupported_connect_retains_its_validated_response_limit() {
        let mut props = vec![0x11, 0, 0, 0, 7];
        props.extend_from_slice(&[0x27, 0, 0, 0, 3]);
        // A user name remains outside this profile.
        let mut clean = with_user_name(connect(&props, b"small-refusal"));
        clean[7] |= 2;
        let error = decode(0x10, &clean, BOUNDS).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unsupported);
        assert_eq!(error.peer_maximum(), Some(3));
        assert!(refusal(error, true).len() > error.peer_maximum().unwrap() as usize);

        let finite = decode(
            0x10,
            &with_user_name(connect(&[0x11, 0, 0, 0, 7], b"unsupported-user-name")),
            BOUNDS,
        )
        .unwrap_err();
        assert_eq!(finite.peer_maximum(), Some(u32::MAX));
        clean.pop();
        assert_eq!(
            decode(0x10, &clean, BOUNDS).unwrap_err().peer_maximum(),
            None
        );
        let mut duplicate = props;
        duplicate.extend_from_slice(&[0x27, 0, 0, 0, 2]);
        assert_eq!(
            decode(0x10, &connect(&duplicate, b"bad"), BOUNDS)
                .unwrap_err()
                .peer_maximum(),
            None
        );
    }

    #[test]
    fn profile_refusals_differ_from_malformed_or_contradictory_properties() {
        kind(
            decode(
                0x10,
                &with_user_name(connect(&[0x11, 0, 0, 0, 7], b"client")),
                BOUNDS,
            ),
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
    fn retained_flags_are_encoded_without_inventing_qos0_identifiers() {
        let topic = SharedBytes::copy_from(b"a/b", 1024).unwrap();
        let payload = SharedBytes::copy_from(b"state", 32768).unwrap();
        let qos0 = encode_qos0_retained(&topic, &payload, true).unwrap();
        assert_eq!(qos0[0], 0x31);
        let Packet::Core(InboundPacket::Publish {
            qos,
            retain,
            packet_id,
            ..
        }) = decode(qos0[0], &qos0[2..], BOUNDS).unwrap()
        else {
            panic!("publish")
        };
        assert_eq!(qos, Qos::AtMostOnce);
        assert!(retain);
        assert_eq!(packet_id, None);
        let qos1 = encode_publish_retained(
            OutboundPacketId::new(7).unwrap(),
            &topic,
            &payload,
            false,
            true,
        )
        .unwrap();
        assert_eq!(qos1[0], 0x33);
        assert_eq!(
            encode_publish_retained(
                OutboundPacketId::new(7).unwrap(),
                &topic,
                &payload,
                true,
                false
            )
            .unwrap()[0],
            0x3a
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
            expiry: None,
            properties: None,
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
    fn qos1_publish_preserves_its_qos_identifier_and_dup_into_the_core() {
        let body = b"\0\x03a/b\0\x07\0payload";
        for (first, duplicate) in [(0x32, false), (0x3a, true)] {
            let decoded = decode(first, body, BOUNDS);
            assert!(
                decoded.is_ok(),
                "QoS 1 is part of the product profile: {decoded:?}"
            );
            let Packet::Core(InboundPacket::Publish {
                topic,
                qos,
                retain,
                dup,
                packet_id,
                payload,
                expiry: None,
                properties: None,
            }) = decoded.unwrap()
            else {
                panic!("PUBLISH")
            };
            assert_eq!(qos, Qos::AtLeastOnce);
            assert_eq!(topic.as_slice(), b"a/b");
            assert_eq!(payload.as_slice(), b"payload");
            assert_eq!(packet_id.unwrap().get(), 7);
            assert_eq!(dup, duplicate);
            assert!(!retain);
        }
    }

    #[test]
    fn puback_encodes_only_reasons_valid_for_its_packet_role() {
        let packet_id = InboundPacketId::new(7).unwrap();
        for (reason, expected) in [
            (PubackReason::Success, vec![0x40, 2, 0, 7]),
            (
                PubackReason::NoMatchingSubscribers,
                vec![0x40, 3, 0, 7, 0x10],
            ),
            (PubackReason::QuotaExceeded, vec![0x40, 3, 0, 7, 0x97]),
            (
                PubackReason::ImplementationSpecificError,
                vec![0x40, 3, 0, 7, 0x83],
            ),
        ] {
            assert_eq!(
                encode(OutboundFrame::Puback { packet_id, reason }).unwrap(),
                expected
            );
        }
        // PacketIdentifierNotFound cannot be represented by PubackReason.
        assert_eq!(
            encode(OutboundFrame::Pubrec {
                packet_id,
                reason: AckReason::PacketIdentifierNotFound,
            })
            .unwrap_err()
            .kind(),
            ErrorKind::Protocol,
        );
        assert_eq!(
            encode(OutboundFrame::Pubcomp {
                packet_id,
                reason: AckReason::QuotaExceeded,
            })
            .unwrap_err()
            .kind(),
            ErrorKind::Protocol,
        );
    }

    #[test]
    fn publish_restrictions_do_not_hide_invalid_fields() {
        assert!(decode(0x30, b"\0\x03a/b\0payload", BOUNDS).is_ok());
        assert!(matches!(
            decode(0x35, b"\0\x03a/b\0\x07\0payload", BOUNDS),
            Ok(Packet::Core(InboundPacket::Publish { retain: true, .. }))
        ));
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

    /// MQTT 5 gives no meaning to a User Property outside a publication,
    /// to a Reason String, or to a request for Response Information the
    /// server is free not to answer: a broker that does not use them must
    /// still take the packet. What the packet says is what it says
    /// without them.
    #[test]
    fn information_the_broker_may_leave_unused_does_not_refuse_its_packet() {
        const USER: &[u8] = &[0x26, 0, 1, b'k', 0, 1, b'v'];
        const REASON: &[u8] = &[0x1f, 0, 3, b'w', b'h', b'y'];
        let with = |properties: &[&[u8]]| -> Vec<u8> {
            let joined = properties.concat();
            let mut out = vec![u8::try_from(joined.len()).unwrap()];
            out.extend_from_slice(&joined);
            out
        };
        // CONNECT: a repeated User Property and Request Response Information 1.
        let plain = decode(0x10, &connect(EXPIRY, b"plain"), BOUNDS).unwrap();
        let informed = decode(
            0x10,
            &connect(&[EXPIRY, USER, USER, &[0x19, 1]].concat(), b"plain"),
            BOUNDS,
        )
        .unwrap();
        assert_eq!(format!("{informed:?}"), format!("{plain:?}"));
        // SUBSCRIBE and UNSUBSCRIBE.
        for (first, tail) in [(0x82, &[0, 1, b't', 1][..]), (0xa2, &[0, 1, b't'][..])] {
            let body = |properties: &[&[u8]]| [&[0, 9][..], &with(properties), tail].concat();
            assert_eq!(
                decode(first, &body(&[USER, USER]), BOUNDS).unwrap(),
                decode(first, &body(&[]), BOUNDS).unwrap()
            );
        }
        // The four acknowledgements a client sends, with their Reason Code.
        for first in [0x40, 0x50, 0x62, 0x70] {
            let body = |properties: &[&[u8]]| [&[0, 9, 0][..], &with(properties)].concat();
            assert_eq!(
                decode(first, &body(&[REASON, USER]), BOUNDS).unwrap(),
                decode(first, &[0, 9], BOUNDS).unwrap(),
                "packet {first:#x}"
            );
        }
        // DISCONNECT.
        assert_eq!(
            decode(0xe0, &[&[0][..], &with(&[REASON, USER])].concat(), BOUNDS).unwrap(),
            decode(0xe0, &[], BOUNDS).unwrap()
        );
        // A Reason String stays single, and a PUBLISH carries its User Property
        // into the core for end-to-end forwarding (ADR 0053).
        assert_eq!(
            decode(
                0x40,
                &[&[0, 9, 0][..], &with(&[REASON, REASON])].concat(),
                BOUNDS
            )
            .unwrap_err()
            .code(),
            "DuplicateProperty"
        );
        let Packet::Core(InboundPacket::Publish {
            properties: Some(props),
            ..
        }) = decode(
            0x30,
            &[&[0, 1, b't'][..], &with(&[USER]), b"x"].concat(),
            BOUNDS,
        )
        .unwrap()
        else {
            panic!("PUBLISH with User Property must be accepted");
        };
        assert_eq!(props.user_properties.len(), 1);
        assert_eq!(props.user_properties[0].0.as_slice(), b"k");
        assert_eq!(props.user_properties[0].1.as_slice(), b"v");
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
        assert_eq!(
            decode(0xe0, &[], BOUNDS).unwrap(),
            Packet::Disconnect {
                expiry: None,
                publish_will: false
            }
        );
        kind(decode(0xc0, &[0], BOUNDS), ErrorKind::Malformed);
        kind(decode(0x60, &[0, 7], BOUNDS), ErrorKind::Malformed);
    }

    /// What the broker announced it does not support is refused with the
    /// reason the specification names, not with a generic one: a shared
    /// subscription (0x9E), a Subscription Identifier (0xA1) and an
    /// Authentication Method (0x8C).
    #[test]
    fn unsupported_features_are_refused_with_their_own_reason() {
        let shared = decode(0x82, b"\0\x01\0\0\x0a$share/g/t\0", BOUNDS).unwrap_err();
        assert_eq!(shared.reason, 0x9e, "{}", shared.code);
        let identified = decode(0x82, b"\0\x01\x02\x0b\x05\0\x01t\0", BOUNDS).unwrap_err();
        assert_eq!(identified.reason, 0xa1, "{}", identified.code);
        let authenticated = decode(
            0x10,
            b"\0\x04MQTT\x05\x02\0\x1e\x06\x15\0\x03abc\0\x03cid",
            BOUNDS,
        )
        .unwrap_err();
        assert_eq!(authenticated.reason, 0x8c, "{}", authenticated.code);
    }

    #[test]
    fn valid_subscription_options_reach_the_core_without_loss() {
        for bits in [0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44] {
            let packet =
                decode(0x82, &[0, 1, 0, 0, 3, b'a', b'/', b'#', bits | 2], BOUNDS).unwrap();
            let Packet::Core(InboundPacket::Subscribe { filters, .. }) = packet else {
                panic!("subscribe");
            };
            assert_eq!(filters.as_slice()[0].options.bits(), bits);
            assert_eq!(filters.as_slice()[0].qos, Qos::ExactlyOnce);
        }
        // Retain Handling 3 and QoS 3 are protocol errors of defined fields.
        for bits in [0x30, 3] {
            kind(
                decode(0x82, &[0, 1, 0, 0, 3, b'a', b'/', b'#', bits], BOUNDS),
                ErrorKind::Protocol,
            );
        }
        // Reserved bits make the packet malformed (MQTT-3.8.3-5).
        for bits in [0x40, 0x80, 0xc0] {
            let error = decode(0x82, &[0, 1, 0, 0, 3, b'a', b'/', b'#', bits], BOUNDS).unwrap_err();
            assert_eq!((error.kind, error.reason), (ErrorKind::Malformed, 0x81));
        }
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
                0x21,
                0,
                8,
                0x27,
                ANNOUNCED[0],
                ANNOUNCED[1],
                ANNOUNCED[2],
                ANNOUNCED[3],
                0x25,
                1,
                0x22,
                0,
                0,
                0x28,
                1,
                0x29,
                0,
                0x2a,
                0
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

    #[test]
    fn server_only_disconnect_reasons_are_protocol_errors_from_a_client() {
        kind(decode(0xe0, &[0x87], BOUNDS), ErrorKind::Protocol);
    }

    #[test]
    fn a_refusal_never_sends_a_reason_from_the_wrong_packet_role() {
        let version = Error {
            kind: ErrorKind::Unsupported,
            code: "UnsupportedProtocolVersion",
            reason: 0x84,
            peer_maximum: None,
            disconnect_expiry: None,
        };
        assert_eq!(refusal(version, true), [0x20, 3, 0, 0x84, 0]);
        assert_eq!(refusal(version, false), [0xe0, 2, 0x83, 0]);
        let impossible_success = Error {
            reason: 0,
            ..version
        };
        assert_eq!(refusal(impossible_success, false), [0xe0, 2, 0x83, 0]);
    }

    #[test]
    fn field_bounds_accept_the_boundary_and_refuse_one_byte_more() {
        let client = vec![b'c'; BOUNDS.max_client_id];
        assert!(decode(0x10, &connect(EXPIRY, &client), BOUNDS).is_ok());
        let body = |topic: &[u8], payload: &[u8]| {
            let mut b = (topic.len() as u16).to_be_bytes().to_vec();
            b.extend_from_slice(topic);
            b.extend_from_slice(&[0, 7, 0]);
            b.extend_from_slice(payload);
            b
        };
        let topic = vec![b't'; BOUNDS.max_topic];
        let payload = vec![b'x'; BOUNDS.max_payload];
        assert!(decode(0x34, &body(&topic, &payload), BOUNDS).is_ok());
        kind(
            decode(
                0x34,
                &body(&vec![b't'; BOUNDS.max_topic + 1], &payload),
                BOUNDS,
            ),
            ErrorKind::Oversized,
        );
        kind(
            decode(
                0x34,
                &body(&topic, &vec![b'x'; BOUNDS.max_payload + 1]),
                BOUNDS,
            ),
            ErrorKind::Oversized,
        );
    }

    #[test]
    fn utf8_bom_is_preserved_and_unsupported_tails_are_validated() {
        let id = b"\xef\xbb\xbfclient";
        let Packet::Connect(c) = decode(0x10, &connect(EXPIRY, id), BOUNDS).unwrap() else {
            panic!("CONNECT")
        };
        assert_eq!(c.client_id.as_slice(), id);
        let mut will = connect(EXPIRY, b"will-client");
        will[7] = 4;
        kind(decode(0x10, &will, BOUNDS), ErrorKind::Malformed);
        will.extend_from_slice(&[0, 0, 3, b'a', b'/', b'b', 0, 0]);
        kind(decode(0x10, &will, BOUNDS), ErrorKind::Unsupported);
    }

    #[test]
    fn a_finite_header_and_body_mutation_sweep_never_panics_or_owns_over_budget_fields() {
        let cases = [
            (0x10, connect(EXPIRY, b"bounded-client")),
            (0x34, b"\0\x03a/b\0\x07\0payload".to_vec()),
            (0x62, vec![0, 7]),
            (0xc0, vec![]),
            (0xe0, vec![]),
            (0x82, b"\0\x01\0\0\x03a/#\x02".to_vec()),
        ];
        let mut attempts = 0usize;
        let mut check = |first, body: &[u8]| {
            if let Ok(Packet::Core(InboundPacket::Publish { topic, payload, .. })) =
                decode(first, body, BOUNDS)
            {
                assert!(topic.len() <= BOUNDS.max_topic && payload.len() <= BOUNDS.max_payload);
            }
            attempts += 1;
        };
        for first in 0..=u8::MAX {
            let _ = header(&[first, 0], BOUNDS.max_packet);
            check(first, &[]);
        }
        for (first, body) in cases {
            for end in 0..=body.len() {
                check(first, &body[..end]);
            }
            for index in 0..body.len() {
                for byte in [0, 0x80, 0xff] {
                    let mut changed = body.clone();
                    changed[index] = byte;
                    check(first, &changed);
                }
            }
        }
        assert!(attempts < 2048);
        println!("bounded codec exploration: 256 headers and {attempts} bodies");
    }
}
