//! Bounded MQTT 5 PUBLISH application properties (ADR 0053).
//!
//! Carries the five end-to-end application properties of MQTT 5.0 PUBLISH
//! packets: Payload Format Indicator (`0x01`), Content Type (`0x03`),
//! Response Topic (`0x08`), Correlation Data (`0x09`), and ordered User
//! Properties (`0x26`). Message Expiry Interval (`0x02`) remains owned by
//! [`crate::message_time::MessageExpiry`] because its remaining wire value
//! ages with the monotonic clock.
//!
//! This module lives in `mqttiumd-contract` (`no_std + alloc`, no unsafe) so
//! the core, every store backend, and the wire codec share one canonical
//! representation and exact size accounting.

use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::canonical::{Reader, RecipeError};
use crate::message::SharedBytes;

/// Maximum number of ordered User Property pairs carried by one PUBLISH.
pub const MAX_USER_PROPERTIES: usize = 64;

/// Maximum aggregate logical byte size of PUBLISH application properties
/// (sum of string/binary value lengths plus Payload Format Indicator byte).
pub const MAX_PROPERTY_BYTES: usize = 16_384;

const FLAG_PAYLOAD_FORMAT: u8 = 0x01;
const FLAG_CONTENT_TYPE: u8 = 0x02;
const FLAG_RESPONSE_TOPIC: u8 = 0x04;
const FLAG_CORRELATION_DATA: u8 = 0x08;
const FLAG_USER_PROPERTIES: u8 = 0x10;
const ALL_FLAGS: u8 = FLAG_PAYLOAD_FORMAT
    | FLAG_CONTENT_TYPE
    | FLAG_RESPONSE_TOPIC
    | FLAG_CORRELATION_DATA
    | FLAG_USER_PROPERTIES;

/// Exact MQTT Variable Byte Integer length (1..=4 bytes).
#[must_use]
pub const fn varint_len(value: usize) -> usize {
    if value < 128 {
        1
    } else if value < 16_384 {
        2
    } else if value < 2_097_152 {
        3
    } else {
        4
    }
}

/// Validates an MQTT 5 UTF-8 Encoded String (section 1.5.4): well-formed
/// UTF-8, length <= 65,535, no U+0000, no disallowed control characters
/// (`U+0001..=U+001F`, `U+007F..=U+009F`), and no Unicode noncharacters.
#[must_use]
pub fn valid_mqtt_utf8(bytes: &[u8]) -> bool {
    if bytes.len() > usize::from(u16::MAX) {
        return false;
    }
    let Ok(text) = core::str::from_utf8(bytes) else {
        return false;
    };
    !text.chars().any(|c| {
        let n = u32::from(c);
        n <= 0x1f
            || (0x7f..=0x9f).contains(&n)
            || (0xfdd0..=0xfdef).contains(&n)
            || n & 0xfffe == 0xfffe
    })
}

/// Validates an MQTT 5 Response Topic (section 3.3.2.3.5): valid non-empty
/// MQTT UTF-8 string containing no wildcard characters (`+`, `#`).
#[must_use]
pub fn valid_response_topic(bytes: &[u8]) -> bool {
    !bytes.is_empty() && valid_mqtt_utf8(bytes) && !bytes.iter().any(|b| matches!(*b, b'+' | b'#'))
}

/// Immutable MQTT 5 PUBLISH application properties forwarded end-to-end.
///
/// All User Properties are forwarded unaltered (MQTT-3.3.2-17) and their order
/// is preserved verbatim (MQTT-3.3.2-18).
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct PublishProperties {
    pub payload_format_indicator: Option<u8>,
    pub content_type: Option<SharedBytes>,
    pub response_topic: Option<SharedBytes>,
    pub correlation_data: Option<SharedBytes>,
    pub user_properties: Arc<[(SharedBytes, SharedBytes)]>,
}

impl PublishProperties {
    /// Whether no application property is present.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.payload_format_indicator.is_none()
            && self.content_type.is_none()
            && self.response_topic.is_none()
            && self.correlation_data.is_none()
            && self.user_properties.is_empty()
    }

    /// Aggregate logical byte size bounded by [`MAX_PROPERTY_BYTES`].
    #[must_use]
    pub fn logical_bytes(&self) -> usize {
        let mut total = usize::from(self.payload_format_indicator.is_some());
        if let Some(value) = &self.content_type {
            total = total.saturating_add(value.len());
        }
        if let Some(value) = &self.response_topic {
            total = total.saturating_add(value.len());
        }
        if let Some(value) = &self.correlation_data {
            total = total.saturating_add(value.len());
        }
        for (name, value) in self.user_properties.iter() {
            total = total.saturating_add(name.len()).saturating_add(value.len());
        }
        total
    }

    /// Exact MQTT 5 wire length of these properties inside the PUBLISH
    /// Properties section (excluding the Property Length varint and excluding
    /// Message Expiry Interval `0x02`).
    #[must_use]
    pub fn wire_len(&self) -> usize {
        let mut total = 0usize;
        if self.payload_format_indicator.is_some() {
            total += 2; // 0x01 + u8
        }
        if let Some(value) = &self.content_type {
            total += 1 + 2 + value.len(); // 0x03 + u16 + bytes
        }
        if let Some(value) = &self.response_topic {
            total += 1 + 2 + value.len(); // 0x08 + u16 + bytes
        }
        if let Some(value) = &self.correlation_data {
            total += 1 + 2 + value.len(); // 0x09 + u16 + bytes
        }
        for (name, value) in self.user_properties.iter() {
            total += 1 + 2 + name.len() + 2 + value.len(); // 0x26 + u16 + k + u16 + v
        }
        total
    }

    /// Validates all field domains and profile bounds.
    pub fn check(&self) -> Result<(), RecipeError> {
        if self.payload_format_indicator.is_some_and(|v| v > 1) {
            return Err(RecipeError::InvalidField("payload_format_indicator"));
        }
        if let Some(value) = &self.content_type
            && !valid_mqtt_utf8(value.as_slice())
        {
            return Err(RecipeError::InvalidField("content_type"));
        }
        if let Some(value) = &self.response_topic
            && !valid_response_topic(value.as_slice())
        {
            return Err(RecipeError::InvalidField("response_topic"));
        }
        if let Some(value) = &self.correlation_data
            && value.len() > usize::from(u16::MAX)
        {
            return Err(RecipeError::InvalidField("correlation_data"));
        }
        if self.user_properties.len() > MAX_USER_PROPERTIES {
            return Err(RecipeError::InvalidField("user_properties_count"));
        }
        for (name, value) in self.user_properties.iter() {
            if !valid_mqtt_utf8(name.as_slice()) || !valid_mqtt_utf8(value.as_slice()) {
                return Err(RecipeError::InvalidField("user_property"));
            }
        }
        if self.logical_bytes() > MAX_PROPERTY_BYTES {
            return Err(RecipeError::InvalidField("publish_properties_size"));
        }
        Ok(())
    }

    /// Validates payload consistency with Payload Format Indicator (`0x01 == 1`
    /// requires RFC 3629 UTF-8 payload per MQTT 5.0 section 3.3.2.3.2).
    pub fn check_payload(&self, payload: &[u8]) -> Result<(), RecipeError> {
        if self.payload_format_indicator == Some(1) && core::str::from_utf8(payload).is_err() {
            return Err(RecipeError::InvalidField("payload_format_invalid"));
        }
        Ok(())
    }

    /// Appends the unique canonical byte representation of these properties.
    pub fn encode_canonical(&self, out: &mut Vec<u8>) -> Result<(), RecipeError> {
        self.check()?;
        let mut flags = 0u8;
        if self.payload_format_indicator.is_some() {
            flags |= FLAG_PAYLOAD_FORMAT;
        }
        if self.content_type.is_some() {
            flags |= FLAG_CONTENT_TYPE;
        }
        if self.response_topic.is_some() {
            flags |= FLAG_RESPONSE_TOPIC;
        }
        if self.correlation_data.is_some() {
            flags |= FLAG_CORRELATION_DATA;
        }
        if !self.user_properties.is_empty() {
            flags |= FLAG_USER_PROPERTIES;
        }
        out.push(flags);
        if let Some(pfi) = self.payload_format_indicator {
            out.push(pfi);
        }
        if let Some(value) = &self.content_type {
            out.extend_from_slice(&(value.len() as u16).to_be_bytes());
            out.extend_from_slice(value.as_slice());
        }
        if let Some(value) = &self.response_topic {
            out.extend_from_slice(&(value.len() as u16).to_be_bytes());
            out.extend_from_slice(value.as_slice());
        }
        if let Some(value) = &self.correlation_data {
            out.extend_from_slice(&(value.len() as u16).to_be_bytes());
            out.extend_from_slice(value.as_slice());
        }
        if !self.user_properties.is_empty() {
            out.extend_from_slice(&(self.user_properties.len() as u16).to_be_bytes());
            for (name, value) in self.user_properties.iter() {
                out.extend_from_slice(&(name.len() as u16).to_be_bytes());
                out.extend_from_slice(name.as_slice());
                out.extend_from_slice(&(value.len() as u16).to_be_bytes());
                out.extend_from_slice(value.as_slice());
            }
        }
        Ok(())
    }

    /// Decodes the unique canonical byte representation, refusing non-canonical
    /// flags, trailing bytes, or out-of-bounds values.
    pub fn decode_canonical(bytes: &[u8]) -> Result<Self, RecipeError> {
        let mut reader = Reader { bytes };
        let flags = reader.u8()?;
        if flags & !ALL_FLAGS != 0 {
            return Err(RecipeError::InvalidField("publish_properties_flags"));
        }
        let payload_format_indicator = if flags & FLAG_PAYLOAD_FORMAT != 0 {
            let v = reader.u8()?;
            if v > 1 {
                return Err(RecipeError::InvalidField("payload_format_indicator"));
            }
            Some(v)
        } else {
            None
        };
        let content_type = if flags & FLAG_CONTENT_TYPE != 0 {
            let len = usize::from(reader.u16()?);
            if len > MAX_PROPERTY_BYTES {
                return Err(RecipeError::InvalidField("publish_properties_size"));
            }
            let slice = reader.take(len)?;
            Some(
                SharedBytes::copy_from(slice, MAX_PROPERTY_BYTES)
                    .map_err(|_| RecipeError::InvalidField("publish_properties_size"))?,
            )
        } else {
            None
        };
        let response_topic = if flags & FLAG_RESPONSE_TOPIC != 0 {
            let len = usize::from(reader.u16()?);
            if len > MAX_PROPERTY_BYTES {
                return Err(RecipeError::InvalidField("publish_properties_size"));
            }
            let slice = reader.take(len)?;
            Some(
                SharedBytes::copy_from(slice, MAX_PROPERTY_BYTES)
                    .map_err(|_| RecipeError::InvalidField("publish_properties_size"))?,
            )
        } else {
            None
        };
        let correlation_data = if flags & FLAG_CORRELATION_DATA != 0 {
            let len = usize::from(reader.u16()?);
            if len > MAX_PROPERTY_BYTES {
                return Err(RecipeError::InvalidField("publish_properties_size"));
            }
            let slice = reader.take(len)?;
            Some(
                SharedBytes::copy_from(slice, MAX_PROPERTY_BYTES)
                    .map_err(|_| RecipeError::InvalidField("publish_properties_size"))?,
            )
        } else {
            None
        };
        let user_properties: Arc<[(SharedBytes, SharedBytes)]> =
            if flags & FLAG_USER_PROPERTIES != 0 {
                let count = usize::from(reader.u16()?);
                if count == 0 || count > MAX_USER_PROPERTIES {
                    return Err(RecipeError::InvalidField("user_properties_count"));
                }
                let mut pairs = Vec::new();
                pairs
                    .try_reserve_exact(count)
                    .map_err(|_| RecipeError::AllocationFailed)?;
                for _ in 0..count {
                    let klen = usize::from(reader.u16()?);
                    if klen > MAX_PROPERTY_BYTES {
                        return Err(RecipeError::InvalidField("publish_properties_size"));
                    }
                    let k = SharedBytes::copy_from(reader.take(klen)?, MAX_PROPERTY_BYTES)
                        .map_err(|_| RecipeError::InvalidField("publish_properties_size"))?;
                    let vlen = usize::from(reader.u16()?);
                    if vlen > MAX_PROPERTY_BYTES {
                        return Err(RecipeError::InvalidField("publish_properties_size"));
                    }
                    let v = SharedBytes::copy_from(reader.take(vlen)?, MAX_PROPERTY_BYTES)
                        .map_err(|_| RecipeError::InvalidField("publish_properties_size"))?;
                    pairs.push((k, v));
                }
                Arc::from(pairs)
            } else {
                Arc::from([])
            };
        if !reader.bytes.is_empty() {
            return Err(RecipeError::TrailingBytes);
        }
        let props = Self {
            payload_format_indicator,
            content_type,
            response_topic,
            correlation_data,
            user_properties,
        };
        props.check()?;
        Ok(props)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sb(s: &[u8]) -> SharedBytes {
        SharedBytes::copy_from(s, MAX_PROPERTY_BYTES).unwrap()
    }

    #[test]
    fn canonical_round_trip_preserves_every_property_and_order() {
        let props = PublishProperties {
            payload_format_indicator: Some(1),
            content_type: Some(sb(b"application/json")),
            response_topic: Some(sb(b"replies/client-1")),
            correlation_data: Some(sb(&[0x00, 0xfe, 0x01, 0xff])),
            user_properties: Arc::from([
                (sb(b"trace"), sb(b"first")),
                (sb(b"trace"), sb(b"second")),
                (sb(b"env"), sb(b"arm64")),
            ]),
        };
        let mut encoded = Vec::new();
        props.encode_canonical(&mut encoded).unwrap();
        let decoded = PublishProperties::decode_canonical(&encoded).unwrap();
        assert_eq!(decoded, props);
        assert_eq!(
            props.wire_len(),
            2 + (3 + 16) + (3 + 16) + (3 + 4) + (5 + 5 + 5) + (5 + 5 + 6) + (5 + 3 + 5)
        );
        for cut in 0..encoded.len() {
            assert!(PublishProperties::decode_canonical(&encoded[..cut]).is_err());
        }
        let mut trailing = encoded.clone();
        trailing.push(0);
        assert_eq!(
            PublishProperties::decode_canonical(&trailing),
            Err(RecipeError::TrailingBytes)
        );
    }

    #[test]
    fn noncanonical_and_out_of_domain_properties_are_rejected() {
        // Unknown flag bit.
        assert!(PublishProperties::decode_canonical(&[0x20]).is_err());
        // User-properties flag set with zero pairs.
        assert!(PublishProperties::decode_canonical(&[FLAG_USER_PROPERTIES, 0, 0]).is_err());
        // Wildcard in response topic.
        let bad_rt = PublishProperties {
            response_topic: Some(sb(b"replies/+")),
            ..PublishProperties::default()
        };
        assert!(bad_rt.check().is_err());
        // Invalid PFI value.
        let bad_pfi = PublishProperties {
            payload_format_indicator: Some(2),
            ..PublishProperties::default()
        };
        assert!(bad_pfi.check().is_err());
        // PFI=1 checks RFC 3629 UTF-8 payload (allowing newlines).
        let utf8_pfi = PublishProperties {
            payload_format_indicator: Some(1),
            ..PublishProperties::default()
        };
        assert!(utf8_pfi.check_payload(b"{\"k\": \"v\"}\n").is_ok());
        assert!(utf8_pfi.check_payload(&[0xff, 0xfe]).is_err());
    }
}
