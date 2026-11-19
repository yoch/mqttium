use mqttiumd::codec::{self, Bounds, Packet, Version::V311};
use mqttiumd_contract::ids::{InboundPacketId, OutboundPacketId};
use mqttiumd_contract::packet::{AckReason, InboundPacket, OutboundFrame, PubackReason, Qos};
use mqttiumd_contract::pubsub::FilterResults;
use mqttiumd_contract::session::SessionExpiry;

const BOUNDS: Bounds = Bounds {
    max_packet: 65536,
    max_client_id: 1024,
    max_topic: 1024,
    max_payload: 32768,
};

#[test]
fn clean_session_translates_to_fresh_or_persistent_without_properties() {
    for (flags, expiry) in [(0, SessionExpiry::Never), (2, SessionExpiry::OnDisconnect)] {
        let mut body = *b"\0\x04MQTT\x04\0\0\x1e\0\x03cid";
        body[7] = flags;
        let Packet::Connect(connect) = codec::decode(0x10, &body, BOUNDS).unwrap() else {
            panic!("expected CONNECT");
        };
        assert_eq!(connect.version, V311);
        assert_eq!(connect.expiry, expiry);
        assert_eq!(connect.clean_start, flags == 2);
        assert_eq!(connect.client_id.as_slice(), b"cid");
        assert_eq!(
            codec::encode_for(
                V311,
                OutboundFrame::Connack {
                    session_present: flags == 0
                }
            )
            .unwrap(),
            [0x20, 2, u8::from(flags == 0), 0]
        );
    }
    let unknown = b"\0\x04MQTT\x06\x02\0\0\0\x03cid";
    let error = codec::decode(0x10, unknown, BOUNDS).unwrap_err();
    assert_eq!(codec::connect_response_version(unknown), Some(V311));
    assert_eq!(
        codec::refusal_for(V311, error, true),
        Some(vec![0x20, 2, 0, 1])
    );
    let empty = b"\0\x04MQTT\x04\0\0\0\0\0";
    let error = codec::decode(0x10, empty, BOUNDS).unwrap_err();
    assert_eq!(
        codec::refusal_for(V311, error, true),
        Some(vec![0x20, 2, 0, 2])
    );
    assert_eq!(
        codec::session_refusal_for(V311, 0x87).unwrap(),
        [0x20, 2, 0, 5]
    );
}

#[test]
fn legacy_publish_keeps_binary_payload_and_has_no_properties() {
    for (header, body, qos) in [
        (0x30, b"\0\x01t\0\xff\x02".as_slice(), Qos::AtMostOnce),
        (
            0x32,
            b"\0\x01t\0\x07\0\xff\x02".as_slice(),
            Qos::AtLeastOnce,
        ),
    ] {
        let Packet::Core(InboundPacket::Publish {
            topic,
            payload,
            qos: actual,
            expiry: None,
            ..
        }) = codec::decode_for(V311, header, body, BOUNDS).unwrap()
        else {
            panic!("expected plain PUBLISH");
        };
        assert_eq!(
            (actual, topic.as_slice(), payload.as_slice()),
            (qos, b"t".as_slice(), b"\0\xff\x02".as_slice())
        );
        let encoded = if qos == Qos::AtMostOnce {
            codec::encode_qos0_for(V311, &topic, &payload, false, Some(120))
        } else {
            codec::encode_publish_for(
                V311,
                OutboundPacketId::new(7).unwrap(),
                &topic,
                &payload,
                false,
                false,
                Some(120),
            )
        }
        .unwrap();
        assert_eq!(encoded.expiry_value_offset, None);
        assert_eq!(&encoded.bytes[..2], &[header, body.len() as u8]);
        assert_eq!(&encoded.bytes[2..], body);
    }
}

#[test]
fn legacy_qos2_uses_only_the_packet_identifier_in_acknowledgements() {
    let mid = InboundPacketId::new(7).unwrap();
    for (header, duplicate) in [(0x34, false), (0x3c, true)] {
        let Packet::Core(InboundPacket::Publish {
            packet_id: Some(packet_id),
            payload,
            qos: Qos::ExactlyOnce,
            dup,
            expiry: None,
            ..
        }) = codec::decode_for(V311, header, b"\0\x01t\0\x07\0\xff", BOUNDS).unwrap()
        else {
            panic!("expected legacy QoS 2 PUBLISH");
        };
        assert_eq!(
            (packet_id, dup, payload.as_slice()),
            (mid, duplicate, &[0, 255][..])
        );
    }
    assert!(codec::decode_for(V311, 0x34, b"\0\x01t\0\0payload", BOUNDS).is_err());
    assert!(matches!(
        codec::decode_for(V311, 0x62, &[0, 7], BOUNDS),
        Ok(Packet::Core(InboundPacket::Pubrel { packet_id })) if packet_id == mid
    ));
    for (header, body) in [
        (0x62, &[0, 0][..]),
        (0x62, &[7]),
        (0x62, &[0, 7, 0]),
        (0x62, &[0, 7, 0, 0]),
        (0x6a, &[0, 7]),
    ] {
        assert!(codec::decode_for(V311, header, body, BOUNDS).is_err());
    }
    assert_eq!(
        codec::encode_for(
            V311,
            OutboundFrame::Pubrec {
                packet_id: mid,
                reason: AckReason::Success
            }
        )
        .unwrap(),
        [0x50, 2, 0, 7]
    );
    for reason in [AckReason::Success, AckReason::PacketIdentifierNotFound] {
        assert_eq!(
            codec::encode_for(
                V311,
                OutboundFrame::Pubcomp {
                    packet_id: mid,
                    reason
                }
            )
            .unwrap(),
            [0x70, 2, 0, 7]
        );
    }
    for reason in [
        AckReason::QuotaExceeded,
        AckReason::ImplementationSpecificError,
    ] {
        assert!(
            codec::encode_for(
                V311,
                OutboundFrame::Pubrec {
                    packet_id: mid,
                    reason
                }
            )
            .is_err()
        );
    }
}

#[test]
fn versioned_subscription_acknowledgements_and_disconnect() {
    assert!(matches!(
        codec::decode_for(V311, 0x82, b"\0\x07\0\x03t/+\x01", BOUNDS),
        Ok(Packet::Core(InboundPacket::Subscribe { .. }))
    ));
    assert!(codec::decode_for(V311, 0x82, b"\0\x07\0\x03t/+\x09", BOUNDS).is_err());
    assert!(matches!(
        codec::decode_for(V311, 0xa2, b"\0\x07\0\x03t/+", BOUNDS),
        Ok(Packet::Core(InboundPacket::Unsubscribe { .. }))
    ));
    let mid = InboundPacketId::new(7).unwrap();
    assert_eq!(
        codec::encode_for(
            V311,
            OutboundFrame::Suback {
                packet_id: mid,
                results: FilterResults::new(&[1, 0x97]).unwrap()
            }
        )
        .unwrap(),
        [0x90, 4, 0, 7, 1, 0x80]
    );
    assert_eq!(
        codec::encode_for(
            V311,
            OutboundFrame::Unsuback {
                packet_id: mid,
                results: FilterResults::new(&[0, 0x11]).unwrap()
            }
        )
        .unwrap(),
        [0xb0, 2, 0, 7]
    );
    assert_eq!(
        codec::encode_for(
            V311,
            OutboundFrame::Puback {
                packet_id: mid,
                reason: PubackReason::Success
            }
        )
        .unwrap(),
        [0x40, 2, 0, 7]
    );
    assert!(
        codec::encode_for(
            V311,
            OutboundFrame::Puback {
                packet_id: mid,
                reason: PubackReason::QuotaExceeded
            }
        )
        .is_err()
    );
    assert!(matches!(
        codec::decode_for(V311, 0x40, &[0, 7], BOUNDS),
        Ok(Packet::Core(InboundPacket::Puback { reason: 0, .. }))
    ));
    assert!(codec::decode_for(V311, 0x40, &[0, 7, 0], BOUNDS).is_err());
    assert!(matches!(
        codec::decode_for(V311, 0xe0, &[], BOUNDS),
        Ok(Packet::Disconnect {
            expiry: None,
            publish_will: false
        })
    ));
    assert!(codec::decode_for(V311, 0xe0, &[0], BOUNDS).is_err());
    assert_eq!(
        codec::encode_for(V311, OutboundFrame::Pingresp).unwrap(),
        [0xd0, 0]
    );
}
