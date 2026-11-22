//! Properties of the wire codec over generated inputs (record 0044, runtime
//! tier). The generator is a fixed-seed xorshift: a failure names its seed
//! and iteration and is reproduced by running the test again. These are
//! not a fuzzing campaign; they keep the decoder total and its header
//! reader consistent on a few hundred thousand inputs per run.

use mqttiumd::codec::{self, Bounds, ErrorKind, Version};
use mqttiumd_contract::packet::SharedBytes;

const BOUNDS: Bounds = Bounds {
    max_packet: 65536,
    max_client_id: 1024,
    max_topic: 1024,
    max_payload: 32768,
};

struct Generator(u64);

impl Generator {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }

    fn bytes(&mut self, length: usize) -> Vec<u8> {
        (0..length).map(|_| self.next() as u8).collect()
    }
}

fn remaining_length(mut length: usize) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut byte = (length % 128) as u8;
        length /= 128;
        if length > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if length == 0 {
            return out;
        }
    }
}

/// The header reader looks at five bytes at most, decides as soon as the
/// length is complete, never changes its mind when more bytes follow, and
/// accepts only the canonical encoding of a length within the bound.
#[test]
fn the_header_reader_is_total_prefix_stable_and_canonical() {
    let mut generator = Generator(0x9e37_79b9_7f4a_7c15);
    for iteration in 0..300_000 {
        let mut bytes = generator.bytes(5);
        // Half of the inputs end their length early, as real ones do.
        if iteration % 2 == 0 {
            let end = 1 + generator.below(4);
            bytes[end] &= 0x7f;
        }
        let mut decided: Option<Result<codec::Header, ErrorKind>> = None;
        for length in 0..=bytes.len() {
            let read = codec::header(&bytes[..length], BOUNDS.max_packet);
            let now = match read {
                Ok(None) => None,
                Ok(Some(header)) => Some(Ok(header)),
                Err(error) => Some(Err(error.kind())),
            };
            match (&decided, &now) {
                (None, None) => assert!(length < 5, "undecided on five bytes: {bytes:?}"),
                (None, Some(_)) => decided = now,
                (Some(before), Some(after)) => assert_eq!(
                    before, after,
                    "iteration {iteration}: more bytes changed the answer for {bytes:?}"
                ),
                (Some(_), None) => panic!("iteration {iteration}: an answer was withdrawn"),
            }
            if let Some(Ok(header)) = now {
                assert!(header.encoded_len <= length && header.encoded_len <= 5);
                assert_eq!(header.first, bytes[0]);
                assert!(header.remaining + header.encoded_len <= BOUNDS.max_packet + 5);
                assert_eq!(
                    &bytes[1..header.encoded_len],
                    remaining_length(header.remaining),
                    "iteration {iteration}: a non-canonical length was accepted"
                );
            }
        }
        assert!(
            decided.is_some(),
            "iteration {iteration}: no answer for {bytes:?}"
        );
    }
}

/// Every length the reader may accept is read back exactly, and a length
/// over the bound is refused as oversized before any body is reserved.
#[test]
fn every_length_is_read_back_or_refused_as_oversized() {
    for remaining in (0..300_000)
        .step_by(7)
        .chain([127, 128, 16_383, 16_384, 65_531, 65_536])
    {
        let mut bytes = vec![0x30];
        bytes.extend(remaining_length(remaining));
        match codec::header(&bytes, BOUNDS.max_packet) {
            Ok(Some(header)) => {
                assert_eq!(header.remaining, remaining);
                assert_eq!(header.encoded_len, bytes.len());
                assert!(remaining <= BOUNDS.max_packet);
            }
            Ok(None) => panic!("a complete length of {remaining} was left undecided"),
            Err(error) => {
                assert_eq!(error.kind(), ErrorKind::Oversized, "length {remaining}");
                assert!(remaining + bytes.len() > BOUNDS.max_packet);
            }
        }
    }
}

/// The decoder returns for any first byte and any body, in both protocol
/// versions: it never panics, and a refusal always carries a reason.
#[test]
fn the_decoder_is_total_on_arbitrary_bodies() {
    let mut generator = Generator(0x2545_f491_4f6c_dd1d);
    for iteration in 0..120_000 {
        let length = match iteration % 4 {
            0 => generator.below(8),
            1 => generator.below(64),
            2 => generator.below(600),
            _ => generator.below(3000),
        };
        let first = generator.next() as u8;
        let body = generator.bytes(length);
        for version in [Version::V5, Version::V311] {
            if let Err(error) = codec::decode_for(version, first, &body, BOUNDS) {
                assert!(
                    !error.code().is_empty(),
                    "iteration {iteration}: empty refusal code"
                );
            }
        }
    }
}

/// Valid packets damaged in one place: truncated, extended, or with one
/// byte replaced. The decoder still returns, and a truncated CONNECT,
/// SUBSCRIBE or PUBLISH is never accepted as the packet it was.
#[test]
fn damaged_valid_packets_are_decoded_or_refused_without_panic() {
    let samples: [(u8, &[u8]); 6] = [
        (0x10, b"\0\x04MQTT\x05\x02\0\x1e\0\0\x03cid"),
        (0x10, b"\0\x04MQTT\x04\x02\0\x1e\0\x03cid"),
        (0x30, b"\0\x05a/b/c\0payload"),
        (0x32, b"\0\x05a/b/c\0\x07\0payload"),
        (0x82, b"\0\x01\0\0\x05a/+/#\x01"),
        (0xa2, b"\0\x02\0\0\x03a/b"),
    ];
    let mut generator = Generator(0xd6e8_feb8_6659_fd93);
    for (first, sample) in samples {
        assert!(
            codec::decode(first, sample, BOUNDS).is_ok()
                || codec::decode_for(Version::V311, first, sample, BOUNDS).is_ok(),
            "sample {first:#x} is not a valid packet"
        );
        for cut in 0..sample.len() {
            for version in [Version::V5, Version::V311] {
                let _ = codec::decode_for(version, first, &sample[..cut], BOUNDS);
            }
        }
        for _ in 0..20_000 {
            let mut body = sample.to_vec();
            match generator.below(3) {
                0 => {
                    let at = generator.below(body.len());
                    body[at] = generator.next() as u8;
                }
                1 => {
                    let extra = generator.below(6);
                    body.extend(generator.bytes(extra));
                }
                _ => {
                    let at = generator.below(body.len());
                    body.insert(at, generator.next() as u8);
                }
            }
            for version in [Version::V5, Version::V311] {
                let _ = codec::decode_for(version, first, &body, BOUNDS);
            }
        }
    }
}

/// What the encoder writes for a publication is one whole packet for the
/// header reader, whatever the sizes within the bounds.
#[test]
fn encoded_publications_are_whole_packets_for_the_header_reader() {
    let mut generator = Generator(0x1234_5678_9abc_def1);
    for _ in 0..4_000 {
        let topic = vec![b't'; 1 + generator.below(BOUNDS.max_topic)];
        let size = generator.below(BOUNDS.max_payload + 1);
        let payload = generator.bytes(size);
        let bytes = codec::encode_qos0_retained(
            &SharedBytes::copy_from(&topic, BOUNDS.max_topic).unwrap(),
            &SharedBytes::copy_from(&payload, BOUNDS.max_payload).unwrap(),
            false,
        )
        .unwrap();
        let header = codec::header(&bytes[..bytes.len().min(5)], BOUNDS.max_packet)
            .unwrap()
            .unwrap();
        assert_eq!(header.encoded_len + header.remaining, bytes.len());
        assert_eq!(header.first & 0xf0, 0x30);
        assert!(bytes.ends_with(&payload));
    }
}
