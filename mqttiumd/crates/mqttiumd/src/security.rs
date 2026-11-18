//! Static authentication and topic authorization at the transport boundary.

use crate::codec;
use mqttiumd_contract::packet::InboundPacket;
use mqttiumd_store_sqlite::StoragePolicy;
use rustls::client::danger::HandshakeSignatureValid;
use rustls::client::verify_server_cert_signed_by_trust_anchor;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::{NoServerSessionStorage, ParsedCertificate, WebPkiClientVerifier};
use rustls::{
    CertificateError, DigitallySignedStruct, DistinguishedName, RootCertStore, ServerConfig,
    SignatureScheme,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{Cursor, Read};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const MAX_UNAUTHENTICATED: usize = 8;
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);
pub const HANDSHAKE_BYTES: usize = 64 * 1024;
pub const CERT_BYTES: usize = 8 * 1024;
pub const CHAIN_BYTES: usize = 32 * 1024;
pub const CHAIN_CERTS: usize = 4;
pub const TLS_BUFFER_BYTES: usize = 96 * 1024;
const PEM_BYTES: usize = 48 * 1024;
const CONFIG_BYTES: usize = 256 * 1024;
const MAX_PRINCIPALS: usize = 128;
const MAX_PINS: usize = 256;
const MAX_CLIENT_IDS: usize = 128;
const MAX_RULES: usize = 512;
const FIELD_BYTES: usize = 1024;

#[derive(Clone)]
pub struct TlsFiles {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub client_ca: PathBuf,
    pub identities: PathBuf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Principal(usize);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    version: u32,
    principals: Vec<Identity>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    name: String,
    certificates: Vec<String>,
    client_ids: Vec<String>,
    publish: Vec<String>,
    subscribe: Vec<String>,
}

pub struct Policy {
    by_cert: Arc<BTreeMap<[u8; 32], Principal>>,
    storage: StoragePolicy,
    rules: Vec<Identity>,
}

pub struct Security {
    pub tls: Arc<ServerConfig>,
    pub policy: Arc<Policy>,
}

fn bounded_file(path: &Path, maximum: usize) -> Result<Vec<u8>, &'static str> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| "security_file_unavailable")?;
    let metadata = file.metadata().map_err(|_| "security_file_unavailable")?;
    if !metadata.is_file() || metadata.len() > maximum as u64 {
        return Err("security_file_budget");
    }
    let mut bytes = Vec::new();
    file.take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "security_file_unavailable")?;
    if bytes.len() > maximum {
        return Err("security_file_budget");
    }
    Ok(bytes)
}

fn fingerprint(cert: &CertificateDer<'_>) -> [u8; 32] {
    Sha256::digest(cert.as_ref()).into()
}

fn parse_fingerprint(value: &str) -> Result<[u8; 32], &'static str> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err("invalid_certificate_fingerprint");
    }
    let mut bytes = [0; 32];
    for (pair, byte) in value.as_bytes().chunks_exact(2).zip(&mut bytes) {
        let hex = std::str::from_utf8(pair).map_err(|_| "invalid_certificate_fingerprint")?;
        *byte = u8::from_str_radix(hex, 16).map_err(|_| "invalid_certificate_fingerprint")?;
    }
    Ok(bytes)
}

fn mqtt_text(value: &str) -> bool {
    !value.is_empty() && value.len() <= FIELD_BYTES && codec::utf8(value.as_bytes()).is_ok()
}

fn literal_prefix(value: &str) -> bool {
    mqtt_text(value) && codec::topic(value.as_bytes()).is_ok() && !value.starts_with("$share/")
}

// A grant includes the root topic and descendants. For a validated filter,
// the same test requires that every level in the granted prefix is literal.
fn within(candidate: &[u8], prefix: &str) -> bool {
    candidate
        .strip_prefix(prefix.as_bytes())
        .is_some_and(|rest| rest.is_empty() || rest.first() == Some(&b'/'))
}

impl Policy {
    fn parse(bytes: &[u8]) -> Result<Self, &'static str> {
        let mut config: Configuration =
            serde_json::from_slice(bytes).map_err(|_| "invalid_identities_json")?;
        if config.version != 1
            || config.principals.is_empty()
            || config.principals.len() > MAX_PRINCIPALS
        {
            return Err("invalid_identity_count_or_version");
        }
        config
            .principals
            .sort_unstable_by(|a, b| a.name.cmp(&b.name));
        let mut pins = BTreeMap::new();
        let mut owners = BTreeMap::new();
        let mut hash = Sha256::new();
        hash.update(b"mqttiumd/static-segment-prefix-policy/v1\0");
        let mut rules = 0;
        let mut previous: Option<&str> = None;
        for (index, identity) in config.principals.iter_mut().enumerate() {
            if identity.name.is_empty()
                || identity.name.len() > 64
                || !identity
                    .name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
                || previous == Some(identity.name.as_str())
                || identity.certificates.is_empty()
                || identity.client_ids.is_empty()
            {
                return Err("invalid_principal");
            }
            previous = Some(&identity.name);
            for pin in &identity.certificates {
                if pins.len() == MAX_PINS
                    || pins
                        .insert(parse_fingerprint(pin)?, Principal(index))
                        .is_some()
                {
                    return Err("certificate_allowlist_conflict_or_budget");
                }
            }
            for client in &identity.client_ids {
                if !mqtt_text(client)
                    || owners.len() == MAX_CLIENT_IDS
                    || owners
                        .insert(client.as_bytes().to_vec(), identity.name.clone())
                        .is_some()
                {
                    return Err("client_id_conflict_or_budget");
                }
            }
            // This canonical fingerprint includes every authorization input.
            // Certificate leaf pins are intentionally excluded for rotation.
            hash.update((identity.name.len() as u32).to_be_bytes());
            hash.update(&identity.name);
            for field in [
                &mut identity.client_ids,
                &mut identity.publish,
                &mut identity.subscribe,
            ] {
                field.sort_unstable();
                if field.windows(2).any(|pair| pair[0] == pair[1]) {
                    return Err("duplicate_policy_rule");
                }
                hash.update((field.len() as u32).to_be_bytes());
                for value in field.iter() {
                    hash.update((value.len() as u32).to_be_bytes());
                    hash.update(value);
                }
            }
            rules += identity.publish.len() + identity.subscribe.len();
            if rules > MAX_RULES
                || !identity
                    .publish
                    .iter()
                    .chain(&identity.subscribe)
                    .all(|p| literal_prefix(p))
            {
                return Err("invalid_topic_prefix_or_rule_budget");
            }
        }
        Ok(Self {
            by_cert: Arc::new(pins),
            storage: StoragePolicy {
                fingerprint: hash.finalize().into(),
                owners,
            },
            rules: config.principals,
        })
    }

    pub fn storage(&self) -> &StoragePolicy {
        &self.storage
    }

    pub fn certificate(&self, leaf: &CertificateDer<'_>) -> Option<Principal> {
        self.by_cert.get(&fingerprint(leaf)).copied()
    }

    pub fn allows_client(&self, principal: Principal, id: &[u8]) -> bool {
        self.storage
            .owners
            .get(id)
            .is_some_and(|owner| owner == &self.rules[principal.0].name)
    }

    pub fn allows_publish(&self, principal: Principal, topic: &[u8]) -> bool {
        self.rules[principal.0]
            .publish
            .iter()
            .any(|prefix| within(topic, prefix))
    }

    fn allows_filter(&self, principal: Principal, filter: &[u8]) -> bool {
        self.rules[principal.0]
            .subscribe
            .iter()
            .any(|prefix| within(filter, prefix))
    }

    pub fn allows_packet(&self, principal: Principal, packet: &InboundPacket) -> bool {
        match packet {
            InboundPacket::Publish { topic, .. } => {
                self.allows_publish(principal, topic.as_slice())
            }
            InboundPacket::Subscribe { filters, .. } => filters
                .as_slice()
                .iter()
                .all(|item| self.allows_filter(principal, item.filter.as_slice())),
            InboundPacket::Unsubscribe { filters, .. } => filters
                .as_slice()
                .iter()
                .all(|filter| self.allows_filter(principal, filter.as_slice())),
            _ => true,
        }
    }
}

fn chain_bounded(chain: &[CertificateDer<'_>]) -> bool {
    !chain.is_empty()
        && chain.len() <= CHAIN_CERTS
        && chain
            .iter()
            .all(|cert| !cert.is_empty() && cert.len() <= CERT_BYTES)
        && chain.iter().map(|cert| cert.len()).sum::<usize>() <= CHAIN_BYTES
}

fn certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, &'static str> {
    let bytes = bounded_file(path, PEM_BYTES)?;
    let entries = rustls_pemfile::read_all(&mut Cursor::new(bytes))
        .map(|entry| match entry {
            Ok(rustls_pemfile::Item::X509Certificate(cert)) => Ok(cert),
            _ => Err("invalid_certificate_file"),
        })
        .take(CHAIN_CERTS + 1)
        .collect::<Result<Vec<_>, _>>()?;
    if !chain_bounded(&entries) {
        return Err("certificate_chain_budget");
    }
    Ok(entries)
}

fn private_key(path: &Path) -> Result<PrivateKeyDer<'static>, &'static str> {
    // The PEM buffer and DER key are wiped on drop. Keep this material out of
    // Debug, product events, process arguments and reception archives.
    let bytes = zeroize::Zeroizing::new(bounded_file(path, 16 * 1024)?);
    let mut input = Cursor::new(bytes.as_slice());
    let key = match rustls_pemfile::read_one(&mut input).map_err(|_| "invalid_private_key")? {
        Some(rustls_pemfile::Item::Pkcs8Key(key)) => PrivateKeyDer::Pkcs8(key),
        Some(rustls_pemfile::Item::Pkcs1Key(key)) => PrivateKeyDer::Pkcs1(key),
        Some(rustls_pemfile::Item::Sec1Key(key)) => PrivateKeyDer::Sec1(key),
        _ => return Err("invalid_private_key"),
    };
    if rustls_pemfile::read_one(&mut input)
        .map_err(|_| "invalid_private_key")?
        .is_some()
    {
        return Err("invalid_private_key");
    }
    Ok(key)
}

#[derive(Debug)]
struct AllowedClients {
    standard: Arc<dyn ClientCertVerifier>,
    pins: Arc<BTreeMap<[u8; 32], Principal>>,
}

impl ClientCertVerifier for AllowedClients {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.standard.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        leaf: &CertificateDer<'_>,
        rest: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        if rest.len() >= CHAIN_CERTS
            || leaf.is_empty()
            || leaf.len() > CERT_BYTES
            || rest
                .iter()
                .any(|cert| cert.is_empty() || cert.len() > CERT_BYTES)
            || leaf.len() + rest.iter().map(|cert| cert.len()).sum::<usize>() > CHAIN_BYTES
        {
            return Err(rustls::Error::InvalidCertificate(
                CertificateError::BadEncoding,
            ));
        }
        let verified = self.standard.verify_client_cert(leaf, rest, now)?;
        if !self.pins.contains_key(&fingerprint(leaf)) {
            return Err(rustls::Error::InvalidCertificate(
                CertificateError::ApplicationVerificationFailure,
            ));
        }
        Ok(verified)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        sig: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.standard.verify_tls12_signature(message, cert, sig)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        sig: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.standard.verify_tls13_signature(message, cert, sig)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.standard.supported_verify_schemes()
    }
}

impl Security {
    pub fn load(files: &TlsFiles) -> Result<Self, &'static str> {
        let policy = Arc::new(Policy::parse(&bounded_file(
            &files.identities,
            CONFIG_BYTES,
        )?)?);
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut roots = RootCertStore::empty();
        for cert in certificates(&files.client_ca)? {
            roots.add(cert).map_err(|_| "invalid_trust_anchor")?;
        }
        let chain = certificates(&files.cert)?;
        let now = certificate_time().ok_or("certificate_clock_unavailable")?;
        let parsed =
            ParsedCertificate::try_from(&chain[0]).map_err(|_| "invalid_server_certificate")?;
        verify_server_cert_signed_by_trust_anchor(
            &parsed,
            &roots,
            &chain[1..],
            now,
            provider.signature_verification_algorithms.all,
        )
        .map_err(|_| "server_certificate_chain_or_time")?;
        let standard =
            WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
                .build()
                .map_err(|_| "invalid_client_ca")?;
        let verifier = Arc::new(AllowedClients {
            standard,
            pins: policy.by_cert.clone(),
        });
        let mut config = ServerConfig::builder_with_details(provider, Arc::new(CertificateClock))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|_| "tls_configuration")?
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain, private_key(&files.key)?)
            .map_err(|_| "server_certificate_key_mismatch")?;
        config.session_storage = Arc::new(NoServerSessionStorage {});
        config.send_tls13_tickets = 0;
        config.max_early_data_size = 0;
        config.send_half_rtt_data = false;
        Ok(Self {
            tls: Arc::new(config),
            policy,
        })
    }
}

fn certificate_time() -> Option<UnixTime> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(UnixTime::since_unix_epoch)
}

#[derive(Debug)]
struct CertificateClock;
impl rustls::time_provider::TimeProvider for CertificateClock {
    fn current_time(&self) -> Option<UnixTime> {
        certificate_time()
    }
}
