//! BOOTTIME authority for finite session retention, isolated from the pure core.
//!
//! A tick is an exact, checked nanosecond count. UTC and `Instant` never enter
//! the durable time contract. An unrecognizable origin gets a fresh process
//! domain: recovery must commit a conservative anchor rather than invent time.
use mqttiumd_store_api::{SessionClockAnchor, SessionClockReading};
use nix::time::{ClockId, clock_gettime};
use std::fs::File;
use std::io::Read;
use std::os::unix::fs::MetadataExt;

const NANOS_PER_SECOND: u64 = 1_000_000_000;
const BOOT_ID_BYTES: usize = 64;
const OFFSET_BYTES: usize = 256;
const RECOGNIZED_DOMAIN: [u8; 4] = *b"BT01";
const PROCESS_DOMAIN: [u8; 4] = *b"BP01";

/// Stable programmatic causes; no diagnostic copies a procfs value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeError {
    ClockUnavailable,
    RandomUnavailable,
    InvalidReading,
    ClockRegressed,
    DomainChanged,
    ArithmeticOverflow,
}
impl TimeError {
    pub fn code(self) -> &'static str {
        match self {
            Self::ClockUnavailable => "session_clock_unavailable",
            Self::RandomUnavailable => "session_clock_random_unavailable",
            Self::InvalidReading => "session_clock_invalid_reading",
            Self::ClockRegressed => "session_clock_regressed",
            Self::DomainChanged => "session_clock_domain_changed",
            Self::ArithmeticOverflow => "session_clock_overflow",
        }
    }
}
impl std::fmt::Display for TimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}
impl std::error::Error for TimeError {}

/// The adapter owns the provider. Tests inject readings, never host clock changes.
pub trait SessionClock: Send + Sync {
    fn read(&self) -> Result<SessionClockReading, TimeError>;

    fn origin_status(&self) -> &'static str {
        "injected_clock"
    }
}

pub struct SystemClock {
    domain: [u8; 32],
    origin_status: &'static str,
}
impl SystemClock {
    pub fn new() -> Result<Self, TimeError> {
        let (domain, origin_status) = match recognize_domain() {
            Ok(domain) => (domain, "boottime_origin_recognized"),
            Err(cause) => {
                let mut domain = [0; 32];
                getrandom::fill(&mut domain[..28]).map_err(|_| TimeError::RandomUnavailable)?;
                domain[28..].copy_from_slice(&PROCESS_DOMAIN);
                (domain, cause)
            }
        };
        let clock = Self {
            domain,
            origin_status,
        };
        clock.read()?;
        Ok(clock)
    }

    pub fn origin_status(&self) -> &'static str {
        self.origin_status
    }

    pub fn origin_is_recognized(&self) -> bool {
        self.origin_status == "boottime_origin_recognized"
    }
}
impl SessionClock for SystemClock {
    fn origin_status(&self) -> &'static str {
        self.origin_status
    }

    fn read(&self) -> Result<SessionClockReading, TimeError> {
        let time =
            clock_gettime(ClockId::CLOCK_BOOTTIME).map_err(|_| TimeError::ClockUnavailable)?;
        Ok(SessionClockReading {
            domain: self.domain,
            tick: tick_from_parts(time.tv_sec(), time.tv_nsec())?,
        })
    }
}

/// Project only within an already established domain. A changed domain must
/// first be anchored durably at the previously committed logical progress.
pub fn logical_now(
    anchor: SessionClockAnchor,
    reading: SessionClockReading,
) -> Result<u64, TimeError> {
    if anchor.reading.domain != reading.domain {
        return Err(TimeError::DomainChanged);
    }
    let elapsed = reading
        .tick
        .checked_sub(anchor.reading.tick)
        .ok_or(TimeError::ClockRegressed)?;
    anchor
        .logical
        .checked_add(elapsed)
        .ok_or(TimeError::ArithmeticOverflow)
}

fn tick_from_parts(seconds: i64, nanoseconds: i64) -> Result<u64, TimeError> {
    let seconds = u64::try_from(seconds).map_err(|_| TimeError::InvalidReading)?;
    let nanoseconds = u64::try_from(nanoseconds).map_err(|_| TimeError::InvalidReading)?;
    if nanoseconds >= NANOS_PER_SECOND {
        return Err(TimeError::InvalidReading);
    }
    seconds
        .checked_mul(NANOS_PER_SECOND)
        .and_then(|whole| whole.checked_add(nanoseconds))
        .ok_or(TimeError::ArithmeticOverflow)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct NamespaceIdentity {
    device: u64,
    inode: u64,
}
fn namespace(path: &str) -> Result<NamespaceIdentity, &'static str> {
    let metadata = std::fs::metadata(path).map_err(|_| "boottime_namespace_unavailable")?;
    Ok(NamespaceIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

/// Fixed buffers put the budget before reading, independently of procfs trust.
fn read_bounded<const N: usize>(path: &str) -> Result<([u8; N], usize), &'static str> {
    let mut file = File::open(path).map_err(|_| "boottime_origin_unavailable")?;
    let mut bytes = [0; N];
    let mut length = 0;
    while length < N {
        match file.read(&mut bytes[length..]) {
            Ok(0) => return Ok((bytes, length)),
            Ok(count) => length += count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return Err("boottime_origin_unavailable"),
        }
    }
    let mut extra = [0];
    loop {
        match file.read(&mut extra) {
            Ok(0) => return Ok((bytes, length)),
            Ok(_) => return Err("boottime_origin_limit"),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return Err("boottime_origin_unavailable"),
        }
    }
}

fn recognize_domain() -> Result<[u8; 32], &'static str> {
    let current = namespace("/proc/thread-self/ns/time")?;
    let children = namespace("/proc/thread-self/ns/time_for_children")?;
    // timens_offsets belongs to the children namespace. Without this equality
    // its offset cannot establish the calling thread's BOOTTIME origin.
    if current != children {
        return Err("boottime_namespace_mismatch");
    }
    let (boot, boot_length) = read_bounded::<BOOT_ID_BYTES>("/proc/sys/kernel/random/boot_id")?;
    let (offset, offset_length) = read_bounded::<OFFSET_BYTES>("/proc/thread-self/timens_offsets")?;
    let domain = domain_from_evidence(
        &boot[..boot_length],
        &offset[..offset_length],
        current,
        children,
    )?;
    if namespace("/proc/thread-self/ns/time")? != current
        || namespace("/proc/thread-self/ns/time_for_children")? != children
    {
        return Err("boottime_namespace_changed");
    }
    let (second_boot, second_length) =
        read_bounded::<BOOT_ID_BYTES>("/proc/sys/kernel/random/boot_id")?;
    if second_boot[..second_length] != boot[..boot_length] {
        return Err("boottime_boot_changed");
    }
    Ok(domain)
}

fn domain_from_evidence(
    boot: &[u8],
    offsets: &[u8],
    current: NamespaceIdentity,
    children: NamespaceIdentity,
) -> Result<[u8; 32], &'static str> {
    if current != children {
        return Err("boottime_namespace_mismatch");
    }
    let boot = std::str::from_utf8(boot).map_err(|_| "boottime_boot_invalid")?;
    let boot = boot.strip_suffix('\n').unwrap_or(boot).as_bytes();
    if boot.len() != 36 {
        return Err("boottime_boot_invalid");
    }
    let mut uuid = [0; 16];
    let mut out = 0;
    let mut high = None;
    for (index, byte) in boot.iter().copied().enumerate() {
        if [8, 13, 18, 23].contains(&index) {
            if byte != b'-' {
                return Err("boottime_boot_invalid");
            }
            continue;
        }
        let nibble = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => return Err("boottime_boot_invalid"),
        };
        if let Some(first) = high.take() {
            uuid[out] = first * 16 + nibble;
            out += 1;
        } else {
            high = Some(nibble);
        }
    }
    if out != uuid.len() || high.is_some() {
        return Err("boottime_boot_invalid");
    }
    let offsets = std::str::from_utf8(offsets).map_err(|_| "boottime_offset_invalid")?;
    let mut boottime = None;
    let mut monotonic = false;
    for line in offsets.lines() {
        let mut fields = line.split_ascii_whitespace();
        let clock = fields.next().ok_or("boottime_offset_invalid")?;
        let seconds = fields
            .next()
            .ok_or("boottime_offset_invalid")?
            .parse::<i64>()
            .map_err(|_| "boottime_offset_invalid")?;
        let nanos = fields
            .next()
            .ok_or("boottime_offset_invalid")?
            .parse::<u32>()
            .map_err(|_| "boottime_offset_invalid")?;
        if nanos >= 1_000_000_000 || fields.next().is_some() {
            return Err("boottime_offset_invalid");
        }
        match clock {
            "monotonic" if !monotonic => monotonic = true,
            "boottime" if boottime.is_none() => boottime = Some((seconds, nanos)),
            _ => return Err("boottime_offset_invalid"),
        }
    }
    let (seconds, nanos) = boottime.ok_or("boottime_offset_missing")?;
    if !monotonic {
        return Err("boottime_offset_invalid");
    }
    // Same kernel boot and same effective offset mean the same BOOTTIME
    // origin, even for two namespaces deliberately using equal offsets.
    let mut domain = [0; 32];
    domain[..16].copy_from_slice(&uuid);
    domain[16..24].copy_from_slice(&seconds.to_be_bytes());
    domain[24..28].copy_from_slice(&nanos.to_be_bytes());
    domain[28..].copy_from_slice(&RECOGNIZED_DOMAIN);
    Ok(domain)
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use std::sync::Mutex;

    pub struct FakeClock {
        reading: Mutex<Result<SessionClockReading, TimeError>>,
    }
    impl FakeClock {
        pub fn new(reading: SessionClockReading) -> Self {
            Self {
                reading: Mutex::new(Ok(reading)),
            }
        }
        pub fn set(&self, reading: SessionClockReading) {
            *self.reading.lock().unwrap() = Ok(reading);
        }
        pub fn fail(&self, cause: TimeError) {
            *self.reading.lock().unwrap() = Err(cause);
        }
    }
    impl SessionClock for FakeClock {
        fn read(&self) -> Result<SessionClockReading, TimeError> {
            *self.reading.lock().unwrap()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BOOT: &[u8] = b"94220b48-8eaf-49a1-87de-18b0392e1094\n";
    const OFFSETS: &[u8] = b"monotonic           0         0\nboottime            0         0\n";
    fn ns(inode: u64) -> NamespaceIdentity {
        NamespaceIdentity { device: 1, inode }
    }
    fn reading(domain: u8, tick: u64) -> SessionClockReading {
        SessionClockReading {
            domain: [domain; 32],
            tick,
        }
    }

    #[test]
    fn same_origin_progress_survives_repeated_process_anchors() {
        let initial = SessionClockAnchor {
            reading: reading(1, 100),
            logical: 5_000,
        };
        let second = reading(1, 150);
        assert_eq!(logical_now(initial, second), Ok(5_050));
        let committed = SessionClockAnchor {
            reading: second,
            logical: logical_now(initial, second).unwrap(),
        };
        assert_eq!(logical_now(committed, reading(1, 170)), Ok(5_070));
        assert_eq!(logical_now(initial, reading(1, 170)), Ok(5_070));
    }

    #[test]
    fn changed_origin_cannot_invent_downtime_or_reset_committed_progress() {
        let old = SessionClockAnchor {
            reading: reading(1, 1_000_000),
            logical: 500,
        };
        let next = reading(2, u64::MAX - 100);
        assert_eq!(logical_now(old, next), Err(TimeError::DomainChanged));
        let new_anchor = SessionClockAnchor {
            reading: next,
            logical: old.logical,
        };
        assert_eq!(logical_now(new_anchor, reading(2, u64::MAX - 90)), Ok(510));
    }

    #[test]
    fn backward_clock_and_overflow_are_refusals_not_expiration() {
        let anchor = SessionClockAnchor {
            reading: reading(1, 100),
            logical: u64::MAX - 1,
        };
        assert_eq!(
            logical_now(anchor, reading(1, 99)),
            Err(TimeError::ClockRegressed)
        );
        assert_eq!(
            logical_now(anchor, reading(1, 102)),
            Err(TimeError::ArithmeticOverflow)
        );
        assert_eq!(tick_from_parts(-1, 0), Err(TimeError::InvalidReading));
        assert_eq!(tick_from_parts(1, -1), Err(TimeError::InvalidReading));
        assert_eq!(
            tick_from_parts(1, 1_000_000_000),
            Err(TimeError::InvalidReading)
        );
        assert_eq!(
            tick_from_parts(i64::MAX, 0),
            Err(TimeError::ArithmeticOverflow)
        );
        assert_eq!(tick_from_parts(1, 999_999_999), Ok(1_999_999_999));
    }

    #[test]
    fn boot_and_effective_namespace_offset_identify_the_clock_origin() {
        let domain = domain_from_evidence(BOOT, OFFSETS, ns(10), ns(10)).unwrap();
        assert_eq!(&domain[28..], b"BT01");
        assert_eq!(
            domain_from_evidence(BOOT, OFFSETS, ns(11), ns(11)),
            Ok(domain)
        );
        let shifted = b"monotonic 0 0\nboottime -5 123\n";
        assert_ne!(
            domain_from_evidence(BOOT, shifted, ns(10), ns(10)).unwrap(),
            domain
        );
        let other_boot = b"94220b48-8eaf-49a1-87de-18b0392e1095\n";
        assert_ne!(
            domain_from_evidence(other_boot, OFFSETS, ns(10), ns(10)).unwrap(),
            domain
        );
    }

    #[test]
    fn children_namespace_offset_is_not_misattributed_to_the_current_clock() {
        assert_eq!(
            domain_from_evidence(BOOT, OFFSETS, ns(10), ns(11)),
            Err("boottime_namespace_mismatch")
        );
    }

    #[test]
    fn malformed_or_duplicate_evidence_cannot_establish_continuity() {
        for offset in [
            b"boottime 0 0\n".as_slice(),
            b"monotonic 0 0\nboottime 0 1000000000\n",
            b"monotonic 0 0\nboottime 0 0\nboottime 0 0\n",
            b"monotonic 0 0\nboottime 0 0 trailing\n",
            b"monotonic 0 0\nboottime 9223372036854775808 0\n",
        ] {
            assert!(domain_from_evidence(BOOT, offset, ns(10), ns(10)).is_err());
        }
        assert!(domain_from_evidence(b"not-a-boot-id", OFFSETS, ns(10), ns(10)).is_err());
    }

    #[test]
    fn injected_clock_changes_are_explicit_and_typed() {
        let clock = testing::FakeClock::new(reading(1, 5));
        assert_eq!(clock.read(), Ok(reading(1, 5)));
        clock.set(reading(1, 6));
        assert_eq!(clock.read(), Ok(reading(1, 6)));
        clock.fail(TimeError::ClockUnavailable);
        assert_eq!(clock.read(), Err(TimeError::ClockUnavailable));
    }

    #[test]
    fn system_readings_are_monotone_with_one_stable_process_domain() {
        let clock = SystemClock::new().unwrap();
        let first = clock.read().unwrap();
        let second = clock.read().unwrap();
        assert_eq!(first.domain, second.domain);
        assert!(second.tick >= first.tick);
        assert_eq!(
            clock.origin_is_recognized(),
            clock.origin_status() == "boottime_origin_recognized"
        );
    }
}
