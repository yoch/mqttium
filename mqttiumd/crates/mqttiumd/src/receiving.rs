//! Receive Maximum accounting is scoped to a PUBLISH occurrence and its QoS,
//! not only its reusable packet identifier. A delayed terminal ACK receipt
//! cannot free a later exchange's slot, including a use with another QoS.

use mqttiumd_contract::packet::Qos;

pub const MAXIMUM: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Terminal {
    Puback,
    Pubcomp,
    RejectedPubrec,
}

impl Terminal {
    fn qos(self) -> Qos {
        match self {
            Self::Puback => Qos::AtLeastOnce,
            Self::Pubcomp | Self::RejectedPubrec => Qos::ExactlyOnce,
        }
    }
}

#[derive(Clone, Copy)]
struct Occurrence {
    serial: u64,
    qos: Qos,
}

const EMPTY_OCCURRENCE: Occurrence = Occurrence {
    serial: 0,
    qos: Qos::AtMostOnce,
};

/// Inline compact receive-window table (`MAXIMUM = 8`), inspired by Redis
/// `listpack`: zero heap allocation per connection and single-cache-line scan.
pub struct Window {
    entries: [(u16, Occurrence); MAXIMUM],
    len: u8,
}

impl Default for Window {
    fn default() -> Self {
        Self {
            entries: [(0, EMPTY_OCCURRENCE); MAXIMUM],
            len: 0,
        }
    }
}

impl Window {
    #[inline]
    fn find(&self, id: u16) -> Option<usize> {
        let len = usize::from(self.len);
        self.entries[..len].iter().position(|(key, _)| *key == id)
    }

    #[inline]
    fn remove_at(&mut self, idx: usize) {
        let last = usize::from(self.len) - 1;
        self.entries[idx] = self.entries[last];
        self.len = last as u8;
    }

    /// Isolated test mutation: deliberately discard the occurrence and QoS
    /// guards. This entry point is absent from a normal product build.
    #[cfg(feature = "test-hooks")]
    pub fn complete_current(&mut self, id: u16) -> bool {
        if let Some(idx) = self.find(id) {
            self.remove_at(idx);
            true
        } else {
            false
        }
    }

    /// Refreshing a reusable identifier does not consume another receive
    /// slot. Older accepted frames retain their own frozen occurrence serial.
    pub fn publish_qos(&mut self, id: u16, serial: u64, qos: Qos) -> Result<(), ()> {
        if qos == Qos::AtMostOnce {
            return Err(());
        }
        if let Some(idx) = self.find(id) {
            self.entries[idx].1 = Occurrence { serial, qos };
            return Ok(());
        }
        let len = usize::from(self.len);
        if len >= MAXIMUM {
            return Err(());
        }
        self.entries[len] = (id, Occurrence { serial, qos });
        self.len = (len + 1) as u8;
        Ok(())
    }

    /// Only a terminal ACK for this QoS and this exact PUBLISH occurrence
    /// releases its credit. The caller supplies a core-frozen serial, never
    /// one inferred later from the current use of the Packet Identifier.
    pub fn complete_frame(&mut self, id: u16, serial: Option<u64>, terminal: Terminal) -> bool {
        let Some(serial) = serial else {
            return false;
        };
        let Some(idx) = self.find(id) else {
            return false;
        };
        let occurrence = self.entries[idx].1;
        if occurrence.serial == serial && occurrence.qos == terminal.qos() {
            self.remove_at(idx);
            true
        } else {
            false
        }
    }

    pub fn can_complete_frame(&self, id: u16, serial: Option<u64>, terminal: Terminal) -> bool {
        serial.is_some_and(|serial| {
            self.find(id).is_some_and(|idx| {
                let occurrence = self.entries[idx].1;
                occurrence.serial == serial && occurrence.qos == terminal.qos()
            })
        })
    }

    pub fn contains(&self, id: u16) -> bool {
        self.find(id).is_some()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn serial(&self, id: u16) -> Option<u64> {
        self.find(id).map(|idx| self.entries[idx].1.serial)
    }
}

#[cfg(test)]
mod tests {
    use super::{Terminal, Window};
    use mqttiumd_contract::packet::Qos;

    #[test]
    fn a_terminal_frame_must_match_both_occurrence_and_qos() {
        let mut window = Window::default();
        assert_eq!(window.publish_qos(1, 10, Qos::AtLeastOnce), Ok(()));
        assert!(
            !window.complete_frame(1, Some(10), Terminal::Pubcomp),
            "PUBCOMP must not release the QoS 1 occurrence"
        );
        assert_eq!(window.serial(1), Some(10));
        assert!(!window.complete_frame(1, Some(10), Terminal::RejectedPubrec));
        assert!(window.complete_frame(1, Some(10), Terminal::Puback));
        assert!(!window.complete_frame(1, Some(10), Terminal::Puback));

        assert_eq!(window.publish_qos(1, 11, Qos::ExactlyOnce), Ok(()));
        assert!(
            !window.complete_frame(1, Some(11), Terminal::Puback),
            "PUBACK must not release the QoS 2 occurrence"
        );
        assert_eq!(window.serial(1), Some(11));
        assert!(window.complete_frame(1, Some(11), Terminal::Pubcomp));
    }

    #[test]
    fn delayed_terminal_receipts_do_not_release_a_new_mixed_qos_occurrence() {
        let mut window = Window::default();
        assert_eq!(window.publish_qos(1, 1, Qos::AtLeastOnce), Ok(()));
        assert_eq!(window.publish_qos(1, 2, Qos::ExactlyOnce), Ok(()));
        assert!(!window.complete_frame(1, Some(1), Terminal::Puback));
        assert_eq!(window.serial(1), Some(2));
        assert!(window.complete_frame(1, Some(2), Terminal::Pubcomp));
        assert_eq!(window.publish_qos(1, 3, Qos::AtLeastOnce), Ok(()));
        assert!(!window.complete_frame(1, Some(2), Terminal::Pubcomp));
        assert!(!window.complete_frame(1, None, Terminal::Puback));
        assert_eq!(window.serial(1), Some(3));
        assert!(window.complete_frame(1, Some(3), Terminal::Puback));
    }

    #[test]
    fn a_negative_pubrec_and_puback_reclaim_their_exact_receive_credit() {
        let mut window = Window::default();
        for id in 1..=8 {
            let qos = if id % 2 == 0 {
                Qos::AtLeastOnce
            } else {
                Qos::ExactlyOnce
            };
            assert_eq!(window.publish_qos(id, u64::from(id), qos), Ok(()));
        }
        assert_eq!(window.publish_qos(9, 9, Qos::AtLeastOnce), Err(()));
        assert!(window.complete_frame(1, Some(1), Terminal::RejectedPubrec));
        assert_eq!(window.publish_qos(9, 9, Qos::AtLeastOnce), Ok(()));
        assert_eq!(window.publish_qos(10, 10, Qos::ExactlyOnce), Err(()));
        assert!(window.complete_frame(2, Some(2), Terminal::Puback));
        assert_eq!(window.publish_qos(10, 10, Qos::ExactlyOnce), Ok(()));
        assert!(!window.complete_frame(1, Some(1), Terminal::RejectedPubrec));
        assert_eq!(window.publish_qos(11, 11, Qos::AtLeastOnce), Err(()));
        assert!(!window.complete_frame(9, Some(9), Terminal::RejectedPubrec));
        assert_eq!(window.serial(9), Some(9));
    }

    #[test]
    fn an_old_pubcomp_receipt_cannot_free_a_reused_identifier() {
        let mut window = Window::default();
        assert_eq!(window.publish_qos(1, 1, Qos::ExactlyOnce), Ok(()));
        let old_completion = window.serial(1);
        assert_eq!(window.publish_qos(1, 2, Qos::ExactlyOnce), Ok(()));
        window.complete_frame(1, old_completion, Terminal::Pubcomp);
        assert_eq!(
            window.serial(1),
            Some(2),
            "the new exchange still holds its slot"
        );
        for id in 2..=8 {
            assert_eq!(
                window.publish_qos(id, u64::from(id) + 1, Qos::ExactlyOnce),
                Ok(())
            );
        }
        assert_eq!(
            window.publish_qos(9, 10, Qos::ExactlyOnce),
            Err(()),
            "the ninth distinct exchange is refused"
        );
        assert_eq!(window.serial(9), None);
        window.complete_frame(1, Some(2), Terminal::Pubcomp);
        assert_eq!(window.serial(1), None);
        assert_eq!(
            window.publish_qos(9, 10, Qos::ExactlyOnce),
            Ok(()),
            "a matching receipt frees its slot"
        );
    }

    #[test]
    fn only_a_matching_present_serial_can_complete_an_exchange() {
        let mut window = Window::default();
        assert_eq!(window.publish_qos(7, 1, Qos::ExactlyOnce), Ok(()));
        window.complete_frame(7, None, Terminal::Pubcomp);
        assert_eq!(window.serial(7), Some(1));
        window.complete_frame(7, Some(2), Terminal::Pubcomp);
        assert_eq!(window.serial(7), Some(1));
        window.complete_frame(8, Some(1), Terminal::Pubcomp);
        assert_eq!(window.serial(7), Some(1));
        window.complete_frame(7, Some(1), Terminal::Pubcomp);
        assert_eq!(window.serial(7), None);
    }

    #[test]
    fn duplicate_refreshes_remain_bounded_and_need_their_latest_serial() {
        let mut window = Window::default();
        for id in 1..=8 {
            assert_eq!(
                window.publish_qos(id, u64::from(id), Qos::ExactlyOnce),
                Ok(())
            );
        }
        for serial in 9..=1024 {
            assert_eq!(window.publish_qos(1, serial, Qos::ExactlyOnce), Ok(()));
        }
        assert_eq!(window.publish_qos(9, 1025, Qos::ExactlyOnce), Err(()));
        window.complete_frame(1, Some(1), Terminal::Pubcomp);
        assert_eq!(window.serial(1), Some(1024));
        window.complete_frame(1, Some(1024), Terminal::Pubcomp);
        assert_eq!(window.publish_qos(9, 1025, Qos::ExactlyOnce), Ok(()));
    }
}
