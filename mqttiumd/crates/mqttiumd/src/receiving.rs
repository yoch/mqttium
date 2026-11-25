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

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReceiveStep {
    Puback,
    Pubrec,
    RemotePubrel,
    ReleaseObserved,
}

impl ReceiveStep {
    fn qos(self) -> Qos {
        match self {
            Self::Puback => Qos::AtLeastOnce,
            Self::Pubrec | Self::RemotePubrel | Self::ReleaseObserved => Qos::ExactlyOnce,
        }
    }
}

/// Inline compact receive-window table (`MAXIMUM = 8`), laid out as a
/// structure of arrays so `ids` packs into 16 bytes (one SIMD register) and
/// the entire window fits in 96 bytes instead of 200 bytes.
pub struct Window {
    serials: [u64; MAXIMUM],
    ids: [u16; MAXIMUM],
    steps: [ReceiveStep; MAXIMUM],
    len: u8,
}

impl Default for Window {
    #[inline]
    fn default() -> Self {
        Self {
            serials: [0; MAXIMUM],
            ids: [0; MAXIMUM],
            steps: [ReceiveStep::Pubrec; MAXIMUM],
            len: 0,
        }
    }
}

impl Window {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    fn find(&self, id: u16) -> Option<usize> {
        let len = usize::from(self.len);
        self.ids[..len].iter().position(|&key| key == id)
    }

    #[inline]
    fn remove_at(&mut self, idx: usize) {
        let last = usize::from(self.len) - 1;
        self.serials[idx] = self.serials[last];
        self.ids[idx] = self.ids[last];
        self.steps[idx] = self.steps[last];
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
    #[inline]
    pub fn publish_qos(&mut self, id: u16, serial: u64, qos: Qos) -> Result<(), ()> {
        let next = match qos {
            Qos::AtMostOnce => return Err(()),
            Qos::AtLeastOnce => ReceiveStep::Puback,
            Qos::ExactlyOnce => ReceiveStep::Pubrec,
        };
        if let Some(idx) = self.find(id) {
            self.serials[idx] = serial;
            self.steps[idx] = next;
            return Ok(());
        }
        let len = usize::from(self.len);
        if len >= MAXIMUM {
            return Err(());
        }
        self.serials[len] = serial;
        self.ids[len] = id;
        self.steps[len] = next;
        self.len = (len + 1) as u8;
        Ok(())
    }

    /// The positive PUBREC for this occurrence was written. A PUBREL may
    /// already have been read before the transport published its receipt.
    pub fn pubrec_written(&mut self, id: u16, serial: Option<u64>) {
        if let Some(idx) = self.find(id)
            && Some(self.serials[idx]) == serial
            && self.steps[idx] == ReceiveStep::Pubrec
        {
            self.steps[idx] = ReceiveStep::RemotePubrel;
        }
    }

    pub fn pubrel_observed(&mut self, id: u16) {
        if let Some(idx) = self.find(id)
            && self.steps[idx].qos() == Qos::ExactlyOnce
        {
            self.steps[idx] = ReceiveStep::ReleaseObserved;
        }
    }

    pub fn all_wait_for_remote_pubrel(&self) -> bool {
        usize::from(self.len) == MAXIMUM
            && self
                .steps
                .iter()
                .all(|step| *step == ReceiveStep::RemotePubrel)
    }

    /// Only a terminal ACK for this QoS and this exact PUBLISH occurrence
    /// releases its credit. The caller supplies a core-frozen serial, never
    /// one inferred later from the current use of the Packet Identifier.
    #[inline]
    pub fn complete_frame(&mut self, id: u16, serial: Option<u64>, terminal: Terminal) -> bool {
        let Some(serial) = serial else {
            return false;
        };
        let Some(idx) = self.find(id) else {
            return false;
        };
        if self.serials[idx] == serial && self.steps[idx].qos() == terminal.qos() {
            self.remove_at(idx);
            true
        } else {
            false
        }
    }

    #[inline]
    pub fn can_complete_frame(&self, id: u16, serial: Option<u64>, terminal: Terminal) -> bool {
        serial.is_some_and(|serial| {
            self.find(id).is_some_and(|idx| {
                self.serials[idx] == serial && self.steps[idx].qos() == terminal.qos()
            })
        })
    }

    #[inline]
    pub fn contains(&self, id: u16) -> bool {
        self.find(id).is_some()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn serial(&self, id: u16) -> Option<u64> {
        self.find(id).map(|idx| self.serials[idx])
    }
}

#[cfg(test)]
mod tests {
    use super::{Terminal, Window};
    use mqttiumd_contract::packet::Qos;

    #[test]
    fn lookahead_requires_written_pubrec_for_every_live_occurrence() {
        let mut window = Window::default();
        for id in 1..=super::MAXIMUM as u16 {
            window
                .publish_qos(id, u64::from(id), Qos::ExactlyOnce)
                .unwrap();
            if id != 1 {
                window.pubrec_written(id, Some(u64::from(id)));
            }
        }
        assert!(
            !window.all_wait_for_remote_pubrel(),
            "one PUBREC still needs the writer"
        );
        window.pubrec_written(1, Some(1));
        assert!(window.all_wait_for_remote_pubrel());
        window.publish_qos(1, 100, Qos::ExactlyOnce).unwrap();
        window.pubrec_written(1, Some(1));
        assert!(
            !window.all_wait_for_remote_pubrel(),
            "stale write after a repeated PUBLISH"
        );
        window.pubrel_observed(1);
        window.pubrec_written(1, Some(100));
        assert!(
            !window.all_wait_for_remote_pubrel(),
            "PUBREL preceded the delayed write receipt"
        );
        assert!(window.complete_frame(1, Some(100), Terminal::Pubcomp));
        window.publish_qos(9, 101, Qos::AtLeastOnce).unwrap();
        assert!(
            !window.all_wait_for_remote_pubrel(),
            "PUBACK can free the QoS 1 slot locally"
        );
    }

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
