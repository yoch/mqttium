//! Receive Maximum accounting is scoped to a PUBLISH occurrence and its QoS,
//! not only its reusable packet identifier. A delayed terminal ACK receipt
//! cannot free a later exchange's slot, including a use with another QoS.

use std::collections::BTreeMap;

use mqttiumd_contract::packet::Qos;

const MAXIMUM: usize = 8;

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

#[derive(Default)]
pub struct Window {
    receiving: BTreeMap<u16, Occurrence>,
}

impl Window {
    /// Isolated test mutation: deliberately discard the occurrence and QoS
    /// guards. This entry point is absent from a normal product build.
    #[cfg(feature = "test-hooks")]
    pub fn complete_current(&mut self, id: u16) -> bool {
        self.receiving.remove(&id).is_some()
    }
    /// Refreshing a reusable identifier does not consume another receive
    /// slot. Older accepted frames retain their own frozen occurrence serial.
    pub fn publish_qos(&mut self, id: u16, serial: u64, qos: Qos) -> Result<(), ()> {
        if qos == Qos::AtMostOnce
            || (!self.receiving.contains_key(&id) && self.receiving.len() >= MAXIMUM)
        {
            return Err(());
        }
        self.receiving.insert(id, Occurrence { serial, qos });
        Ok(())
    }

    /// Only a terminal ACK for this QoS and this exact PUBLISH occurrence
    /// releases its credit. The caller supplies a core-frozen serial, never
    /// one inferred later from the current use of the Packet Identifier.
    pub fn complete_frame(&mut self, id: u16, serial: Option<u64>, terminal: Terminal) -> bool {
        if let Some(serial) = serial
            && self.receiving.get(&id).is_some_and(|occurrence| {
                occurrence.serial == serial && occurrence.qos == terminal.qos()
            })
        {
            self.receiving.remove(&id);
            return true;
        }
        false
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn serial(&self, id: u16) -> Option<u64> {
        self.receiving.get(&id).map(|occurrence| occurrence.serial)
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
