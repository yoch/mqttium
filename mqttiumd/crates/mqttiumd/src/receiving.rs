//! Receive Maximum accounting is scoped to a PUBLISH occurrence, not only its
//! reusable packet identifier. A delayed PUBCOMP receipt cannot free a later
//! exchange's slot.

use std::collections::BTreeMap;

const MAXIMUM: usize = 8;

#[derive(Default)]
pub struct Window {
    receiving: BTreeMap<u16, u64>,
}

impl Window {
    /// A repeated identifier refreshes its serial without consuming another
    /// slot. The runtime supplies a checked, monotonically increasing serial.
    pub fn publish(&mut self, id: u16, serial: u64) -> Result<(), ()> {
        if !self.receiving.contains_key(&id) && self.receiving.len() >= MAXIMUM {
            return Err(());
        }
        self.receiving.insert(id, serial);
        Ok(())
    }

    pub fn serial(&self, id: u16) -> Option<u64> {
        self.receiving.get(&id).copied()
    }

    /// The runtime captures the serial when it accepts a PUBCOMP frame.
    pub fn complete(&mut self, id: u16, serial: Option<u64>) {
        if let Some(serial) = serial
            && self.receiving.get(&id).copied() == Some(serial)
        {
            self.receiving.remove(&id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Window;

    #[test]
    fn an_old_pubcomp_receipt_cannot_free_a_reused_identifier() {
        let mut window = Window::default();
        assert_eq!(window.publish(1, 1), Ok(()));
        let old_completion = window.serial(1);
        assert_eq!(window.publish(1, 2), Ok(()));
        window.complete(1, old_completion);
        assert_eq!(
            window.serial(1),
            Some(2),
            "the new exchange still holds its slot"
        );
        for id in 2..=8 {
            assert_eq!(window.publish(id, u64::from(id) + 1), Ok(()));
        }
        assert_eq!(
            window.publish(9, 10),
            Err(()),
            "the ninth distinct exchange is refused"
        );
        assert_eq!(window.serial(9), None);
        window.complete(1, Some(2));
        assert_eq!(window.serial(1), None);
        assert_eq!(
            window.publish(9, 10),
            Ok(()),
            "a matching receipt frees its slot"
        );
    }

    #[test]
    fn only_a_matching_present_serial_can_complete_an_exchange() {
        let mut window = Window::default();
        assert_eq!(window.publish(7, 1), Ok(()));
        window.complete(7, None);
        assert_eq!(window.serial(7), Some(1));
        window.complete(7, Some(2));
        assert_eq!(window.serial(7), Some(1));
        window.complete(8, Some(1));
        assert_eq!(window.serial(7), Some(1));
        window.complete(7, Some(1));
        assert_eq!(window.serial(7), None);
    }

    #[test]
    fn duplicate_refreshes_remain_bounded_and_need_their_latest_serial() {
        let mut window = Window::default();
        for id in 1..=8 {
            assert_eq!(window.publish(id, u64::from(id)), Ok(()));
        }
        for serial in 9..=1024 {
            assert_eq!(window.publish(1, serial), Ok(()));
        }
        assert_eq!(window.publish(9, 1025), Err(()));
        window.complete(1, Some(1));
        assert_eq!(window.serial(1), Some(1024));
        window.complete(1, Some(1024));
        assert_eq!(window.publish(9, 1025), Ok(()));
    }
}
