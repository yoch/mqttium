//! Preflight of the proof tooling (lot L6, deliverable 0).
//!
//! Two harnesses on the production dependency, `mqttiumd-core` as the
//! product builds it: one property that is true and one that is
//! deliberately false. `proofs/preflight.sh` runs both and requires the
//! first to be verified and the second to be refuted by the check named
//! here. A tool that accepts the false property, or that fails for another
//! reason, cannot be trusted with the proof.
//!
//! The harnesses reach the checked subtraction of a quantity through the
//! core's re-export, so the path really crosses the production crate.

#[cfg(kani)]
mod harnesses {
    use mqttiumd_core::contract::capacity::PayloadBytes;

    /// True: the subtraction succeeds exactly when it does not underflow,
    /// and then it is exact. Both operands range over all of `u64`.
    #[kani::proof]
    fn preflight_true_property() {
        let used: u64 = kani::any();
        let cost: u64 = kani::any();
        let difference = PayloadBytes::new(used).checked_sub(PayloadBytes::new(cost));
        match difference {
            Some(rest) => {
                assert!(cost <= used, "preflight-true: success implies no underflow");
                assert!(
                    rest.get() == used - cost,
                    "preflight-true: the difference is exact"
                );
            }
            None => assert!(cost > used, "preflight-true: refusal implies underflow"),
        }
        kani::cover!(difference.is_some(), "preflight-true: a success is reached");
        kani::cover!(difference.is_none(), "preflight-true: a refusal is reached");
    }

    /// Deliberately false: it claims the subtraction never refuses. The
    /// tool must refute it, with this very check.
    #[kani::proof]
    fn preflight_false_property() {
        let used: u64 = kani::any();
        let cost: u64 = kani::any();
        let difference = PayloadBytes::new(used).checked_sub(PayloadBytes::new(cost));
        assert!(
            difference.is_some(),
            "preflight-false: the subtraction never refuses"
        );
    }
}
