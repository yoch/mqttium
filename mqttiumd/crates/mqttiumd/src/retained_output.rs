//! Retained replays already consumed at QoS 0 still owe their one send.
//! A full socket keeps that effect on its original generation. Its core
//! delivery row and payload remain charged until a write or a terminal refusal.

use std::collections::{BTreeMap, VecDeque};

use mqttiumd_contract::command::DeliveryCause;
use mqttiumd_contract::effects::{Effect, EffectSink, OwnedEffect, SubmitBlock, SubmitResult};
use mqttiumd_contract::ids::ConnectionGeneration;
use mqttiumd_contract::pubsub::WORK_PAGE_LIMIT;
use mqttiumd_core::owner::QOS0_CONSUMED_WINDOW;

#[derive(Default)]
pub(crate) struct RetainedOutput {
    lanes: BTreeMap<ConnectionGeneration, VecDeque<OwnedEffect>>,
    turns: VecDeque<ConnectionGeneration>,
}

impl RetainedOutput {
    pub(crate) fn is_empty(&self) -> bool {
        self.turns.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn pending(&self, generation: ConnectionGeneration) -> usize {
        self.lanes.get(&generation).map_or(0, VecDeque::len)
    }

    pub(crate) fn sink<'a, S: EffectSink>(&'a mut self, inner: &'a mut S) -> ReplaySink<'a, S> {
        ReplaySink { held: self, inner }
    }

    /// One attempt per generation, bounded for the owner turn. Newly freed
    /// sockets and closed generations both go through the normal adapter.
    pub(crate) fn retry(&mut self, sink: &mut impl EffectSink) -> usize {
        let mut accepted = 0;
        for _ in 0..self.turns.len().min(WORK_PAGE_LIMIT as usize) {
            let generation = self.turns.pop_front().expect("counted lane");
            let lane = self.lanes.get_mut(&generation).expect("scheduled lane");
            let effect = lane.pop_front().expect("nonempty lane");
            match sink.try_submit(effect) {
                SubmitResult::Accepted => accepted += 1,
                SubmitResult::NotSubmitted { effect, .. } => lane.push_front(effect),
            }
            if lane.is_empty() {
                self.lanes.remove(&generation);
            } else {
                self.turns.push_back(generation);
            }
        }
        accepted
    }
}

pub(crate) struct ReplaySink<'a, S> {
    held: &'a mut RetainedOutput,
    inner: &'a mut S,
}

impl<S: EffectSink> EffectSink for ReplaySink<'_, S> {
    fn try_submit(&mut self, effect: OwnedEffect) -> SubmitResult {
        let replay = matches!(
            effect.effect(),
            Effect::EmitQos0 {
                cause: DeliveryCause::RetainedReplay,
                ..
            }
        );
        if !replay && self.held.is_empty() {
            return self.inner.try_submit(effect);
        }
        let Some(generation) = effect.effect().wire_generation() else {
            return self.inner.try_submit(effect);
        };
        if let Some(lane) = self.held.lanes.get_mut(&generation) {
            if replay && lane.len() < QOS0_CONSUMED_WINDOW {
                lane.push_back(effect);
                return SubmitResult::Accepted;
            }
            // The outbox keeps the later wire effect in order on this generation.
            return SubmitResult::NotSubmitted {
                effect,
                reason: SubmitBlock::TransportBackpressure,
            };
        }
        match self.inner.try_submit(effect) {
            SubmitResult::NotSubmitted {
                effect,
                reason: SubmitBlock::TransportBackpressure,
            } if replay => {
                self.held.lanes.insert(generation, VecDeque::from([effect]));
                self.held.turns.push_back(generation);
                SubmitResult::Accepted
            }
            result => result,
        }
    }
}
