//! How the runtime carries out the effects of the owner: `Sink` hands
//! each one to the writer, a connection, a timer or a queue of inputs,
//! or refuses it with the reason the owner reads.

use super::*;

impl Sink<'_, '_> {
    fn push_terminal(&mut self, input: Input) {
        if let Some(id) = Runtime::terminal_effect(&input) {
            *self.terminal_effects.get_or_insert_default(id) += 1;
        }
        self.terminal_inputs.push_back(input);
    }

    fn writer_reply_room(&mut self) -> bool {
        let pending =
            self.inflight.len() + self.work_reads.len() + usize::from(self.will_reads.is_some());
        if self.inputs.len() + pending < WRITER_INPUTS {
            return true;
        }
        let queued = *self.queued_writer_replies.get_or_insert_with(|| {
            self.inputs
                .iter()
                .filter(|input| Runtime::writer_input(input))
                .count()
        });
        queued + pending < WRITER_INPUTS
    }
}
fn recipe_touches_routes(recipe: &Recipe) -> bool {
    match recipe {
        Recipe::AnnotatedPublication(value) => recipe_touches_routes(value.inner.as_ref()),
        Recipe::AdmitPublication(_)
        | Recipe::ClaimedPublication(_)
        | Recipe::MultiClaimedPublication(_)
        | Recipe::RetainedPublication(_)
        | Recipe::RetainedQos0(_)
        | Recipe::ExpiringPublication(_)
        | Recipe::UpdateSubscriptions(_)
        | Recipe::ReplaySubscriptions(_)
        | Recipe::TimedReplaySubscriptions(_) => true,
        _ => false,
    }
}

impl Sink<'_, '_> {
    fn submit_lifecycle(&mut self, effect: OwnedEffect) -> SubmitResult {
        if self.lifecycle_inflight.len() >= LIFECYCLE_IN_FLIGHT {
            return SubmitResult::NotSubmitted {
                effect,
                reason: SubmitBlock::InFlightLimit,
            };
        }
        let operation_id = match effect.effect() {
            Effect::Persist { command } => command.recipe.operation(),
            Effect::ResolveAndFence { operation } => *operation,
            _ => unreachable!("lifecycle effect classified by caller"),
        };
        let id = effect.id();
        let record_submitted = self.events.records("command_submitted");
        let details = record_submitted.then(|| match effect.effect() {
            Effect::Persist { command } => recipe_details(command.recipe.get()),
            _ => json!({"action":"Resolve"}),
        });
        match self
            .writer
            .try_send_control_quiet(ControlRequest::Lifecycle(effect))
        {
            Ok(()) => {
                self.wake_control = true;
                self.lifecycle_inflight.insert(id, operation_id);
                if let Some(details) = details
                    && self
                        .events
                        .emit(
                            "command_submitted",
                            json!({
                                "operation": operation(operation_id),
                                "command_kind": "SessionLifecycle",
                                "details": details,
                            }),
                        )
                        .is_err()
                {
                    self.error = Some(Failure::plain("events_unavailable"));
                }
                SubmitResult::Accepted
            }
            Err(TrySendError::Full(ControlRequest::Lifecycle(effect))) => {
                SubmitResult::NotSubmitted {
                    effect,
                    reason: SubmitBlock::QueueFull,
                }
            }
            Err(TrySendError::Disconnected(ControlRequest::Lifecycle(effect))) => {
                self.error = Some(Failure::plain("writer_control_disconnected"));
                SubmitResult::NotSubmitted {
                    effect,
                    reason: SubmitBlock::Closed,
                }
            }
            _ => unreachable!("exact control request variant"),
        }
    }
}
#[cfg(feature = "test-hooks")]
impl Sink<'_, '_> {
    pub(super) fn release_qos0(&mut self) {
        let Some(front) = self.emission_barriers.front_mut() else {
            return;
        };
        if !front.released {
            let Some(dir) = &self.hooks.directory else {
                self.error = Some(Failure::plain("hook_directory"));
                return;
            };
            let release = dir.join(format!("release-{}", front.point));
            if release.exists() {
                if std::fs::remove_file(release).is_err() {
                    self.error = Some(Failure::plain("hook_release"));
                    return;
                }
                front.released = true;
            } else if front.since.elapsed() > Duration::from_secs(30) {
                self.error = Some(Failure::plain("hook_deadline"));
                return;
            } else {
                return;
            }
        }
        let held = self
            .emission_barriers
            .pop_front()
            .expect("checked bounded queue");
        self.releasing_qos0 = true;
        let result = self.try_submit(held.effect);
        self.releasing_qos0 = false;
        if let SubmitResult::NotSubmitted { effect, .. } = result {
            self.emission_barriers.push_front(HeldEmission {
                point: held.point,
                effect,
                since: held.since,
                released: true,
            });
        }
    }
}
impl EffectSink for Sink<'_, '_> {
    fn try_submit(&mut self, effect: OwnedEffect) -> SubmitResult {
        let id = effect.id();
        let refused = |effect, reason| SubmitResult::NotSubmitted { effect, reason };
        if self.error.is_some() {
            return refused(effect, SubmitBlock::Closed);
        }
        // Applying a cache fact or shutting down a transport belongs between
        // effects: a later Persist/CONNACK must not race this dispatch tail.
        if self.session_change.is_some() || !self.close.is_empty() {
            return refused(effect, SubmitBlock::QueueFull);
        }
        if !matches!(effect.effect(), Effect::ArmDeadline { .. }) {
            self.direct_skip_next_deadline = false;
        }
        match effect.effect() {
            Effect::SessionChanged {
                operation,
                connection,
                client_id,
                change,
            } => {
                self.session_change = Some(AppliedSessionChange {
                    operation: *operation,
                    connection: *connection,
                    client_id: client_id.clone(),
                    change: *change,
                });
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::SessionRefused { connection, reason } => {
                let generation = *connection;
                let reason = *reason;
                if let Some(peer) = self.peers.get_mut(&generation) {
                    let bytes = match codec::session_refusal_for(
                        peer.version.unwrap_or_default(),
                        reason,
                    ) {
                        Ok(bytes) => bytes,
                        Err(_) => {
                            self.error = Some(Failure::plain("invalid_session_refusal"));
                            return refused(effect, SubmitBlock::Closed);
                        }
                    };
                    if bytes.len() <= peer.max_packet_bytes()
                        && peer
                            .io
                            .try_send_outgoing(net::Write {
                                expiry: None,
                                correlation: None,
                                bytes: net::FrameBytes::from(bytes),
                                payload: None,
                                report: net::WriteReport::Complete,
                            })
                            .is_ok()
                    {
                        peer.set_closing(("session_refused", Instant::now()));
                        self.mark_pending_peers.push(generation);
                    } else {
                        self.close.push((generation, "session_refused"));
                    }
                }
                if self.events.emit("session_refused", json!({
                    "connection": generation.get().to_string(),
                    "details": {"reason_code": reason, "code": "lifecycle_rejected_certain"},
                })).is_err() {
                    self.error = Some(Failure::plain("events_unavailable"));
                }
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::Qos0Dropped {
                origin,
                target,
                reason,
            } => {
                let (origin, target, reason) = (*origin, *target, *reason);
                if self
                    .events
                    .emit_with("qos0_dropped", || {
                        json!({
                            "details": {
                                "origin": qos0_origin(origin),
                                "target": target.map(|t|t.get().to_string()).unwrap_or_default(),
                                "reason": reason.code(),
                            },
                        })
                    })
                    .is_err()
                {
                    self.error = Some(Failure::plain("events_unavailable"));
                }
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::DeliveryDiscarded {
                delivery,
                target,
                operation: finished,
                reason,
            } => {
                let (delivery, target, finished, reason) = (*delivery, *target, *finished, *reason);
                if self
                    .events
                    .emit_with("delivery_discarded", || {
                        json!({
                            "operation": operation(finished),
                            "command_kind": "Finish",
                            "details": {
                                "delivery": delivery_text(delivery),
                                "target": target.get().to_string(),
                                "reason": reason.code(),
                            },
                        })
                    })
                    .is_err()
                {
                    self.error = Some(Failure::plain("events_unavailable"));
                }
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::EmitQos0 {
                expiry,
                properties,
                connection,
                origin,
                cause,
                retain,
                topic,
                payload,
            } => {
                let (connection, origin) = (*connection, *origin);
                if self
                    .terminal_inputs
                    .len()
                    .checked_add(2 * self.frames.len())
                    .is_none_or(|used| used >= self.limits.terminal_inputs)
                    || self.frames.len() >= self.limits.frames
                {
                    return refused(effect, SubmitBlock::QueueFull);
                }
                #[cfg(feature = "test-hooks")]
                if let Qos0Origin::Consumed {
                    operation: id_op, ..
                } = origin
                    && self.hooks.pause.iter().any(|p| p == "consume.emission")
                    && !self.releasing_qos0
                {
                    if self.emission_barriers.len() >= IN_FLIGHT {
                        return refused(effect, SubmitBlock::QueueFull);
                    }
                    if self
                        .events
                        .emit(
                            "barrier_hit",
                            json!({
                                "point": "consume.emission",
                                "command_kind": "Consume",
                                "operation": operation(id_op),
                            }),
                        )
                        .is_err()
                    {
                        self.error = Some(Failure::plain("events_unavailable"));
                        return refused(effect, SubmitBlock::Closed);
                    }
                    self.emission_barriers.push_back(HeldEmission {
                        point: "consume.emission",
                        effect,
                        since: Instant::now(),
                        released: false,
                    });
                    return SubmitResult::Accepted;
                }
                let generation = connection.generation;
                let result = if let Some(peer) = self.peers.get_mut(&generation).filter(|p| {
                    p.closing().is_none()
                        && self.bindings.get(&connection.session) == Some(&generation)
                }) {
                    let version = peer.version.unwrap_or_default();
                    let expiry_val = expiry.as_deref().copied();
                    match codec::encode_qos0_segments(
                        version,
                        topic,
                        payload,
                        *retain
                            && (version == Version::V5 || *cause == DeliveryCause::RetainedReplay),
                        expiry_val.map(|value| value.interval),
                        properties.as_deref(),
                    ) {
                        Ok(encoded) if encoded.total_len <= peer.max_packet_bytes() => {
                            let write_expiry = expiry_val.map(|expiry| {
                                Box::new(net::WriteExpiry {
                                    expiry,
                                    value_offset: encoded.expiry_value_offset,
                                    started: false,
                                })
                            });
                            #[cfg(feature = "test-hooks")]
                            let duplicate = if MUTANT_REPLAY_QOS0
                                && matches!(origin, Qos0Origin::Consumed { .. })
                            {
                                Some((encoded.header.clone(), encoded.payload.clone()))
                            } else {
                                None
                            };
                            let result = peer.io.try_send_outgoing(net::Write {
                                expiry: write_expiry,
                                correlation: Some((id, connection)),
                                bytes: encoded.header,
                                payload: encoded.payload,
                                report: net::WriteReport::Complete,
                            });
                            #[cfg(feature = "test-hooks")]
                            if result.is_ok()
                                && let Some((bytes, payload)) = duplicate
                            {
                                let _ = peer.io.try_send_outgoing(net::Write {
                                    expiry: None,
                                    correlation: None,
                                    bytes,
                                    payload,
                                    report: net::WriteReport::None,
                                });
                            }
                            match result {
                                Ok(()) => Ok(()),
                                Err(TrySendError::Full(_))
                                    if *cause == DeliveryCause::RetainedReplay =>
                                {
                                    return refused(effect, SubmitBlock::TransportBackpressure);
                                }
                                Err(TrySendError::Full(_)) => Err(Qos0DropReason::OutputSaturated),
                                Err(TrySendError::Disconnected(_)) => {
                                    Err(Qos0DropReason::ConnectionClosed)
                                }
                            }
                        }
                        _ => Err(Qos0DropReason::PacketTooLarge),
                    }
                } else {
                    Err(Qos0DropReason::ConnectionClosed)
                };
                match result {
                    Ok(()) => {
                        self.frames.insert(
                            id,
                            PendingFrame {
                                connection,
                                kind: FrameKind::Publish0,
                                packet_id: None,
                                receive_serial: None,
                                terminal: None,
                                wire_released: false,
                                was_retained: false,
                            },
                        );
                        if self
                            .events
                            .emit_with("frame_queued", || {
                                json!({
                                    "connection": generation.get().to_string(),
                                    "details": {
                                        "packet_kind": "PUBLISH",
                                        "qos": 0,
                                        "origin": qos0_origin(origin),
                                        "effect": effect_text(id),
                                    },
                                })
                            })
                            .is_err()
                        {
                            self.error = Some(Failure::plain("events_unavailable"));
                        }
                    }
                    Err(reason) => self.push_terminal(Input::Qos0EmitRefused {
                        effect: id,
                        connection,
                        reason,
                    }),
                }
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::Persist { command }
                if matches!(
                    command.recipe.get(),
                    Recipe::SessionLifecycle(_) | Recipe::WillPublication(_)
                ) =>
            {
                let conflicts_with_route_ops = match command.recipe.get() {
                    Recipe::WillPublication(_) => true,
                    Recipe::SessionLifecycle(lifecycle) => matches!(
                        &lifecycle.action,
                        SessionAction::Close { .. }
                            | SessionAction::EndWill {
                                expiry: SessionExpiry::OnDisconnect,
                                ..
                            }
                            | SessionAction::Expire { .. }
                            | SessionAction::Open {
                                expected: Some(_),
                                clean_start: true,
                                ..
                            }
                            | SessionAction::OpenAt {
                                expected: Some(_),
                                clean_start: true,
                                ..
                            }
                            | SessionAction::OpenWillAt {
                                expected: Some(_),
                                clean_start: true,
                                ..
                            }
                    ),
                    _ => false,
                };
                if conflicts_with_route_ops && !self.inflight_route_ops.is_empty() {
                    return refused(effect, SubmitBlock::QueueFull);
                }
                if let Recipe::SessionLifecycle(lifecycle) = command.recipe.get()
                    && let SessionAction::Open {
                        expected: Some(guard),
                        ..
                    }
                    | SessionAction::OpenAt {
                        expected: Some(guard),
                        ..
                    }
                    | SessionAction::OpenWillAt {
                        expected: Some(guard),
                        ..
                    } = &lifecycle.action
                    && self
                        .peers
                        .values()
                        .any(|p| p.session == Some(guard.session) && p.closing().is_some())
                {
                    return refused(effect, SubmitBlock::QueueFull);
                }
                self.submit_lifecycle(effect)
            }
            Effect::Persist { command } => {
                if self.inflight.len() >= self.limits.in_flight || !self.writer_reply_room() {
                    return refused(effect, SubmitBlock::InFlightLimit);
                }
                let operation_id = command.recipe.operation();
                let record_submitted = self.events.records("command_submitted");
                let submitted_meta = record_submitted.then(|| {
                    (
                        crate::writer::recipe_kind(command.recipe.get()),
                        recipe_details(command.recipe.get()),
                    )
                });
                #[cfg(feature = "test-hooks")]
                if MUTANT_EARLY_QOS0
                    && let Recipe::ConsumeDelivery(c) = command.recipe.get()
                    && let Some(item) = self.work_cache.get(&c.delivery)
                    && let Some(g) = self.bindings.get(&c.target)
                    && let Some(peer) = self.peers.get(g)
                    && let Ok(bytes) = codec::encode_qos0(&item.topic, &item.payload)
                {
                    let _ = peer.io.try_send_outgoing(net::Write {
                        expiry: None,
                        correlation: None,
                        bytes: net::FrameBytes::from(bytes),
                        payload: None,
                        report: net::WriteReport::Complete,
                    });
                }
                #[cfg(feature = "test-hooks")]
                let early_finish = if MUTANT_EARLY_FINISH
                    && let Recipe::FinishDelivery(f) = command.recipe.get()
                {
                    Some(Input::StoreResult {
                        effect: id,
                        operation: operation_id,
                        reply: StoreReply::Outcome(StoreOutcome::Committed(
                            CommitFacts::DeliveryFinished {
                                delivery: f.delivery,
                                target: f.target,
                                packet_id: f.packet_id,
                                claim: f.claim,
                            },
                        )),
                    })
                } else {
                    None
                };
                #[cfg(feature = "test-hooks")]
                if MUTANT_EARLY_OUTGOING
                    && let Recipe::ClaimDelivery(c) = command.recipe.get()
                    && let Some(item) = self.work_cache.get(&c.delivery)
                    && let Some(generation) = self.bindings.get(&c.target)
                    && let Some(peer) = self.peers.get(generation)
                    && let Ok(bytes) =
                        codec::encode_publish(c.packet_id, &item.topic, &item.payload, false)
                {
                    let _ = peer.io.try_send_outgoing(net::Write {
                        expiry: None,
                        correlation: None,
                        bytes: net::FrameBytes::from(bytes),
                        payload: None,
                        report: net::WriteReport::Complete,
                    });
                }
                #[cfg(feature = "test-hooks")]
                if MUTANT_EARLY_ACK
                    && let mqttiumd_contract::command::Recipe::AdmitPublication(admission) =
                        command.recipe.get()
                    && admission.qos == mqttiumd_contract::packet::Qos::ExactlyOnce
                    && let Some(p) = self.peers.values().find(|p| {
                        p.receiving_serial(admission.source.packet_id.get())
                            .is_some()
                    })
                {
                    let _ = p.io.try_send_outgoing(net::Write {
                        expiry: None,
                        correlation: None,
                        bytes: net::FrameBytes::from(
                            codec::encode(OutboundFrame::Pubrec {
                                packet_id: admission.source.packet_id,
                                reason: mqttiumd_contract::packet::AckReason::Success,
                            })
                            .unwrap_or_default(),
                        ),
                        payload: None,
                        report: net::WriteReport::Complete,
                    });
                }
                #[cfg(feature = "test-hooks")]
                if MUTANT_EARLY_PUBACK
                    && let mqttiumd_contract::command::Recipe::AdmitPublication(admission) =
                        command.recipe.get()
                    && admission.qos == mqttiumd_contract::packet::Qos::AtLeastOnce
                    && let Some(p) = self.peers.values().find(|p| {
                        p.receiving_serial(admission.source.packet_id.get())
                            .is_some()
                    })
                {
                    let _ = p.io.try_send_outgoing(net::Write {
                        expiry: None,
                        correlation: None,
                        bytes: net::FrameBytes::from(
                            codec::encode(OutboundFrame::Puback {
                                packet_id: admission.source.packet_id,
                                reason: mqttiumd_contract::packet::PubackReason::Success,
                            })
                            .unwrap_or_default(),
                        ),
                        payload: None,
                        report: net::WriteReport::Complete,
                    });
                }
                if self.writer.is_direct() {
                    let (id, Effect::Persist { command }) = effect.into_parts() else {
                        unreachable!("matched Effect::Persist");
                    };
                    let reply = self
                        .writer
                        .apply_persist_direct(command, self.pending_work_targets);
                    self.inputs.push_back(Input::StoreResult {
                        effect: id,
                        operation: operation_id,
                        reply,
                    });
                    if let Some(count) = self.queued_writer_replies.as_mut() {
                        *count += 1;
                    }
                    self.direct_skip_next_deadline = matches!(
                        reply,
                        StoreReply::Outcome(
                            StoreOutcome::Committed(_) | StoreOutcome::RejectedCertain(_)
                        )
                    );
                    if let Some((command_kind, details)) = submitted_meta
                        && self
                            .events
                            .emit(
                                "command_submitted",
                                json!({
                                    "operation": operation(operation_id),
                                    "command_kind": command_kind,
                                    "details": details,
                                }),
                            )
                            .is_err()
                    {
                        self.error = Some(Failure::plain("events_unavailable"));
                    }
                    return SubmitResult::Accepted;
                }
                let touches_routes = recipe_touches_routes(command.recipe.get());
                match self.writer.try_send(Request::Effect(effect)) {
                    Ok(()) => {
                        self.inflight.insert(id, operation_id);
                        if touches_routes {
                            self.inflight_route_ops.insert(id, ());
                        }
                        #[cfg(feature = "test-hooks")]
                        if let Some(input) = early_finish {
                            self.inputs.push_back(input);
                            if let Some(count) = self.queued_writer_replies.as_mut() {
                                *count += 1;
                            }
                        }
                        if let Some((command_kind, details)) = submitted_meta
                            && self
                                .events
                                .emit(
                                    "command_submitted",
                                    json!({
                                        "operation": operation(operation_id),
                                        "command_kind": command_kind,
                                        "details": details,
                                    }),
                                )
                                .is_err()
                        {
                            self.error = Some(Failure::plain("events_unavailable"));
                        }
                        SubmitResult::Accepted
                    }
                    Err(TrySendError::Full(Request::Effect(effect))) => {
                        refused(effect, SubmitBlock::QueueFull)
                    }
                    Err(TrySendError::Disconnected(Request::Effect(effect))) => {
                        self.error = Some(Failure::plain("writer_disconnected"));
                        refused(effect, SubmitBlock::Closed)
                    }
                    _ => unreachable!(),
                }
            }
            Effect::ResolveAndFence { operation }
                if self.lifecycle_ops.contains(operation)
                    || self.will_operation == Some(*operation) =>
            {
                self.submit_lifecycle(effect)
            }
            Effect::ResolveAndFence { operation } => {
                if self.inflight.len() >= self.limits.in_flight || !self.writer_reply_room() {
                    return refused(effect, SubmitBlock::InFlightLimit);
                }
                let op = *operation;
                match self.writer.try_send(Request::Effect(effect)) {
                    Ok(()) => {
                        self.inflight.insert(id, op);
                        SubmitResult::Accepted
                    }
                    Err(TrySendError::Full(Request::Effect(e))) => {
                        refused(e, SubmitBlock::QueueFull)
                    }
                    Err(TrySendError::Disconnected(Request::Effect(e))) => {
                        refused(e, SubmitBlock::Closed)
                    }
                    _ => unreachable!(),
                }
            }
            Effect::EmitFrame {
                connection,
                frame,
                publish_order,
                pubrel,
            } => {
                let (connection, frame) = (*connection, *frame);
                let receive_serial = *publish_order;
                let binding = match (*pubrel, frame) {
                    (Some((delivery, claim)), OutboundFrame::Pubrel { packet_id }) => {
                        let key = (connection, packet_id);
                        if self.outgoing_bindings.len() >= self.limits.outgoing_bindings {
                            return refused(effect, SubmitBlock::InFlightLimit);
                        }
                        if self.outgoing_bindings.contains_key(&key) {
                            self.error = Some(Failure::plain("pubrel_binding_conflict"));
                            return refused(effect, SubmitBlock::InFlightLimit);
                        }
                        Some((
                            key,
                            PubackTarget {
                                delivery,
                                claim,
                                frame: id,
                            },
                        ))
                    }
                    (None, _) => None,
                    (Some(_), _) => unreachable!("only PUBREL binds an outgoing occurrence here"),
                };
                let terminal = match frame {
                    OutboundFrame::Puback { .. } => Some(Terminal::Puback),
                    OutboundFrame::Pubcomp { .. } => Some(Terminal::Pubcomp),
                    OutboundFrame::Pubrec { reason, .. }
                        if reason != mqttiumd_contract::packet::AckReason::Success =>
                    {
                        Some(Terminal::RejectedPubrec)
                    }
                    _ => None,
                };
                let generation = connection.generation;
                let (kind, packet_id) = frame_info(frame);
                if self
                    .terminal_inputs
                    .len()
                    .checked_add(2 * self.frames.len())
                    .is_none_or(|used| used >= self.limits.terminal_inputs)
                    || self.frames.len() >= self.limits.frames
                    || (!self.shards_active
                        && kind == FrameKind::Connack
                        && ordinary_room(
                            self.inputs,
                            self.ordinary_reserved,
                            self.limits.ordinary_inputs,
                        ) == 0)
                {
                    return refused(effect, SubmitBlock::QueueFull);
                }
                let Some(peer) = self.peers.get_mut(&generation).filter(|peer| {
                    peer.closing().is_none()
                        && self.bindings.get(&connection.session) == Some(&generation)
                }) else {
                    let _ = effect.into_parts();
                    if self
                        .events
                        .emit(
                            "frame_failed",
                            json!({
                                "connection": generation.get().to_string(),
                                "details": {
                                    "effect": effect_text(id),
                                    "packet_kind": kind.as_str(),
                                    "code": "unusable_generation",
                                },
                            }),
                        )
                        .is_err()
                    {
                        self.error = Some(Failure::plain("events_unavailable"));
                    }
                    self.push_terminal(Input::FrameResult {
                        effect: id,
                        connection,
                        progress: FrameProgress::Failed,
                    });
                    return SubmitResult::Accepted;
                };
                // The answer of one unit request of a larger SUBSCRIBE or
                // UNSUBSCRIBE: it is kept for the one acknowledgement of
                // the whole request and reported to the core as written.
                let part_of = matches!(frame, OutboundFrame::Unsuback { .. });
                if let Some(answered) = packet_id
                    && matches!(kind, FrameKind::Suback | FrameKind::Unsuback)
                    && let Some(requests) = peer.cold.as_mut().and_then(|c| c.requests.as_mut())
                    && requests.awaiting
                    && requests.packet_id.get() == answered
                    && requests.unsubscribe == part_of
                {
                    let version = peer.version.unwrap_or_default();
                    let Ok(bytes) = codec::encode_for(version, frame) else {
                        self.close.push((generation, "failure_without_wire_ack"));
                        return refused(effect, SubmitBlock::Closed);
                    };
                    requests
                        .served
                        .extend_from_slice(codec::filter_ack_codes(version, &bytes));
                    requests.awaiting = false;
                    let _ = effect.into_parts();
                    self.push_terminal(Input::FrameResult {
                        effect: id,
                        connection,
                        progress: FrameProgress::Written {
                            confirmed: bytes.len() as u64,
                            total: bytes.len() as u64,
                        },
                    });
                    return SubmitResult::Accepted;
                }
                let bytes = match codec::encode_frame_bytes(peer.version.unwrap_or_default(), frame)
                {
                    Ok(bytes) if bytes.len() <= peer.max_packet_bytes() => bytes,
                    Err(error) if error.code() == "AckNotEncodableInMqtt311" => {
                        let code = if matches!(
                            frame,
                            OutboundFrame::Puback {
                                reason: mqttiumd_contract::packet::PubackReason::QuotaExceeded,
                                ..
                            } | OutboundFrame::Pubrec {
                                reason: mqttiumd_contract::packet::AckReason::QuotaExceeded,
                                ..
                            }
                        ) {
                            "capacity_exceeded"
                        } else {
                            "failure_without_wire_ack"
                        };
                        self.close.push((generation, code));
                        return refused(effect, SubmitBlock::Closed);
                    }
                    _ => {
                        self.close.push((generation, "outgoing_queue_full"));
                        return refused(effect, SubmitBlock::Closed);
                    }
                };
                let report = if cfg!(any(test, feature = "test-hooks")) && terminal.is_some() {
                    net::WriteReport::CompleteAndWireProgress
                } else {
                    net::WriteReport::Complete
                };
                let write = net::Write {
                    expiry: None,
                    correlation: Some((id, connection)),
                    bytes,
                    payload: None,
                    report,
                };
                let sent = if self.shards_active && kind == FrameKind::Suback {
                    peer.io.try_send_suback(write)
                } else {
                    peer.io.try_send_outgoing(write)
                };
                match sent {
                    Ok(()) => {
                        if let Some((key, target)) = binding {
                            self.outgoing_bindings.insert(key, target);
                        }
                        if kind == FrameKind::Connack {
                            // Pipeline processing starts only after the core's
                            // CONNACK occupies the ordered socket output queue.
                            peer.pending = false;
                            self.mark_pending_peers.push(generation);
                            // Readiness is effective only after the ordered
                            // CONNACK enters the bounded writer queue. The
                            // earlier cache-install fence deliberately kept
                            // this destination ineligible for discovery.
                            if self.shards_active {
                                self.shard_attach_ready.push(connection);
                            } else if self.queued_work_sessions.insert(connection.session) {
                                self.inputs.push_back(Input::WorkAvailable {
                                    session: connection.session,
                                });
                            }
                        } else if self.shards_active && kind == FrameKind::Suback {
                            self.shard_suback_ready.push(connection.session);
                        }
                        self.frames.insert(
                            id,
                            PendingFrame {
                                connection,
                                kind,
                                packet_id,
                                receive_serial,
                                terminal,
                                wire_released: false,
                                was_retained: peer.submitted_output(id),
                            },
                        );
                        let _ = effect.into_parts();
                        if kind != FrameKind::Pingresp
                            && self
                                .events
                                .emit_with("frame_queued", || {
                                    json!({
                                        "connection": generation.get().to_string(),
                                        "details": {
                                            "packet_kind": kind.as_str(),
                                            "effect": effect_text(id),
                                            "packet_id": packet_id.unwrap_or(0),
                                        },
                                    })
                                })
                                .is_err()
                        {
                            self.error = Some(Failure::plain("events_unavailable"));
                        }
                        SubmitResult::Accepted
                    }
                    Err(TrySendError::Full(_)) => {
                        self.mark_pending_peers.push(generation);
                        if peer.cold_mut().blocked_output.replace(id) != Some(id)
                            && self
                                .events
                                .emit_with("effect_retained", || {
                                    json!({
                                        "connection": generation.get().to_string(),
                                        "details": {
                                            "code": "output_queue_full",
                                            "effect": effect_text(id),
                                            "packet_kind": kind.as_str(),
                                        },
                                    })
                                })
                                .is_err()
                        {
                            self.error = Some(Failure::plain("events_unavailable"));
                        }
                        refused(effect, SubmitBlock::TransportBackpressure)
                    }
                    Err(TrySendError::Disconnected(_)) => {
                        self.close.push((generation, "outgoing_queue_full"));
                        refused(effect, SubmitBlock::Closed)
                    }
                }
            }
            Effect::CloseConnection { connection, reason } => {
                let code = match reason {
                    CloseReason::ProtocolError => "protocol_error",
                    CloseReason::SessionTakenOver => "session_taken_over",
                    CloseReason::OutcomeUnknown => "outcome_unknown",
                    CloseReason::CapacityExceeded => "capacity_exceeded",
                };
                let mut flushing = false;
                // An MQTT 5 client is told why: its session was taken over,
                // or a limit of the broker was exceeded (Quota exceeded).
                let notice = match reason {
                    CloseReason::SessionTakenOver => Some(0x8e),
                    CloseReason::CapacityExceeded => Some(0x97),
                    CloseReason::ProtocolError | CloseReason::OutcomeUnknown => None,
                };
                if let Some(notice) = notice
                    && let Some(peer) = self.peers.get_mut(connection)
                {
                    if peer.closing().is_some() {
                        flushing = true;
                    } else if peer.version != Some(Version::V311) {
                        let bytes = codec::disconnect_reason(notice);
                        if bytes.len() <= peer.max_packet_bytes()
                            && peer
                                .io
                                .try_send_outgoing(net::Write {
                                    expiry: None,
                                    correlation: None,
                                    bytes: net::FrameBytes::from(bytes),
                                    payload: None,
                                    report: net::WriteReport::Complete,
                                })
                                .is_ok()
                        {
                            peer.set_closing((code, Instant::now()));
                            self.mark_pending_peers.push(*connection);
                            flushing = true;
                        }
                    }
                }
                if !flushing {
                    self.close.push((*connection, code));
                }
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::CollectReceipts { collection } => {
                let collection = *collection;
                match self.writer.try_send_control(ControlRequest::Collect {
                    effect: id,
                    collection,
                }) {
                    Ok(()) => {
                        let mut details = crate::writer::collection_details(collection);
                        details["effect"] = json!(effect_text(id));
                        if self
                            .events
                            .emit(
                                "receipt_collection_submitted",
                                json!({"command_kind":"ReceiptCollection","details":details}),
                            )
                            .is_err()
                        {
                            self.error = Some(Failure::plain("events_unavailable"));
                        }
                        let _ = effect.into_parts();
                        SubmitResult::Accepted
                    }
                    Err(TrySendError::Full(_)) => refused(effect, SubmitBlock::QueueFull),
                    Err(TrySendError::Disconnected(_)) => {
                        self.error = Some(Failure::plain("writer_control_disconnected"));
                        refused(effect, SubmitBlock::Closed)
                    }
                }
            }
            Effect::ArmDeadline { token, after } => {
                if std::mem::take(&mut self.direct_skip_next_deadline) {
                    let _ = effect.into_parts();
                    return SubmitResult::Accepted;
                }
                if self.timers.len() >= self.limits.timers {
                    return refused(effect, SubmitBlock::QueueFull);
                }
                let Some(due) = Instant::now().checked_add(Duration::from_millis(after.get()))
                else {
                    self.error = Some(Failure::plain("timer_overflow"));
                    return refused(effect, SubmitBlock::Closed);
                };
                self.timers.insert(*token, due);
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::Continue { continuation } => {
                if ordinary_room(
                    self.inputs,
                    self.ordinary_reserved,
                    self.limits.ordinary_inputs,
                ) == 0
                {
                    return refused(effect, SubmitBlock::QueueFull);
                }
                self.inputs.push_front(Input::Continue {
                    continuation: *continuation,
                    budget: WorkUnits::new(65536),
                });
                let _ = effect.into_parts();
                SubmitResult::Accepted
            }
            Effect::LoadWills { .. } => {
                if self.will_reads.is_some() || !self.writer_reply_room() {
                    return refused(effect, SubmitBlock::InFlightLimit);
                }
                match self.writer.try_send(Request::Effect(effect)) {
                    Ok(()) => {
                        *self.will_reads = Some(id);
                        SubmitResult::Accepted
                    }
                    Err(TrySendError::Full(Request::Effect(effect))) => {
                        refused(effect, SubmitBlock::QueueFull)
                    }
                    Err(TrySendError::Disconnected(Request::Effect(effect))) => {
                        refused(effect, SubmitBlock::Closed)
                    }
                    _ => unreachable!(),
                }
            }
            Effect::LoadWork { session, .. } => {
                if self.work_reads.len() >= self.limits.in_flight || !self.writer_reply_room() {
                    return refused(effect, SubmitBlock::InFlightLimit);
                }
                let session = *session;
                match self.writer.try_send(Request::Effect(effect)) {
                    Ok(()) => {
                        self.work_reads.insert(id, session);
                        SubmitResult::Accepted
                    }
                    Err(TrySendError::Full(Request::Effect(e))) => {
                        refused(e, SubmitBlock::QueueFull)
                    }
                    Err(TrySendError::Disconnected(Request::Effect(e))) => {
                        self.error = Some(Failure::plain("writer_disconnected"));
                        refused(e, SubmitBlock::Closed)
                    }
                    _ => unreachable!(),
                }
            }
            Effect::EmitPublish {
                expiry,
                properties,
                started,
                connection,
                delivery,
                cause,
                qos,
                packet_id,
                claim,
                topic,
                payload,
                dup,
                retain,
            } => {
                let (connection, delivery, packet_id, claim, dup, retain) =
                    (*connection, *delivery, *packet_id, *claim, *dup, *retain);
                #[cfg(feature = "test-hooks")]
                if self
                    .hooks
                    .pause
                    .iter()
                    .any(|point| point == "publish.emission")
                    && !self.releasing_qos0
                {
                    if self.emission_barriers.len() >= IN_FLIGHT {
                        return refused(effect, SubmitBlock::QueueFull);
                    }
                    if self
                        .events
                        .emit(
                            "barrier_hit",
                            json!({
                                "point": "publish.emission",
                                "command_kind": "Claim",
                                "operation": operation(claim),
                                "details": {"delivery": delivery_text(delivery)},
                            }),
                        )
                        .is_err()
                    {
                        self.error = Some(Failure::plain("events_unavailable"));
                        return refused(effect, SubmitBlock::Closed);
                    }
                    self.emission_barriers.push_back(HeldEmission {
                        point: "publish.emission",
                        effect,
                        since: Instant::now(),
                        released: false,
                    });
                    return SubmitResult::Accepted;
                }
                if self
                    .terminal_inputs
                    .len()
                    .checked_add(2 * self.frames.len())
                    .is_none_or(|used| used >= self.limits.terminal_inputs)
                    || self.frames.len() >= self.limits.frames
                {
                    return refused(effect, SubmitBlock::QueueFull);
                }
                let generation = connection.generation;
                let Some(peer) = self.peers.get_mut(&generation).filter(|p| {
                    p.closing().is_none()
                        && self.bindings.get(&connection.session) == Some(&generation)
                }) else {
                    let _ = effect.into_parts();
                    if self
                        .events
                        .emit(
                            "frame_failed",
                            json!({
                                "connection": generation.get().to_string(),
                                "details": {"effect": effect_text(id), "packet_kind": "PUBLISH",
                                            "code": "unusable_generation"}
                            }),
                        )
                        .is_err()
                    {
                        self.error = Some(Failure::plain("events_unavailable"));
                    }
                    self.push_terminal(Input::FrameResult {
                        effect: id,
                        connection,
                        progress: FrameProgress::Failed,
                    });
                    return SubmitResult::Accepted;
                };
                let version = peer.version.unwrap_or_default();
                let retain =
                    retain && (version == Version::V5 || *cause == DeliveryCause::RetainedReplay);
                let expiry_val = expiry.as_deref().copied();
                let encoded = match codec::encode_delivery_segments(
                    version,
                    (packet_id, *qos),
                    topic,
                    payload,
                    dup,
                    retain,
                    expiry_val.map(|value| value.interval),
                    properties.as_deref(),
                ) {
                    Ok(encoded) if encoded.total_len <= peer.max_packet_bytes() => encoded,
                    _ => {
                        self.close.push((generation, "outgoing_packet_limit"));
                        return refused(effect, SubmitBlock::Closed);
                    }
                };
                let write_expiry = expiry_val.map(|expiry| {
                    Box::new(net::WriteExpiry {
                        expiry,
                        value_offset: encoded.expiry_value_offset,
                        started: *started,
                    })
                });
                let binding_key = (connection, packet_id);
                let binding = PubackTarget {
                    delivery,
                    claim,
                    frame: id,
                };
                if self.outgoing_bindings.contains_key(&binding_key) {
                    self.error = Some(Failure::plain("outgoing_binding_conflict"));
                    return refused(effect, SubmitBlock::InFlightLimit);
                }
                if self.outgoing_bindings.len() >= self.limits.outgoing_bindings {
                    return refused(effect, SubmitBlock::InFlightLimit);
                }
                // Register before the network task can observe this frame.
                // A rejected queue submission rolls this precise entry back.
                self.outgoing_bindings.insert(binding_key, binding);
                let track_written = write_expiry.is_some()
                    || peer.blocked_output() == Some(id)
                    || self.events.records("frame_written")
                    || cfg!(any(test, feature = "test-hooks"));
                match peer.io.try_send_outgoing(net::Write {
                    expiry: write_expiry,
                    correlation: track_written.then_some((id, connection)),
                    bytes: encoded.header,
                    payload: encoded.payload,
                    report: if track_written {
                        net::WriteReport::Complete
                    } else {
                        net::WriteReport::None
                    },
                }) {
                    Ok(()) => {
                        let was_retained = peer.submitted_output(id);
                        if track_written {
                            self.frames.insert(
                                id,
                                PendingFrame {
                                    connection,
                                    kind: FrameKind::Publish,
                                    packet_id: Some(packet_id.get()),
                                    receive_serial: None,
                                    terminal: None,
                                    wire_released: false,
                                    was_retained,
                                },
                            );
                        }
                        let _ = effect.into_parts();
                        if self
                            .events
                            .emit_with("frame_queued", || {
                                json!({
                                    "connection": generation.get().to_string(),
                                    "details": {
                                        "packet_kind": "PUBLISH",
                                        "effect": effect_text(id),
                                        "delivery": delivery_text(delivery),
                                        "target": connection.session.get().to_string(),
                                        "packet_id": packet_id.get(),
                                        "claim": operation_text(claim),
                                        "dup": dup,
                                        "retain": retain,
                                    },
                                })
                            })
                            .is_err()
                        {
                            self.error = Some(Failure::plain("events_unavailable"));
                        }
                        SubmitResult::Accepted
                    }
                    Err(TrySendError::Full(_)) => {
                        self.outgoing_bindings.remove(&binding_key);
                        self.mark_pending_peers.push(generation);
                        if peer.cold_mut().blocked_output.replace(id) != Some(id)
                            && self
                                .events
                                .emit_with("effect_retained", || {
                                    json!({
                                        "connection": generation.get().to_string(),
                                        "details": {
                                            "code": "output_queue_full",
                                            "effect": effect_text(id),
                                            "packet_kind": "PUBLISH",
                                        },
                                    })
                                })
                                .is_err()
                        {
                            self.error = Some(Failure::plain("events_unavailable"));
                        }
                        refused(effect, SubmitBlock::TransportBackpressure)
                    }
                    Err(TrySendError::Disconnected(_)) => {
                        self.outgoing_bindings.remove(&binding_key);
                        self.close.push((generation, "outgoing_queue_full"));
                        refused(effect, SubmitBlock::Closed)
                    }
                }
            }
        }
    }
}
