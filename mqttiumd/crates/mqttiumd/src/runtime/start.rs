//! The start and the end of a run: the paths the store and the journal
//! may not share, the thread that accepts connections, the opening of
//! the store, the owner and the listener, the readiness report, the loop
//! of the runtime, and the shutdown of the writer and the shards.

use super::*;

fn normalize_path_for_collision(path: &std::path::Path) -> PathBuf {
    if let (Some(parent), Some(name)) = (path.parent(), path.file_name()) {
        let dir = if parent.as_os_str().is_empty() {
            std::path::Path::new(".")
        } else {
            parent
        };
        if let Ok(canon) = std::fs::canonicalize(dir) {
            return canon.join(name);
        }
    }
    path.to_path_buf()
}

fn path_with_suffix(path: &std::path::Path, suffix: &str) -> PathBuf {
    let mut os = path.as_os_str().to_os_string();
    os.push(suffix);
    PathBuf::from(os)
}

fn paths_collide_with_db(events: &std::path::Path, db: &std::path::Path) -> bool {
    let ev = normalize_path_for_collision(events);
    let ev_prev = path_with_suffix(&ev, ".previous");
    let db_norm = normalize_path_for_collision(db);
    let db_paths = [
        db_norm.clone(),
        path_with_suffix(&db_norm, "-wal"),
        path_with_suffix(&db_norm, "-shm"),
        path_with_suffix(&db_norm, ".mlog"),
        path_with_suffix(&db_norm, ".writer-lock"),
    ];
    db_paths.iter().any(|p| *p == ev || *p == ev_prev)
}

pub fn run(config: Config) -> Result<(), Failure> {
    let disk_db_active = matches!(
        config.storage_mode,
        crate::writer::StorageMode::Durable
            | crate::writer::StorageMode::Log
            | crate::writer::StorageMode::Volatile {
                snapshot_interval: Some(_),
            }
    ) || config.require_existing_db;
    if disk_db_active && paths_collide_with_db(&config.events, &config.db) {
        return Err(Failure::plain("events_db_path_collision"));
    }
    let events = Events::open_at(&config.events, config.event_level).map_err(event_failure)?;
    let security = match &config.tls {
        Some(files) => Security::load(files).map(Some).map_err(Failure::plain),
        None if config.listen.ip().is_loopback() => Ok(None),
        None => Err(Failure::plain("cleartext_requires_loopback")),
    };
    let security = match security {
        Ok(security) => security,
        Err(failure) => {
            let _ = events.emit("startup_or_runtime_refused", failure.fields());
            return Err(failure);
        }
    };
    let cancel = Arc::new(AtomicBool::new(false));
    let signal = cancel.clone();
    ctrlc::set_handler(move || signal.store(true, Ordering::Relaxed))
        .map_err(|_| Failure::plain("signal_handler"))?;
    events
        .emit(
            "starting",
            json!({
                "details": {
                    "test_hooks": cfg!(feature="test-hooks"),
                    "sqlite_kill_points": mqttiumd_store_sqlite::KILL_POINTS_ENABLED,
                },
            }),
        )
        .map_err(event_failure)?;
    let policy = security
        .as_ref()
        .map(|security| security.policy.storage().clone());
    let delivery_shards = config
        .delivery_shards
        .min(crate::delivery_shard::MAX_DELIVERY_SHARDS);
    let use_direct = matches!(
        config.storage_mode,
        crate::writer::StorageMode::Volatile { .. }
    ) && delivery_shards == 0;
    if use_direct {
        let (mut direct, boot) = DirectWriter::open(
            config.db,
            events.clone(),
            config.hooks.clone(),
            cancel.clone(),
            policy,
            config.require_existing_db,
            config.storage_mode,
        )?;
        let result = run_inner(
            &config.listen,
            config.hooks,
            &events,
            cancel.clone(),
            StoreHandle::Direct(&mut direct),
            Some(boot),
            security,
            config.limits,
            0,
        );
        if let Err(failure) = &result {
            let _ = events.emit("startup_or_runtime_refused", failure.fields());
        }
        cancel.store(true, Ordering::Relaxed);
        let writer_result = direct.finish();
        if let Err(failure) = &writer_result {
            let _ = events.emit("startup_or_runtime_refused", failure.fields());
        } else {
            events.emit("stopped",json!({
                "details": {"writer_released": true, "accepted_commands_certainly_rejected": false},
            })).map_err(event_failure)?;
        }
        return result.and(writer_result);
    }
    let mut writer = Writer::spawn_configured_sharded(
        config.db,
        events.clone(),
        config.hooks.clone(),
        cancel.clone(),
        policy,
        config.require_existing_db,
        config.storage_mode,
        delivery_shards,
    )?;
    let result = run_inner(
        &config.listen,
        config.hooks,
        &events,
        cancel.clone(),
        StoreHandle::Threaded(&mut writer),
        None,
        security,
        config.limits,
        delivery_shards,
    );
    if let Err(failure) = &result {
        let _ = events.emit("startup_or_runtime_refused", failure.fields());
    }
    cancel.store(true, Ordering::Relaxed);
    // Stop is queued after already accepted work. No accepted command is called rejected.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut stop_sent = false;
    while Instant::now() < deadline && !writer.is_finished() {
        if !stop_sent && writer.sender.try_send(Request::Stop).is_ok() {
            stop_sent = true;
        }
        while writer.receiver.try_recv().is_ok() {}
        while writer.control_receiver.try_recv().is_ok() {}
        thread::sleep(Duration::from_millis(2));
    }
    if !writer.is_finished() {
        let _ = events.emit(
            "shutdown_incomplete",
            json!({"details":{"code":"writer_deadline","accepted_outcome":"unknown"}}),
        );
        return Err(Failure::plain("writer_shutdown_deadline"));
    }
    let writer_result = writer.join_finished();
    if let Err(failure) = &writer_result {
        let _ = events.emit("startup_or_runtime_refused", failure.fields());
    } else {
        events.emit("stopped",json!({
            "details": {"writer_released": true, "accepted_commands_certainly_rejected": false},
        })).map_err(event_failure)?;
    }
    result.and(writer_result)
}
/// Accepts on its own thread so that the reactor never polls the listener.
/// The lane is bounded: a reactor that does not take connections leaves
/// them in the kernel backlog.
struct Acceptor {
    accepted: std::sync::mpsc::Receiver<Result<std::net::TcpStream, ()>>,
    handle: thread::JoinHandle<()>,
}

impl Acceptor {
    fn spawn(
        listener: TcpListener,
        cancel: Arc<AtomicBool>,
        wake: crate::wake::Wake,
    ) -> Result<Self, Failure> {
        listener
            .set_nonblocking(false)
            .map_err(|_| Failure::plain("listen_blocking"))?;
        let (sender, accepted) = std::sync::mpsc::sync_channel(4);
        let handle = thread::Builder::new()
            .name("mqttiumd-acceptor".into())
            .spawn(move || {
                while !cancel.load(Ordering::Relaxed) {
                    let item = match listener.accept() {
                        Ok((stream, _)) => Ok(stream),
                        Err(error)
                            if matches!(
                                error.kind(),
                                std::io::ErrorKind::Interrupted
                                    | std::io::ErrorKind::ConnectionAborted
                                    | std::io::ErrorKind::WouldBlock
                            ) =>
                        {
                            continue;
                        }
                        Err(_) => Err(()),
                    };
                    if cancel.load(Ordering::Relaxed) {
                        break;
                    }
                    let failed = item.is_err();
                    if sender.send(item).is_err() {
                        break;
                    }
                    wake.wake();
                    if failed {
                        break;
                    }
                }
            })
            .map_err(|_| Failure::plain("acceptor_spawn"))?;
        Ok(Self { accepted, handle })
    }

    /// The cancel flag is already set. One local connection ends the
    /// blocking accept; the listener closes with the thread.
    fn stop(self, address: SocketAddr) {
        drop(self.accepted);
        let _ = std::net::TcpStream::connect_timeout(&address, Duration::from_millis(200));
        let deadline = Instant::now() + Duration::from_secs(1);
        while !self.handle.is_finished() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(2));
        }
        if self.handle.is_finished() {
            let _ = self.handle.join();
        }
    }
}

fn check_startup_cancel(cancel: &AtomicBool) -> Result<(), Failure> {
    if cancel.load(Ordering::Relaxed) {
        Err(Failure::plain("startup_cancelled"))
    } else {
        Ok(())
    }
}
trait StartupRecv<T> {
    fn try_recv_startup(&self) -> Result<T, TryRecvError>;
    fn recv_timeout_startup(
        &self,
        timeout: Duration,
    ) -> Result<T, std::sync::mpsc::RecvTimeoutError>;
}

impl<T> StartupRecv<T> for std::sync::mpsc::Receiver<T> {
    fn try_recv_startup(&self) -> Result<T, TryRecvError> {
        self.try_recv()
    }
    fn recv_timeout_startup(
        &self,
        timeout: Duration,
    ) -> Result<T, std::sync::mpsc::RecvTimeoutError> {
        self.recv_timeout(timeout)
    }
}

impl<T> StartupRecv<T> for crate::wake::LaneReceiver<T> {
    fn try_recv_startup(&self) -> Result<T, TryRecvError> {
        self.try_recv()
    }
    fn recv_timeout_startup(
        &self,
        timeout: Duration,
    ) -> Result<T, std::sync::mpsc::RecvTimeoutError> {
        self.recv_timeout(timeout)
    }
}

fn startup_reply<T>(
    receiver: &impl StartupRecv<T>,
    cancel: &AtomicBool,
    deadline: Instant,
    code: &'static str,
) -> Result<T, Failure> {
    loop {
        // An already produced typed failure is read before observing the stop
        // flag. Cancelling an outstanding request never calls it rejected.
        match receiver.try_recv_startup() {
            Ok(reply) => return Ok(reply),
            Err(TryRecvError::Disconnected) => return Err(Failure::plain("writer_disconnected")),
            Err(TryRecvError::Empty) => {}
        }
        check_startup_cancel(cancel)?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(Failure::plain(code));
        }
        match receiver.recv_timeout_startup(remaining.min(Duration::from_millis(10))) {
            Ok(reply) => return Ok(reply),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return Err(Failure::plain("writer_disconnected"));
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
        }
    }
}
#[allow(clippy::too_many_arguments)]
pub(super) fn run_inner(
    listen: &SocketAddr,
    hooks: Hooks,
    events: &Events,
    cancel: Arc<AtomicBool>,
    mut writer: StoreHandle<'_>,
    direct_boot: Option<Boot>,
    security: Option<Security>,
    limits: Limits,
    delivery_shards: usize,
) -> Result<(), Failure> {
    let _ = &hooks;
    let (boot, wake) = match (&mut writer, direct_boot) {
        (StoreHandle::Direct(_), Some(boot)) => (boot, crate::wake::Wake::new()),
        (StoreHandle::Threaded(w), None) => {
            let boot = w
                .boot
                .recv_timeout(Duration::from_secs(10))
                .map_err(|_| Failure::plain("startup_deadline"))??;
            (boot, w.wake.clone())
        }
        _ => return Err(Failure::plain("startup_protocol")),
    };
    let epoch = boot.grant.epoch();
    let incarnation = boot.grant.incarnation();
    let collect_closed = boot.collect_closed;
    let sqlite = boot.sqlite.clone();
    let storage_mode = boot.storage_mode;
    let total_owners = NonZeroU32::new(
        u32::try_from(delivery_shards)
            .ok()
            .and_then(|n| n.checked_add(1))
            .ok_or_else(|| Failure::plain("authority_allocation"))?,
    )
    .ok_or_else(|| Failure::plain("authority_allocation"))?;
    let mut authority = OwnerAuthority::from_grant(boot.grant, total_owners)
        .map_err(|_| Failure::plain("authority_allocation"))?;
    let outbox = authority
        .create_outbox(
            LogicalOwnerId::new(0),
            NonZeroUsize::new(limits.effect_box).expect("nonzero"),
            nz(64),
        )
        .map_err(|_| Failure::plain("outbox_creation"))?;
    // Every session can hold a window of exchanges only its client ends:
    // the table is sized for the sessions, not for the connections (record
    // 0061).
    let max_exchanges = crate::writer::SESSIONS * crate::receiving::MAXIMUM;
    let mut owner = Owner::new(
        OwnerConfig {
            max_sessions: nz(crate::writer::SESSIONS as u32),
            max_routes: nz(mqttiumd_contract::pubsub::MAX_SUBSCRIPTIONS as u32),
            max_exchanges: nz(max_exchanges as u32),
            session_qos2_exchanges: nz(crate::receiving::MAXIMUM as u32),
            account: AccountId::new(1),
            payload_bytes: PayloadBytes::new(64 * 1024 * 1024),
            resolution_delay: LogicalDuration::new(10000),
            retained_operations: nz(128),
            remembered_connections: nz(8),
        },
        outbox,
    )
    .map_err(|_| Failure::plain("owner_creation"))?;
    #[cfg(feature = "test-hooks")]
    let enable_inline_claims = hooks.directory.is_none();
    #[cfg(not(feature = "test-hooks"))]
    let enable_inline_claims = true;
    if enable_inline_claims {
        owner.enable_inline_claims();
    }
    if delivery_shards > 0 {
        owner.enable_delegated_deliveries();
    }
    let mut session_revisions = FlatMap::new();
    for s in boot.sessions.values() {
        session_revisions.insert(s.id, s.revision);
    }
    // The worker handle stays in outer run for cleanup even if recovery fails.
    let expiry_timers = boot
        .sessions
        .values()
        .filter_map(|s| {
            s.detached
                .as_deref()
                .copied()
                .filter(|d| d.deadline.is_some())
                .map(|detached| {
                    (
                        s.id,
                        ExpiryTimer {
                            guard: SessionGuard {
                                session: s.id,
                                opened_by: s.opened_by,
                            },
                            detached,
                            queued: false,
                        },
                    )
                })
        })
        .collect();
    let mut runtime = Runtime {
        progress: 0,
        wake,
        limits,
        security,
        message_expiry_request: None,
        message_expiry_retry: false,
        message_expiry_retry_attempts: 0,
        message_expiry_observation: None,
        message_expiry_after: None,
        message_expiry_scan: Instant::now(),
        message_expiry_input: None,
        message_last_time: None,
        last_message_clock_sync: Instant::now(),
        message_clock_needed: true,
        clock: boot.clock,
        clock_anchor: boot.clock_anchor,
        clock_checkpoint: None,
        last_clock_checkpoint: Instant::now(),
        checkpoint_degraded: false,
        expiry_timers,
        expiry_inputs: VecDeque::new(),
        last_expiry_inventory: None,
        owner,
        _authority: authority,
        shards: None,
        shard_notices: Vec::new(),
        pending_suback_notify: FlatSet::new(),
        incarnation,
        epoch,
        writer,
        events: events.clone(),
        peers: PeerTable::new(),
        topic_cache: codec::TopicCache::new(),
        ready_peers: net::ReadyQueue::new(),
        ready_raw: Vec::new(),
        pending_peers: FlatSet::new(),
        capacity_waiters: 0,
        retiring: Vec::new(),
        session_ids: boot.sessions.values().map(|s| s.id).collect(),
        session_revisions,
        sessions: boot.sessions,
        session_requests: BTreeMap::new(),
        will_connects: BTreeMap::new(),
        will_reads: None,
        last_will_deferred: None,
        next_session_request: 0,
        generation: ConnectionGeneration::new(0),
        peer_cursor: 0,
        peer_keys: Vec::new(),
        scratch_incoming: VecDeque::new(),
        scratch_written: Vec::new(),
        scratch_progress: Vec::new(),
        scratch_responses: Vec::new(),
        sink_close: Vec::new(),
        sink_mark_pending: Vec::new(),
        sink_work_targets: Vec::new(),
        sink_attach_ready: Vec::new(),
        sink_suback_ready: Vec::new(),
        last_peer_deadline_scan: Instant::now(),
        inputs: VecDeque::new(),
        ack_inputs: VecDeque::new(),
        control_inputs: VecDeque::new(),
        closing_inputs: VecDeque::new(),
        terminal_inputs: VecDeque::new(),
        terminal_effects: ScopedSeqMap::with_scope((epoch, LogicalOwnerId::new(0))),
        deferred_inputs: VecDeque::new(),
        deferred_by_conn: FlatMap::new(),
        effects_awaited: false,
        deferred_retries: 0,
        credit_progress: Instant::now(),
        timers: BTreeMap::new(),
        inflight: ScopedSeqMap::with_scope((epoch, LogicalOwnerId::new(0))),
        inflight_route_ops: ScopedSeqMap::with_scope((epoch, LogicalOwnerId::new(0))),
        lifecycle_inflight: BTreeMap::new(),
        last_session_change: None,
        session_cardinality_dirty: false,
        last_session_cardinality: None,
        work_reads: ScopedSeqMap::with_scope((epoch, LogicalOwnerId::new(0))),
        pending_work: VecDeque::new(),
        pending_work_set: BTreeSet::new(),
        queued_work_sessions: FlatSet::new(),
        cached_bindings: FlatMap::new(),
        bindings_dirty: true,
        stale_frames_dirty: false,
        timers_dirty: false,
        receipt_pressure_blocked: false,
        last_work_scan: Instant::now(),
        last_will_scan: Instant::now(),
        last_maintenance_flush: Instant::now(),
        maintenance_needed: false,
        #[cfg(feature = "test-hooks")]
        work_cache: BTreeMap::new(),
        #[cfg(feature = "test-hooks")]
        deferred_work: VecDeque::new(),
        #[cfg(feature = "test-hooks")]
        emission_barriers: VecDeque::new(),
        #[cfg(feature = "test-hooks")]
        last_consume_result: None,
        started: Instant::now(),
        #[cfg(feature = "test-hooks")]
        captured: None,
        #[cfg(feature = "test-hooks")]
        hooks,
        #[cfg(feature = "test-hooks")]
        deferred_ack: None,
        #[cfg(feature = "test-hooks")]
        deferred_ack_used: false,
        retained_output: Default::default(),
        frames: ScopedSeqMap::with_scope((epoch, LogicalOwnerId::new(0))),
        outgoing_bindings: FlatMap::new(),
        puback_observation: 0,
        #[cfg(feature = "test-hooks")]
        copied_puback: None,
        #[cfg(feature = "test-hooks")]
        deferred_outgoing_written: None,
        #[cfg(feature = "test-hooks")]
        copied_outgoing_frame: None,
        #[cfg(feature = "test-hooks")]
        last_finish_result: None,
        #[cfg(feature = "test-hooks")]
        copied_session_change: None,
        #[cfg(feature = "test-hooks")]
        copied_session_close: None,
    };
    let result = (|| {
        let mut cursor = None;
        let mut rows = 0usize;
        let mut saw_epoch = false;
        loop {
            check_startup_cancel(&cancel)?;
            let response = match runtime.writer.exec_recovery(cursor)? {
                Some(reply) => reply,
                None => {
                    let StoreHandle::Threaded(w) = &runtime.writer else {
                        return Err(Failure::plain("recovery_protocol"));
                    };
                    startup_reply(
                        &w.receiver,
                        &cancel,
                        Instant::now() + Duration::from_secs(10),
                        "recovery_deadline",
                    )?
                }
            };
            let Response::Recovery(page) = response else {
                return Err(Failure::plain("recovery_protocol"));
            };
            let page = page.map_err(|refused| match refused {
                mqttiumd_store_api::RecoveryRefused::Read(e) => {
                    Failure::read(e, ReadPhase::Recovery)
                }
                _ => Failure::plain(refused.code()),
            })?;
            check_startup_cancel(&cancel)?;
            rows = rows
                .checked_add(page.facts.as_slice().len())
                .filter(|v| *v <= 65536)
                .ok_or_else(|| Failure::read(ReadError::LimitExceeded, ReadPhase::Recovery))?;
            for fact in page.facts.as_slice() {
                if let RecoveredFact::Epoch {
                    incarnation: found,
                    epoch: found_epoch,
                } = fact
                {
                    if *found != incarnation || *found_epoch != epoch {
                        return Err(Failure::plain("recovered_epoch_mismatch"));
                    }
                    saw_epoch = true;
                }
            }
            let last = page.next.is_none();
            cursor = page.next;
            runtime.queue(Input::Recovery {
                facts: page.facts,
                last,
            })?;
            runtime.drain_inputs()?;
            if !runtime.inputs.is_empty() {
                return Err(Failure::plain("recovery_incomplete"));
            }
            if last {
                break;
            }
        }
        if !saw_epoch || runtime.owner.is_recovering() {
            return Err(Failure::plain("recovery_incomplete"));
        }
        if collect_closed {
            let collection = ClosedReceiptCollection {
                through: mqttiumd_contract::ids::RunEpoch::new(
                    epoch
                        .get()
                        .checked_sub(1)
                        .ok_or_else(|| Failure::plain("epoch_underflow"))?,
                ),
                limit: 128,
            };
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut completed = false;
            for _ in 0..128 {
                check_startup_cancel(&cancel)?;
                if Instant::now() >= deadline {
                    return Err(Failure::plain("startup_collection_deadline"));
                }
                let reply = match runtime.writer.exec_closed_collection(collection)? {
                    Some(reply) => reply,
                    None => {
                        let StoreHandle::Threaded(w) = &runtime.writer else {
                            return Err(Failure::plain("startup_collection_protocol"));
                        };
                        startup_reply(
                            &w.control_receiver,
                            &cancel,
                            deadline,
                            "startup_collection_deadline",
                        )?
                    }
                };
                let ControlResponse::Closed {
                    collection: found,
                    result,
                } = reply
                else {
                    return Err(Failure::plain("startup_collection_protocol"));
                };
                if found != collection {
                    return Err(Failure::plain("startup_collection_protocol"));
                }
                match result? {
                    ClosedReceiptReply::Applied {
                        through,
                        removed,
                        removed_origins,
                        retained,
                        more,
                    } => {
                        events
                            .emit(
                                "closed_receipts_collected",
                                json!({
                                    "details": {
                                        "through": through.get().to_string(),
                                        "removed": removed,
                                        "removed_origins": removed_origins,
                                        "retained": retained.to_string(),
                                        "more": more,
                                    },
                                }),
                            )
                            .map_err(event_failure)?;
                        if !more {
                            completed = true;
                            break;
                        }
                    }
                    ClosedReceiptReply::PendingOrUnknown => thread::sleep(Duration::from_millis(2)),
                    ClosedReceiptReply::Refused(_) => {
                        return Err(Failure::plain("startup_collection_refused"));
                    }
                }
            }
            if !completed {
                return Err(Failure::plain("startup_collection_budget"));
            }
        }
        // Empty-prefix registration is itself an exact owner transition. No
        // listener or business command exists until its durable reply is applied.
        check_startup_cancel(&cancel)?;
        runtime.queue(Input::MaintainReceipts)?;
        let deadline = Instant::now() + Duration::from_secs(30);
        while !runtime.owner.collection_ready() {
            if Instant::now() >= deadline {
                return Err(Failure::plain("receipt_registration_deadline"));
            }
            runtime.writer_results()?;
            check_startup_cancel(&cancel)?;
            runtime.due_deadlines()?;
            runtime.drain_inputs()?;
            runtime.dispatch()?;
            thread::sleep(Duration::from_millis(1));
        }
        if delivery_shards > 0 {
            check_startup_cancel(&cancel)?;
            let shard_limits = runtime
                .owner
                .shard_limits()
                .ok_or_else(|| Failure::plain("shard_limits_missing"))?;
            let mut outboxes = Vec::with_capacity(delivery_shards);
            for i in 1..=delivery_shards {
                let ob = runtime
                    ._authority
                    .create_outbox(
                        LogicalOwnerId::new(i as u64),
                        NonZeroUsize::new(limits.effect_box).expect("nonzero"),
                        nz(64),
                    )
                    .map_err(|_| Failure::plain("shard_outbox_creation"))?;
                outboxes.push(ob);
            }
            let store_handles = match &mut runtime.writer {
                StoreHandle::Threaded(w) => w.take_shard_handles(),
                StoreHandle::Direct(_) => {
                    return Err(Failure::plain("shard_requires_threaded_writer"));
                }
            };
            let pool = crate::delivery_shard::ShardPool::spawn(
                outboxes,
                store_handles,
                shard_limits,
                limits,
                events.clone(),
                runtime.clock.clone(),
                runtime.wake.clone(),
                cancel.clone(),
            )?;
            runtime.shards = Some(pool);
        }
        runtime.emit_session_clock_anchor()?;
        let now = runtime.message_now()?;
        events
            .emit(
                "message_expiry_clock",
                json!({
                    "details": {
                        "domain": hex_domain(now.domain),
                        "tick": now.tick.to_string(),
                        "origin_status": runtime.clock.origin_status(),
                        "policy": "quarantine-on-unknown-origin",
                        "time_unit": "nanoseconds",
                        "scan_ms": 100,
                        "batch": MESSAGE_EXPIRY_PAGE_SIZE,
                        "started_proof": "positive-socket-write-then-durable-marker",
                        "terminal_input_slots": limits.terminal_inputs,
                        "socket_write_receipt_slots": net::WRITTEN_SLOTS,
                    },
                }),
            )
            .map_err(event_failure)?;
        events.emit("receipt_policy",json!({
            "details": {
                "receipt_window": mqttiumd_contract::receipts::RECEIPT_WINDOW,
                "receipt_data_window": mqttiumd_contract::receipts::RECEIPT_DATA_WINDOW,
                "receipt_control_reserve": mqttiumd_contract::receipts::RECEIPT_CONTROL_RESERVE,
                "receipt_collection_rows": mqttiumd_contract::receipts::MAX_COLLECTION_ROWS,
                "receipt_collection_bytes": mqttiumd_store_api::ReadBudget::COLLECTION.max_bytes,
                "receipt_collection_field_bytes": mqttiumd_store_api::ReadBudget::COLLECTION.max_field_bytes,
                "receipt_collection_trigger": mqttiumd_contract::receipts::RECEIPT_COLLECTION_TRIGGER,
                "max_receipt_origins": mqttiumd_contract::receipts::MAX_RECEIPT_ORIGINS,
                "startup_origins_trigger": 32,
                "startup_receipts_trigger": 4096,
                "startup_closed_collection": collect_closed,
                "control_request_slots": crate::writer::CONTROL_QUEUE,
                "control_reply_slots": crate::writer::CONTROL_QUEUE,
                "control_input_slots": crate::writer::CONTROL_QUEUE,
            },
        })).map_err(event_failure)?;
        if let Some(sqlite) = &sqlite {
            events
                .emit(
                    "sqlite_configuration",
                    json!({
                        "details": {
                            "journal_mode": sqlite.journal_mode,
                            "synchronous": sqlite.synchronous,
                            "wal_autocheckpoint": sqlite.wal_autocheckpoint,
                            "checkpoint_policy": sqlite.checkpoint_policy,
                            "sqlite_version": sqlite.sqlite_version,
                            "user_version": sqlite.user_version,
                            "page_size": sqlite.page_size,
                            "kill_points": sqlite.kill_points,
                            "hooks_active": false,
                            "install_order": "sqlite-autocheckpoint-before-optional-business-hook",
                        },
                    }),
                )
                .map_err(event_failure)?;
        }
        let sqlite_schema = sqlite.as_ref().map_or_else(
            || {
                if runtime.security.is_some() {
                    mqttiumd_store_sqlite::SECURE_SCHEMA_VERSION
                } else {
                    mqttiumd_store_sqlite::SCHEMA_VERSION
                }
            },
            |s| s.user_version,
        );
        check_startup_cancel(&cancel)?;
        let listener =
            TcpListener::bind(listen).map_err(|_| Failure::plain("listen_unavailable"))?;
        let address = listener
            .local_addr()
            .map_err(|_| Failure::plain("listen_address"))?;
        check_startup_cancel(&cancel)?;
        let profile = if runtime.security.is_some() {
            "mtls-will-pubsub-v1"
        } else {
            "tcp-will-pubsub-v1"
        };
        let pi_candidate_limit =
            mqttiumd_core::owner::outgoing_pi_candidate_limit(max_exchanges, u16::MAX);
        let pi_bitmap_bits = mqttiumd_core::owner::OUTGOING_PI_BITMAP_BITS;
        let mut readiness = json!({
            "details": {
                "address": address.to_string(),
                "epoch": epoch.get().to_string(),
                "incarnation": incarnation.get().to_string(),
                "max_connections": limits.connections,
                "max_sessions": crate::writer::SESSIONS,
                "max_packet_bytes": net::MAX_PACKET_BYTES,
                "max_client_id_bytes": crate::writer::MAX_CLIENT,
                "max_topic_bytes": crate::writer::MAX_TOPIC,
                "max_payload_bytes": crate::writer::MAX_PAYLOAD,
                "receive_maximum": crate::receiving::MAXIMUM,
                "max_exchange_slots": max_exchanges,
                "input_slots": net::INPUT_SLOTS,
                "output_slots": net::OUTPUT_SLOTS,
                "writer_slots": crate::writer::QUEUE,
                "commands_in_flight": limits.in_flight,
                "max_timers": limits.timers,
                "receipt_slots": crate::writer::limits().receipt_slots.get(),
                "partial_packet_ms": 5000,
                "connect_ms": 3000,
                "write_ms": 250,
                "remembered_connections": 8,
                "max_subscriptions": mqttiumd_contract::pubsub::MAX_SUBSCRIPTIONS,
                "max_targets": mqttiumd_contract::retained::MAX_RETAIN_MASK_TARGETS,
                "outgoing_per_connection": mqttiumd_core::owner::MAX_OUTGOING_WINDOW,
                "outgoing_metadata": limits.outgoing_bindings,
                "outgoing_packet_identifier_max": u16::MAX,
                "outgoing_packet_identifier_probe_limit": pi_candidate_limit,
                "outgoing_payload_bytes": 64*1024*1024,
                "work_page_items": 8,
                "work_scan_ms": 100,
                "wildcard_subscriptions": true,
                "no_local": true,
                "shared_subscriptions": false,
                "subscription_identifiers": false,
                "sqlite_schema": sqlite_schema,
                "retained_available": true,
                "retained_topics": mqttiumd_contract::retained::MAX_RETAINED_TOPICS,
                "retained_bytes": mqttiumd_contract::retained::MAX_RETAINED_BYTES,
                "max_replay_candidates": mqttiumd_contract::retained::MAX_REPLAYS,
                "profile": profile,
            },
        });
        if let Some(details) = readiness["details"].as_object_mut() {
            details.insert("storage_mode".into(), json!(storage_mode.mode_name()));
            details.insert(
                "durability_guarantee".into(),
                json!(storage_mode.durability_guarantee()),
            );
            details.insert(
                "snapshot_interval_secs".into(),
                json!(storage_mode.snapshot_interval_secs()),
            );
            details.insert("delivery_shards".into(), json!(delivery_shards));
            details.insert(
                "outgoing_packet_identifier_bitmap_bits".into(),
                json!(pi_bitmap_bits),
            );
            details.insert(
                "outgoing_packet_identifier_fallback_probes".into(),
                json!(pi_candidate_limit.saturating_sub(usize::from(pi_bitmap_bits))),
            );
            details.insert(
                "qos0_window".into(),
                json!(mqttiumd_core::owner::QOS0_WINDOW),
            );
            details.insert(
                "qos0_output_reserve".into(),
                json!(net::QOS0_OUTPUT_RESERVE),
            );
            details.insert(
                "session_expiry_timer_slots".into(),
                json!(crate::writer::SESSIONS),
            );
            details.insert(
                "session_expiry_per_turn".into(),
                json!(SESSION_EXPIRY_PER_TURN),
            );
        }
        if let Some(security) = &runtime.security {
            events
                .emit(
                    "tls_profile",
                    json!({
                        "details": {
                            "version": "TLS1.3",
                            "client_certificate": "required",
                            "policy_fingerprint": hex_domain(security.policy.storage().fingerprint),
                            "max_handshakes": security::MAX_UNAUTHENTICATED,
                            "handshake_ms": security::HANDSHAKE_TIMEOUT.as_millis() as u64,
                            "handshake_bytes": security::HANDSHAKE_BYTES,
                            "certificate_bytes": security::CERT_BYTES,
                            "chain_bytes": security::CHAIN_BYTES,
                            "chain_certificates": security::CHAIN_CERTS,
                            "tls_buffer_bytes": security::TLS_BUFFER_BYTES,
                            "resumption": false,
                            "early_data": false,
                        },
                    }),
                )
                .map_err(event_failure)?;
        }
        events
            .emit(
                "will_capabilities",
                json!({
                    "details": {
                        "will": true,
                        "will_qos": 1,
                        "will_retain": true,
                        "will_delay_interval": 0,
                        "will_message_expiry": false,
                        "max_will_intents": mqttiumd_contract::will::MAX_WILL_INTENTS,
                        "max_will_bytes": mqttiumd_contract::will::MAX_WILL_BYTES,
                        "max_will_receipt_bytes": 8388608,
                    },
                }),
            )
            .map_err(event_failure)?;
        events.emit("ready", readiness).map_err(event_failure)?;
        // The sharded mode has no model of its own protocol yet and known
        // open conditions (record 0066): the operator is told.
        let shards = if delivery_shards > 0 {
            format!(", delivery_shards={delivery_shards} experimental")
        } else {
            String::new()
        };
        match storage_mode {
            crate::writer::StorageMode::Durable => {
                println!("mqttiumd ready {address} ({profile}{shards})");
            }
            crate::writer::StorageMode::Log => {
                println!(
                    "mqttiumd ready {address} ({profile}, storage=log, durability={}{shards})",
                    storage_mode.durability_guarantee()
                );
            }
            crate::writer::StorageMode::Volatile { .. } => {
                println!(
                    "mqttiumd ready {address} ({profile}, storage=volatile, durability={}{shards})",
                    storage_mode.durability_guarantee()
                );
            }
        }
        let acceptor = Acceptor::spawn(listener, cancel.clone(), runtime.wake.clone())?;
        let served = (|| {
            let mut counters_due = Instant::now() + COUNTERS_INTERVAL;
            while !cancel.load(Ordering::Relaxed) {
                let before = runtime.progress;
                for _ in 0..32 {
                    if !runtime.admits_handshake() {
                        break;
                    }
                    match acceptor.accepted.try_recv() {
                        Ok(Ok(stream)) => {
                            runtime.progress = runtime.progress.wrapping_add(1);
                            let _ = runtime.accept(stream)?;
                        }
                        Ok(Err(())) => return Err(Failure::plain("accept_failed")),
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => {
                            return Err(Failure::plain("accept_failed"));
                        }
                    }
                }
                runtime.turn()?;
                let idle = runtime.progress == before;
                // The idle wait below is shorter than the interval, so the
                // summary is at most one tick late.
                if Instant::now() >= counters_due {
                    runtime.events.flush_tallies().map_err(event_failure)?;
                    counters_due = Instant::now() + COUNTERS_INTERVAL;
                }
                if idle {
                    // Nothing was runnable. Every producer signals after it
                    // publishes; the timeout only bounds the wall-clock
                    // checks that have no producer.
                    runtime.wake.wait(runtime.idle_wait());
                }
            }
            // What was counted since the last summary is not lost at the end.
            runtime.events.flush_tallies().map_err(event_failure)
        })();
        acceptor.stop(address);
        served
    })();
    cancel.store(true, Ordering::Relaxed);
    // A typed startup/read failure remains authoritative and performs no
    // further durable writes. A normal stop finishes safe zero-expiry ends.
    let session_result = if result.is_ok() {
        runtime.shutdown_sessions(Duration::from_secs(2))
    } else {
        Ok(())
    };
    for (_, p) in std::mem::take(&mut runtime.peers) {
        p.io.close();
        runtime.retiring.push(p.io);
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    while runtime.retiring.iter().any(|p| !p.is_finished()) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(2));
    }
    let mut socket_result = Ok(());
    for mut p in runtime.retiring {
        if !p.join_finished() {
            socket_result = Err(Failure::plain("socket_shutdown_deadline"));
        }
    }
    let shard_result = if let Some(pool) = runtime.shards.take() {
        pool.shutdown(Duration::from_secs(3))
    } else {
        Ok(())
    };
    result
        .and(session_result)
        .and(shard_result)
        .and(socket_result)
}
