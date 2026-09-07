from pathlib import Path


def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def write(path: str, text: str) -> None:
    Path(path).write_text(text, encoding="utf-8")


def replace_exact(text: str, old: str, new: str, *, label: str, count: int = 1) -> str:
    actual = text.count(old)
    if actual != count:
        raise AssertionError(f"{label}: expected {count} occurrence(s), found {actual}")
    return text.replace(old, new)


def replace_between(text: str, start: str, end: str, new: str, *, label: str) -> str:
    if text.count(start) != 1 or text.count(end) != 1:
        raise AssertionError(
            f"{label}: boundaries not unique: start={text.count(start)} end={text.count(end)}"
        )
    left, rest = text.split(start, 1)
    _old, right = rest.split(end, 1)
    return left + new + end + right


# --- persistence contract -------------------------------------------------
path = "src/mqttium/persistence/memory.py"
text = read(path)
text = replace_exact(
    text,
    "from typing import Protocol, TypeVar, runtime_checkable\n",
    "from typing import Protocol, TypeVar\n",
    label="remove runtime capability typing",
)
modern_contract = '''class InflightStore(Protocol):
    """Persistence contract required by the MQTT state machine.

    Stores provide atomic batches, bounded replay, payload-free metadata reads,
    and conditional transitions.  These are correctness/resource guarantees,
    not optional optimisations: the runtime has one persistence state-machine
    path and never falls back to eager whole-store hydration or read/modify/write
    settlement.

    Storage/backend failures must use backend-native or ordinary Python
    exceptions rather than ``MQTTError`` subclasses.  ``batch()`` must roll back
    prior mutations when its body fails and must not suppress that failure.
    """

    def batch(self) -> AbstractContextManager[None]: ...

    # Outbound records: full payloads are written/materialised only when needed;
    # acknowledgement and state changes use metadata-only operations.
    def put_out(self, msg: OutboundMessage) -> None: ...
    def get_out(self, mid: int) -> OutboundMessage | None: ...
    def delete_out(self, mid: int) -> bool: ...
    def out_summary_pages(
        self, page_size: int = 256
    ) -> Iterator[tuple[OutboundMessageSummary, ...]]: ...
    def set_out_logical_size(self, mid: int, logical_size: int) -> bool: ...
    def out_meta(self, mid: int) -> OutboundRecordMeta | None: ...
    def complete_out(
        self, mid: int, expected_state: OutboundQoSState
    ) -> OutboundRecordMeta | None: ...
    def transition_out(
        self,
        mid: int,
        expected_state: OutboundQoSState,
        new_state: OutboundQoSState,
        *,
        compact: bool = False,
    ) -> OutboundRecordMeta | None: ...
    def clear_out(self) -> None: ...

    # Inbound records: index/replay operations are bounded and transitions do
    # not reconstruct payloads. get_in() exists only when the application must
    # actually redeliver one stored message.
    def put_in(self, msg: InboundMessage) -> None: ...
    def get_in(self, mid: int) -> InboundMessage | None: ...
    def in_count(self) -> int: ...
    def in_replay_pages(
        self,
        max_messages: int = 64,
        max_bytes: int = 1 << 20,
    ) -> Iterator[tuple[InboundMessage, ...]]: ...
    def set_in_logical_size(self, mid: int, logical_size: int) -> bool: ...
    def in_meta(self, mid: int) -> InboundRecordMeta | None: ...
    def in_index_pages(
        self, page_size: int = 256
    ) -> Iterator[tuple[InboundRecordMeta, ...]]: ...
    def mark_in_delivered(self, mid: int) -> bool: ...
    def transition_in(
        self,
        mid: int,
        expected_state: InboundQoSState,
        new_state: InboundQoSState,
        *,
        user_acked: bool | None = None,
    ) -> InboundRecordMeta | None: ...
    def complete_in(
        self, mid: int, expected_state: InboundQoSState
    ) -> InboundRecordMeta | None: ...
    def clear_in(self) -> None: ...


'''
text = replace_between(
    text,
    "class InflightStore(Protocol):\n",
    '_RecordT = TypeVar("_RecordT")',
    modern_contract,
    label="replace capability hierarchy with modern contract",
)
write(path, text)

path = "src/mqttium/persistence/__init__.py"
text = read(path)
text = '''"""Persistence package."""\n\nfrom mqttium.persistence.memory import InflightStore, MemoryInflightStore\nfrom mqttium.persistence.sqlite import SqliteInflightStore\n\n__all__ = [\n    "InflightStore",\n    "MemoryInflightStore",\n    "SqliteInflightStore",\n]\n'''
write(path, text)

# --- outbound session -----------------------------------------------------
path = "src/mqttium/protocol/outbound.py"
text = read(path)
text = replace_exact(
    text,
    "from mqttium.persistence.memory import PagedInflightStore, TransitionInflightStore\n",
    "",
    label="outbound capability imports",
)
for slot in ('        "_paged_store",\n', '        "_transitions",\n'):
    text = replace_exact(text, slot, "", label=f"remove outbound slot {slot.strip()}")
text = replace_exact(
    text,
    '''        # Resolved once: a store either pages or it does not, for its lifetime.\n        self._paged_store = self.store if isinstance(self.store, PagedInflightStore) else None\n        # Same contract for conditional transitions: acknowledgement handling\n        # settles records without ever materialising a payload when the store\n        # supports it, and falls back to the whole-object path when it does not.\n        self._transitions = self.store if isinstance(self.store, TransitionInflightStore) else None\n''',
    "",
    label="outbound capability resolution",
)
settle = '''    def _settle(self, mid: int, expected_state: OutboundQoSState) -> bool:
        """Conditionally settle one outbound record without reading its payload."""
        meta = self.store.complete_out(mid, expected_state)
        if meta is None:
            return False
        self._release_reservation(meta.logical_size)
        if self._parked_entries:
            self._unpark_settled(mid)
        return True

'''
text = replace_between(
    text,
    "    def _settle(self, mid: int, expected_state: OutboundQoSState) -> bool:\n",
    "    def _unpark_settled(self, mid: int) -> None:\n",
    settle,
    label="outbound settlement",
)
on_pubrec = '''    def on_pubrec(self, raw: RawPacket) -> None:
        mid, reason_code, properties = self._decode_pubrec(raw.remaining)
        if properties is not None:
            self._engine._validate_inbound_problem_information(PacketType.PUBREC, properties)
        # MQTT 5 §4.3.3: a negative PUBREC ends the QoS 2 exchange.
        if reason_code >= 128:
            self._fail_after_pubrec(mid, reason_code)
            return
        limit = self._engine.negotiated.maximum_packet_size
        if limit is not None and limit < 4:
            record = self.store.out_meta(mid)
            if record is None:
                self._send_orphan_pubrel(mid)
                return
            if record.state is not OutboundQoSState.WAIT_PUBREC:
                return
            self._require_pubrel_capacity(4)
            return
        changed = self.store.transition_out(
            mid,
            OutboundQoSState.WAIT_PUBREC,
            OutboundQoSState.WAIT_PUBCOMP,
            compact=True,
        )
        if changed is not None:
            self._engine._send(_encode_pubrel_success(mid))
            return
        if self.store.out_meta(mid) is None:
            self._send_orphan_pubrel(mid)

'''
text = replace_between(
    text,
    "    def on_pubrec(self, raw: RawPacket) -> None:\n",
    "    def _send_orphan_pubrel(self, mid: int) -> None:\n",
    on_pubrec,
    label="outbound PUBREC single store path",
)
text = replace_exact(
    text,
    '''        if persisted:\n            transitions = self._transitions\n            if transitions is not None:\n                changed = transitions.transition_out(\n                    msg.mid,\n                    OutboundQoSState.QUEUED,\n                    target_state,\n                )\n                if changed is None:\n                    raise RuntimeError(\n                        f"Outbound mid={msg.mid} changed while launching queued publish"\n                    )\n                # MemoryInflightStore transitions the same object; SQLite only\n                # updates durable metadata. Keep the materialised object aligned\n                # in either case without rewriting its payload to the store.\n                msg.state = target_state\n                msg.encoded_publish = retained\n            else:\n                # Third-party stores keep working through the base interface.\n                # update_out guarantees state/dup persistence and may implement\n                # the same payload-free optimization as the built-in SQLite store.\n                msg.state = target_state\n                msg.encoded_publish = retained\n                self.store.update_out(msg)\n        else:\n''',
    '''        if persisted:\n            changed = self.store.transition_out(\n                msg.mid,\n                OutboundQoSState.QUEUED,\n                target_state,\n            )\n            if changed is None:\n                raise RuntimeError(\n                    f"Outbound mid={msg.mid} changed while launching queued publish"\n                )\n            # The materialised record must follow the durable transition.\n            msg.state = target_state\n            msg.encoded_publish = retained\n        else:\n''',
    label="queued outbound launch transition",
)
# update_out only persisted state/dup for the legacy contract. State transitions
# are now explicit and retransmission always sets DUP before encoding.
text = replace_exact(
    text,
    "            self.store.update_out(msg)\n",
    "",
    label="remove redundant outbound whole-record updates",
    count=2,
)
text = replace_exact(
    text,
    "        if unknown_size and self._transitions is not None:\n",
    "        if unknown_size:\n",
    label="outbound legacy size hydration guard",
)
text = replace_exact(
    text,
    "            self._transitions.set_out_logical_size(msg.mid, logical_size)\n",
    "            self.store.set_out_logical_size(msg.mid, logical_size)\n",
    label="outbound logical size write",
)
summary_pages = '''    def store_summary_pages(
        self,
    ) -> Iterable[tuple[OutboundMessageSummary, ...]]:
        yield from self.store.out_summary_pages()

'''
text = replace_between(
    text,
    "    def store_summary_pages(\n",
    "    def has_client_session_state(self) -> bool:\n",
    summary_pages,
    label="mandatory outbound summary paging",
)
if "_transitions" in text or "_paged_store" in text or "update_out(" in text:
    raise AssertionError("outbound legacy store path still present")
write(path, text)

# Engine's pagination-capability compatibility property cannot survive when the
# capability itself no longer exists. It is Internal and test-only.
path = "src/mqttium/protocol/engine.py"
text = read(path)
text = replace_exact(
    text,
    '''    @property\n    def _paged_store(self) -> object | None:\n        """Compatibility view of the outbound store pagination capability."""\n        return self.outbound._paged_store\n\n''',
    "",
    label="remove obsolete engine pagination view",
)
write(path, text)

# --- inbound session ------------------------------------------------------
path = "src/mqttium/protocol/inbound.py"
text = read(path)
text = replace_exact(text, "from itertools import chain\n", "", label="remove legacy replay chain")
text = replace_exact(
    text,
    '''from mqttium.persistence.memory import (\n    BoundedInboundReplayStore,\n    PagedInflightStore,\n    TransitionInflightStore,\n)\n''',
    "",
    label="inbound capability imports",
)
text = replace_exact(text, "REPLAY_SCAN_LIMIT = 256\n", "", label="legacy replay scan bound")
for slot in (
    '        "_bounded_replay_store",\n',
    '        "_paged_store",\n',
    '        "_transitions",\n',
):
    text = replace_exact(text, slot, "", label=f"remove inbound slot {slot.strip()}")
text = replace_exact(
    text,
    '''        # Resolved once, like the paged extension: with a transition-capable\n        # store, existence checks, delivery marks and acknowledgements never\n        # materialise an inbound payload.\n        self._transitions = self.store if isinstance(self.store, TransitionInflightStore) else None\n        self._paged_store = self.store if isinstance(self.store, PagedInflightStore) else None\n        self._bounded_replay_store = (\n            self.store if isinstance(self.store, BoundedInboundReplayStore) else None\n        )\n''',
    "",
    label="inbound capability resolution",
)
cursor = '''class InboundReplayCursor:
    """Bounded replay page iterator with one-message pushback."""

    __slots__ = ("_page_messages", "_pages", "_pending", "_remaining")

    def __init__(
        self,
        pages: Iterator[tuple[InboundMessage, ...]],
        *,
        remaining: int,
    ) -> None:
        self._pages = pages
        self._page_messages: deque[InboundMessage] | None = None
        self._pending: InboundMessage | None = None
        self._remaining = remaining

    @property
    def bounded_complete(self) -> bool:
        return self._remaining == 0 and self._pending is None and self._page_messages is None

    def begin_page(self) -> bool:
        if self._pending is not None or self._page_messages is not None:
            return True
        page = next(self._pages, None)
        if page is None:
            return False
        self._page_messages = deque(page)
        self._remaining -= len(page)
        return True

    def next_page_message(self) -> InboundMessage | None:
        if self._pending is not None:
            message = self._pending
            self._pending = None
            return message
        messages = self._page_messages
        if messages is None:
            return None
        if not messages:
            self._page_messages = None
            return None
        message = messages.popleft()
        if not messages:
            self._page_messages = None
        return message

    def push_back(self, message: InboundMessage) -> None:
        if self._pending is not None:
            raise AssertionError("replay cursor already has a pending message")
        self._pending = message


'''
text = replace_between(
    text,
    "class InboundReplayCursor:\n",
    "class InboundSession:\n",
    cursor,
    label="bounded-only replay cursor",
)
load_state = '''    def _load_recovered_state(self) -> tuple[set[int], int, int, tuple[int, ...]]:
        """Restore identifiers and accounting from the payload-free store index."""
        mids: set[int] = set()
        recovered_qos1: list[int] = []
        pending_bytes = 0
        session_state_qos2 = 0
        unknown_sizes: list[int] = []
        for page in self.store.in_index_pages(REPLAY_PAGE_SIZE):
            for meta in page:
                mids.add(meta.mid)
                if meta.state in (
                    InboundQoSState.WAIT_PUBREL,
                    InboundQoSState.WAIT_USER_ACK,
                ):
                    session_state_qos2 += 1
                if meta.state is InboundQoSState.WAIT_PUBACK:
                    recovered_qos1.append(meta.mid)
                if meta.logical_size > 0:
                    pending_bytes += meta.logical_size
                else:
                    unknown_sizes.append(meta.mid)
        for mid in unknown_sizes:
            message = self.store.get_in(mid)
            if message is None:
                raise RuntimeError(
                    f"Inbound mid={mid} disappeared while restoring byte accounting"
                )
            size = self.logical_size(message.topic, message.payload, message.properties)
            if not self.store.set_in_logical_size(mid, size):
                raise RuntimeError(
                    f"Inbound mid={mid} disappeared while restoring byte accounting"
                )
            pending_bytes += size
        return mids, pending_bytes, session_state_qos2, tuple(recovered_qos1)

    # --- lifecycle ---------------------------------------------------------
'''
text = replace_between(
    text,
    "    def _load_recovered_state(self) -> tuple[set[int], int, int, tuple[int, ...]]:\n",
    "    # --- lifecycle ---------------------------------------------------------\n",
    load_state,
    label="metadata-only recovered state",
)
lookup_complete = '''    def _lookup_stored_inbound(self, mid: int) -> InboundRecordMeta | None:
        """Return persisted inbound metadata without probing an empty store."""
        if not self._stored_inbound:
            return None
        return self.store.in_meta(mid)

    def _remember_inbound(self) -> None:
        self._stored_inbound += 1

    def _forget_inbound(self) -> None:
        if self._stored_inbound <= 0:
            raise AssertionError("inbound stored-record count underflow")
        self._stored_inbound -= 1

    def _complete_stored_inbound(
        self, mid: int, expected_state: InboundQoSState, action: str
    ) -> int:
        """Conditionally delete one inbound record and return its logical size."""
        completed = self.store.complete_in(mid, expected_state)
        if completed is None:
            raise RuntimeError(f"Inbound mid={mid} changed while {action}")
        return completed.logical_size

    # --- packet handlers ---------------------------------------------------
'''
text = replace_between(
    text,
    "    def _lookup_stored_inbound(self, mid: int) -> InboundMessage | InboundRecordMeta | None:\n",
    "    # --- packet handlers ---------------------------------------------------\n",
    lookup_complete,
    label="inbound metadata lookup and completion",
)
# QoS2 only needs metadata for duplicate detection.
text = replace_exact(text, "        transitions = self._transitions\n", "", label="qos2 transitions local", count=5)
text = replace_exact(
    text,
    "        existing: InboundMessage | InboundRecordMeta | None = None\n",
    "        existing: InboundRecordMeta | None = None\n",
    label="qos2 metadata type",
)
text = replace_exact(
    text,
    '''            if transitions is not None:\n                existing = transitions.in_meta(mid)\n            else:\n                existing = store.get_in(mid)\n''',
    "            existing = store.in_meta(mid)\n",
    label="qos2 metadata lookup",
)
text = replace_exact(
    text,
    "                message = existing if isinstance(existing, InboundMessage) else store.get_in(mid)\n",
    "                message = store.get_in(mid)\n",
    label="manual qos1 duplicate materialization",
)
on_pubrel = '''    def on_pubrel(self, raw: RawPacket) -> None:
        engine = self._engine
        config = self.config
        mid, _reason_code, properties = self._decode_pubrel(raw.remaining)
        if properties is not None:
            engine._validate_inbound_problem_information(PacketType.PUBREL, properties)
        record = self._lookup_stored_inbound(mid)
        if record is None:
            if self._tiny_peer_packet_limit:
                self._raise_mandatory_response_too_large("PUBCOMP")
            engine._send_ack(_encode_pubcomp_success(mid))
            return
        state = record.state
        if state is InboundQoSState.WAIT_USER_ACK:
            if config.manual_ack:
                return
        elif state is not InboundQoSState.WAIT_PUBREL:
            raise ProtocolError(f"PUBREL for inbound mid={mid} in invalid state {state!r}")
        elif config.manual_ack and not record.user_acked:
            changed = self.store.transition_in(
                mid,
                InboundQoSState.WAIT_PUBREL,
                InboundQoSState.WAIT_USER_ACK,
            )
            if changed is None:
                raise RuntimeError(f"Inbound mid={mid} changed while processing PUBREL")
            return
        if self._tiny_peer_packet_limit:
            self._raise_mandatory_response_too_large("PUBCOMP")
        logical_size = self._complete_stored_inbound(mid, state, "completing PUBREL")
        self._forget_inbound()
        self._session_state_qos2 -= 1
        engine._send_ack(_encode_pubcomp_success(mid))
        self._release_slot(logical_size)

    # --- application acknowledgement and replay ---------------------------
'''
text = replace_between(
    text,
    "    def on_pubrel(self, raw: RawPacket) -> None:\n",
    "    # --- application acknowledgement and replay ---------------------------\n",
    on_pubrel,
    label="PUBREL single transition path",
)
mark_ack = '''    def mark_delivered(self, mid: int) -> None:
        if self._stored_inbound:
            self.store.mark_in_delivered(mid)

    def ack(self, mid: int) -> None:
        """Complete a deferred PUBACK or PUBCOMP in manual-ack mode."""
        if not self.config.manual_ack:
            raise ProtocolError("manual_ack is disabled")
        record = self._lookup_stored_inbound(mid)
        if record is None:
            raise ProtocolError(f"No pending inbound ack for mid={mid}")
        state = record.state
        if state is InboundQoSState.WAIT_PUBREL:
            changed = self.store.transition_in(mid, state, state, user_acked=True)
            if changed is None:
                raise ProtocolError(f"Inbound mid={mid} changed while acknowledging")
            return
        if state is InboundQoSState.WAIT_PUBACK:
            self._engine._check_outbound_size(_encode_puback_success(mid))
            self._pending_manual_qos1_acks.add(mid)
            self._drain_manual_qos1_acks()
            return
        if state is not InboundQoSState.WAIT_USER_ACK:
            raise ProtocolError(f"Inbound mid={mid} is not awaiting ack (state={state!r})")

        wire = _encode_pubcomp_success(mid)
        self._engine._check_outbound_size(wire)
        logical_size = self._complete_stored_inbound(mid, state, "acknowledging")
        self._forget_inbound()
        self._session_state_qos2 -= 1
        self._engine._send_ack(wire)
        self._release_slot(logical_size)

'''
text = replace_between(
    text,
    "    def mark_delivered(self, mid: int) -> None:\n",
    "    def _drain_manual_qos1_acks(self) -> None:\n",
    mark_ack,
    label="inbound delivery/ack single store path",
)
replay = '''    def replay_session(self) -> None:
        """Restore Receive Maximum accounting and start bounded redelivery."""
        persisted = self.store.in_count()
        self._inflight = persisted
        if persisted == 0:
            self._recovered_mids.clear()
            return
        pages = self.store.in_replay_pages(REPLAY_BATCH_MESSAGES, REPLAY_BATCH_BYTES)
        self._replay = InboundReplayCursor(iter(pages), remaining=persisted)
        self.drain_replay()

    def drain_replay(self) -> None:
        """Emit one bounded replay batch."""
        cursor = self._replay
        if cursor is None:
            return
        if not cursor.begin_page():
            self._replay = None
            self._recovered_mids.clear()
            return
        emitted = 0
        emitted_bytes = 0
        while emitted < REPLAY_BATCH_MESSAGES and emitted_bytes < REPLAY_BATCH_BYTES:
            inbound = cursor.next_page_message()
            if inbound is None:
                break
            if not self._should_redeliver(inbound):
                continue
            message_bytes = len(inbound.payload) + len(inbound.topic.encode("utf-8"))
            if emitted and emitted_bytes + message_bytes > REPLAY_BATCH_BYTES:
                cursor.push_back(inbound)
                break
            self._emit_message(inbound, dup=True)
            emitted += 1
            emitted_bytes += message_bytes
        if cursor.bounded_complete:
            self._replay = None
            self._recovered_mids.clear()
            return
        self._engine._emit(EffectKind.CONTINUE_INBOUND_REPLAY, None)

    def _should_redeliver(self, inbound: InboundMessage) -> bool:
        if not inbound.delivered:
            return True
        if inbound.mid not in self._recovered_mids or not self.config.manual_ack:
            return False
        if inbound.state in (
            InboundQoSState.WAIT_PUBACK,
            InboundQoSState.WAIT_USER_ACK,
        ):
            return True
        return inbound.state is InboundQoSState.WAIT_PUBREL and not inbound.user_acked

'''
text = replace_between(
    text,
    "    def replay_session(self) -> None:\n",
    "    def _emit_message(self, inbound: InboundMessage, *, dup: bool) -> None:\n",
    replay,
    label="bounded-only inbound replay",
)
legacy_terms = (
    "_transitions",
    "_paged_store",
    "_bounded_replay_store",
    "REPLAY_SCAN_LIMIT",
    "next_message(",
    "pop_in(",
    "update_in(",
    "in_items(",
)
for term in legacy_terms:
    if term in text:
        raise AssertionError(f"inbound legacy store term still present: {term}")
write(path, text)

print("PR434 persistence contract transformation completed")
