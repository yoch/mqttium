"""The canonical bytes of a recipe (DEM-009), read independently.

The layout is a descriptive format shared with the product: ``MQDR``, a
format number, a kind, then fixed-width big-endian fields. The oracle decodes
it to learn which exchange an operation belongs to and which obligations it
names; it never asks the product what a recipe means.
"""

from __future__ import annotations

from dataclasses import dataclass

MAGIC = b"MQDR"
FORMAT = 1
ADMIT_PUBLICATION = 1
RELEASE_INBOUND = 2
CHANGE_SUBSCRIPTIONS = 3
CLAIM_DELIVERY = 4
FINISH_DELIVERY = 5
CONSUME_DELIVERY = 6
UPDATE_SUBSCRIPTIONS = 7
SESSION_LIFECYCLE = 8
SUBSCRIPTION_OPTIONS = 9
RETAINED_PUBLICATION = 10
RETAINED_QOS0 = 11
REPLAY_SUBSCRIPTIONS = 12
EXPIRING_PUBLICATION = 13
TIMED_REPLAY_SUBSCRIPTIONS = 14
MARK_DELIVERY_STARTED = 15
EXPIRE_DELIVERY = 16
WILL_PUBLICATION = 17
RETAINED_WILL_PUBLICATION = 18
PREPARE_PUBREL = 19
WIDE_RETAINED_PUBLICATION = 20
WIDE_RETAINED_QOS0 = 21
WIDE_RETAINED_WILL_PUBLICATION = 22
ANNOTATED_PUBLICATION = 23
CLAIMED_PUBLICATION = 24
MULTI_CLAIMED_PUBLICATION = 25
LEGACY_MAX_TARGETS = 16
MAX_RETAINED_REPLAYS = 32_768
MAX_RETAINED_TARGETS = 1_024
MAX_SESSION_CLIENT_BYTES = 1024
MAX_WILL_TOPIC_BYTES = 1024
MAX_WILL_PAYLOAD_BYTES = 32768
MAX_WILL_TARGETS = 1_024
MAX_USER_PROPERTIES = 64
MAX_PROPERTY_BYTES = 16_384
_FLAG_PAYLOAD_FORMAT = 0x01
_FLAG_CONTENT_TYPE = 0x02
_FLAG_RESPONSE_TOPIC = 0x04
_FLAG_CORRELATION_DATA = 0x08
_FLAG_USER_PROPERTIES = 0x10
_ALL_PROP_FLAGS = (
    _FLAG_PAYLOAD_FORMAT
    | _FLAG_CONTENT_TYPE
    | _FLAG_RESPONSE_TOPIC
    | _FLAG_CORRELATION_DATA
    | _FLAG_USER_PROPERTIES
)


class RecipeError(ValueError):
    """The bytes are not one canonical recipe."""


@dataclass(frozen=True)
class Target:
    session: int
    session_revision: int
    index: int
    effective_qos: int
    cause: int


@dataclass(frozen=True)
class Admit:
    operation: tuple
    request: tuple
    publication: tuple
    session: int
    session_revision: int
    packet_id: int
    exchange_generation: int
    route_revision: int
    qos: int
    retain: bool
    topic: bytes
    payload: tuple
    payload_length: int
    costs: tuple
    targets: tuple

    kind = "AdmitPublication"


@dataclass(frozen=True)
class InlineClaim:
    target_index: int
    packet_id: int


@dataclass(frozen=True)
class ClaimedPublication:
    """An admission and the claims of its QoS 1/2 targets made in the same
    commit: one claim is kind 24, two or more are kind 25."""

    admission: Admit
    claims: tuple

    @property
    def kind(self) -> str:
        return "ClaimedPublication" if len(self.claims) == 1 else "MultiClaimedPublication"

    @property
    def operation(self) -> tuple:
        return self.admission.operation


@dataclass(frozen=True)
class RetainedPublication:
    admission: Admit
    retain_targets: int

    kind = "RetainedPublication"

    @property
    def operation(self) -> tuple:
        return self.admission.operation


@dataclass(frozen=True)
class RetainedQos0:
    operation: tuple
    request: tuple
    publication: tuple
    session: int
    session_revision: int
    connection: int
    ingress_order: int
    route_revision: int
    topic: bytes
    payload: tuple
    payload_length: int
    costs: tuple
    targets: tuple
    retain_targets: int

    kind = "RetainedQos0"


@dataclass(frozen=True)
class WillPublication:
    operation: tuple
    will: tuple
    publication: tuple
    route_revision: int
    topic: bytes
    payload: tuple
    payload_length: int
    costs: tuple
    targets: tuple
    live_retain: int | None

    kind = "WillPublication"


@dataclass(frozen=True)
class Release:
    operation: tuple
    session: int
    packet_id: int
    exchange_generation: int

    kind = "ReleaseInbound"


@dataclass(frozen=True)
class SubscriptionChange:
    topic: bytes
    subscribe: bool


@dataclass(frozen=True)
class Subscriptions:
    operation: tuple
    session: int
    session_revision: int
    route_revision: int
    changes: tuple

    kind = "ChangeSubscriptions"


@dataclass(frozen=True)
class Claim:
    operation: tuple
    publication: tuple
    index: int
    target: int
    target_revision: int
    packet_id: int

    kind = "ClaimDelivery"


@dataclass(frozen=True)
class Finish(Claim):
    claim: tuple

    kind = "FinishDelivery"


@dataclass(frozen=True)
class Consume:
    operation: tuple
    publication: tuple
    index: int
    target: int
    target_revision: int

    kind = "ConsumeDelivery"


@dataclass(frozen=True)
class GrantedSubscriptionChange:
    topic: bytes
    granted: int | None
    options: int = 0


@dataclass(frozen=True)
class UpdateSubscriptions(Subscriptions):
    kind = "UpdateSubscriptions"


@dataclass(frozen=True)
class ReplaySubscriptions:
    update: UpdateSubscriptions
    first_publication: tuple

    kind = "ReplaySubscriptions"

    @property
    def operation(self) -> tuple:
        return self.update.operation

    @property
    def last_publication(self) -> tuple:
        return self.first_publication[:3] + (self.first_publication[3] + MAX_RETAINED_REPLAYS - 1,)


@dataclass(frozen=True)
class MessageTime:
    domain: bytes
    tick: int


@dataclass(frozen=True)
class MessageExpiry:
    observed: MessageTime
    interval: int

    @property
    def deadline(self) -> int:
        return self.observed.tick + self.interval * 1_000_000_000


@dataclass(frozen=True)
class ExpiringPublication:
    admission: Admit | RetainedPublication | RetainedQos0
    expiry: MessageExpiry

    kind = "ExpiringPublication"

    @property
    def operation(self) -> tuple:
        return self.admission.operation


@dataclass(frozen=True)
class TimedReplaySubscriptions:
    replay: ReplaySubscriptions
    observed: MessageTime

    kind = "TimedReplaySubscriptions"

    @property
    def operation(self) -> tuple:
        return self.replay.operation


@dataclass(frozen=True)
class MarkDeliveryStarted(Finish):
    kind = "MarkDeliveryStarted"


@dataclass(frozen=True)
class PreparePubrel(Finish):
    kind = "PreparePubrel"


@dataclass(frozen=True)
class ExpireDelivery:
    finish: Finish
    expiry: MessageExpiry
    observed: MessageTime

    kind = "ExpireDelivery"

    @property
    def operation(self) -> tuple:
        return self.finish.operation


@dataclass(frozen=True)
class PublishProperties:
    payload_format_indicator: int | None = None
    content_type: bytes | None = None
    response_topic: bytes | None = None
    correlation_data: bytes | None = None
    user_properties: tuple[tuple[bytes, bytes], ...] = ()


@dataclass(frozen=True)
class AnnotatedPublication:
    inner: Admit | RetainedPublication | RetainedQos0 | ExpiringPublication
    properties: PublishProperties

    kind = "AnnotatedPublication"

    @property
    def operation(self) -> tuple:
        return self.inner.operation


@dataclass(frozen=True)
class SessionOpen:
    operation: tuple
    clean_start: bool
    expiry: str
    expected: tuple | None
    client_id: bytes

    kind = "SessionLifecycle"
    action = "Open"


@dataclass(frozen=True)
class SessionClose:
    operation: tuple
    guard: tuple
    reason: str

    kind = "SessionLifecycle"
    action = "Close"


@dataclass(frozen=True)
class SessionExpiryAfter:
    seconds: int


@dataclass(frozen=True)
class SessionOpenAt:
    operation: tuple
    clean_start: bool
    expiry: str | SessionExpiryAfter
    expected: tuple | None
    client_id: bytes
    now: int

    kind = "SessionLifecycle"
    action = "OpenAt"


@dataclass(frozen=True)
class WillSpec:
    topic: bytes
    payload: bytes
    retain: bool


@dataclass(frozen=True)
class SessionOpenWillAt(SessionOpenAt):
    will: WillSpec | None

    action = "OpenWillAt"


@dataclass(frozen=True)
class SessionEndWill:
    operation: tuple
    guard: tuple
    expiry: str | SessionExpiryAfter
    observed_at: int
    publish: bool

    kind = "SessionLifecycle"
    action = "EndWill"


@dataclass(frozen=True)
class SessionDetach:
    operation: tuple
    guard: tuple
    expiry: str | SessionExpiryAfter
    observed_at: int

    kind = "SessionLifecycle"
    action = "Detach"


@dataclass(frozen=True)
class SessionExpire:
    operation: tuple
    guard: tuple
    detached_operation: tuple
    deadline: int | None
    now: int

    kind = "SessionLifecycle"
    action = "Expire"


class _Reader:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.at = 0

    def take(self, count: int) -> bytes:
        if self.at + count > len(self.data):
            raise RecipeError("truncated")
        chunk = self.data[self.at : self.at + count]
        self.at += count
        return chunk

    def number(self, width: int) -> int:
        return int.from_bytes(self.take(width), "big")

    def scoped(self) -> tuple:
        return tuple(self.number(8) for _ in range(4))

    def boolean(self) -> bool:
        value = self.number(1)
        if value not in (0, 1):
            raise RecipeError("invalid boolean")
        return bool(value)

    def session_guard(self) -> tuple:
        session = self.number(8)
        return session, self.scoped() if self.boolean() else None

    def session_expiry(self) -> str | SessionExpiryAfter:
        tag = self.number(1)
        if tag in (0, 1):
            return ("OnDisconnect", "Never")[tag]
        if tag == 2:
            seconds = self.number(4)
            if 1 <= seconds <= 0xFFFF_FFFE:
                return SessionExpiryAfter(seconds)
        raise RecipeError("invalid session expiry")

    def client_id(self) -> bytes:
        length = self.number(2)
        if not 1 <= length <= MAX_SESSION_CLIENT_BYTES:
            raise RecipeError("invalid ClientID length")
        client = self.take(length)
        try:
            client.decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:
            raise RecipeError("invalid ClientID") from error
        if b"\x00" in client:
            raise RecipeError("invalid ClientID")
        return client

    def message_time(self) -> MessageTime:
        return MessageTime(self.take(32), self.number(8))

    def message_expiry(self) -> MessageExpiry:
        value = MessageExpiry(self.message_time(), self.number(4))
        if value.deadline > 0xFFFF_FFFF_FFFF_FFFF:
            raise RecipeError("message expiry deadline overflow")
        return value

    def will_spec(self, retain: bool) -> WillSpec:
        length = self.number(2)
        if not 1 <= length <= MAX_WILL_TOPIC_BYTES:
            raise RecipeError("invalid Will topic length")
        topic = self.take(length)
        if not _valid_topic(topic):
            raise RecipeError("invalid Will topic")
        length = self.number(4)
        if length > MAX_WILL_PAYLOAD_BYTES:
            raise RecipeError("invalid Will payload length")
        return WillSpec(topic, self.take(length), retain)


def _valid_filter(topic: bytes) -> bool:
    # This independent format reader validates grammar, never computes fanout.
    if not topic or len(topic) > 65535 or b"\x00" in topic or topic.startswith(b"$share/"):
        return False
    try:
        topic.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return False
    levels = topic.split(b"/")
    return all((b"#" not in level or level == b"#" and index == len(levels) - 1)
               and (b"+" not in level or level == b"+")
               for index, level in enumerate(levels))


def _valid_topic(topic: bytes) -> bool:
    if not topic or len(topic) > 65535 or any(byte in topic for byte in (0, 35, 43)):
        return False
    try:
        topic.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return False
    return True


def _valid_mqtt_utf8(raw: bytes) -> bool:
    if len(raw) > 65535:
        return False
    try:
        text = raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return False
    for ch in text:
        code = ord(ch)
        if (
            code <= 0x1F
            or 0x7F <= code <= 0x9F
            or 0xFDD0 <= code <= 0xFDEF
            or (code & 0xFFFE) == 0xFFFE
        ):
            return False
    return True


def _valid_response_topic(raw: bytes) -> bool:
    return bool(raw) and _valid_mqtt_utf8(raw) and not any(b in (35, 43) for b in raw)


def _decode_publish_properties(raw: bytes) -> PublishProperties:
    reader = _Reader(raw)
    flags = reader.number(1)
    if flags == 0 or (flags & ~_ALL_PROP_FLAGS):
        raise RecipeError("invalid publish properties flags")
    pfi = None
    if flags & _FLAG_PAYLOAD_FORMAT:
        pfi = reader.number(1)
        if pfi > 1:
            raise RecipeError("invalid payload format indicator")
    content_type = None
    if flags & _FLAG_CONTENT_TYPE:
        length = reader.number(2)
        if length > MAX_PROPERTY_BYTES:
            raise RecipeError("publish properties too large")
        content_type = reader.take(length)
        if not _valid_mqtt_utf8(content_type):
            raise RecipeError("invalid content type")
    response_topic = None
    if flags & _FLAG_RESPONSE_TOPIC:
        length = reader.number(2)
        if length > MAX_PROPERTY_BYTES:
            raise RecipeError("publish properties too large")
        response_topic = reader.take(length)
        if not _valid_response_topic(response_topic):
            raise RecipeError("invalid response topic")
    correlation_data = None
    if flags & _FLAG_CORRELATION_DATA:
        length = reader.number(2)
        if length > MAX_PROPERTY_BYTES:
            raise RecipeError("publish properties too large")
        correlation_data = reader.take(length)
    user_properties = []
    if flags & _FLAG_USER_PROPERTIES:
        count = reader.number(2)
        if not 1 <= count <= MAX_USER_PROPERTIES:
            raise RecipeError("invalid user properties count")
        for _ in range(count):
            klen = reader.number(2)
            if klen > MAX_PROPERTY_BYTES:
                raise RecipeError("publish properties too large")
            key = reader.take(klen)
            vlen = reader.number(2)
            if vlen > MAX_PROPERTY_BYTES:
                raise RecipeError("publish properties too large")
            val = reader.take(vlen)
            if not _valid_mqtt_utf8(key) or not _valid_mqtt_utf8(val):
                raise RecipeError("invalid user property")
            user_properties.append((key, val))
    if reader.at != len(reader.data):
        raise RecipeError("trailing bytes in publish properties")
    logical_bytes = (
        (1 if pfi is not None else 0)
        + (len(content_type) if content_type is not None else 0)
        + (len(response_topic) if response_topic is not None else 0)
        + (len(correlation_data) if correlation_data is not None else 0)
        + sum(len(k) + len(v) for k, v in user_properties)
    )
    if logical_bytes > MAX_PROPERTY_BYTES:
        raise RecipeError("publish properties too large")
    return PublishProperties(
        payload_format_indicator=pfi,
        content_type=content_type,
        response_topic=response_topic,
        correlation_data=correlation_data,
        user_properties=tuple(user_properties),
    )


def _retained_mask(mask: int, count: int) -> None:
    if count > LEGACY_MAX_TARGETS or mask >> count:
        raise RecipeError("invalid retain targets")


def _decode_wide_mask(reader: _Reader, count: int) -> int:
    expected = (count + 7) // 8
    byte_len = reader.number(2)
    if byte_len != expected:
        raise RecipeError("invalid wide retain mask length")
    raw = reader.take(byte_len)
    rem = count % 8
    if rem != 0 and raw and (raw[-1] >> rem) != 0:
        raise RecipeError("invalid wide retain mask trailing bits")
    return int.from_bytes(raw, "little")


def _nested(reader: _Reader, allowed: tuple[int, ...], *, target_limit: int = MAX_RETAINED_TARGETS):
    # Inspect every child tag before calling decode. An annotated or temporal
    # envelope may contain strictly permitted inner kinds; no attacker-controlled
    # recursion can consume Python stack space.
    nested = reader.take(reader.number(4))
    if len(nested) < 7 or nested[6] not in allowed:
        raise RecipeError("invalid retained nested kind")
    return decode(nested, _target_limit=target_limit)


def _admission(reader: _Reader, target_limit: int | None) -> Admit:
    """The body of an admission, shared by kinds 1, 24 and 25."""
    operation, request, publication = reader.scoped(), reader.scoped(), reader.scoped()
    session, revision = reader.number(8), reader.number(8)
    packet_id, generation = reader.number(2), reader.number(8)
    route_revision = reader.number(8)
    qos, retain = reader.number(1), reader.number(1)
    topic = reader.take(reader.number(2))
    payload, length = reader.scoped(), reader.number(8)
    costs = tuple(reader.number(8) for _ in range(4))
    count = reader.number(2)
    if target_limit is not None and count > target_limit:
        raise RecipeError("invalid retained target count")
    targets = tuple(
        Target(
            reader.number(8),
            reader.number(8),
            reader.number(4),
            reader.number(1),
            reader.number(1),
        )
        for _ in range(count)
    )
    if packet_id == 0 or qos > 2 or retain > 1:
        raise RecipeError("invalid field")
    return Admit(
        operation,
        request,
        publication,
        session,
        revision,
        packet_id,
        generation,
        route_revision,
        qos,
        bool(retain),
        topic,
        payload,
        length,
        costs,
        targets,
    )


def _checked_claims(admission: Admit, claims: tuple, kind: int) -> tuple:
    """The claims of a claimed publication, as the contract accepts them:
    no retained admission, existing QoS 1/2 targets, and for kind 25 two
    claims or more, in ascending order of target, one for each session."""
    if admission.retain:
        raise RecipeError("inline claim on a retained admission")
    if kind == MULTI_CLAIMED_PUBLICATION and not 2 <= len(claims) <= len(admission.targets):
        raise RecipeError("invalid inline claim count")
    sessions = set()
    previous = -1
    for claim in claims:
        if claim.target_index <= previous or claim.target_index >= len(admission.targets):
            raise RecipeError("invalid inline claim target")
        previous = claim.target_index
        target = admission.targets[claim.target_index]
        if target.effective_qos == 0 or claim.packet_id == 0 or target.session in sessions:
            raise RecipeError("invalid inline claim")
        sessions.add(target.session)
    return claims


def decode(data: bytes, *, _target_limit: int | None = None):
    """The recipe ``data`` holds, or ``RecipeError``."""
    reader = _Reader(data)
    if reader.take(4) != MAGIC:
        raise RecipeError("bad magic")
    if reader.number(2) != FORMAT:
        raise RecipeError("unknown format")
    kind = reader.number(1)
    if kind == ANNOTATED_PUBLICATION:
        props_len = reader.number(4)
        props = _decode_publish_properties(reader.take(props_len))
        inner = _nested(
            reader,
            (
                ADMIT_PUBLICATION,
                RETAINED_PUBLICATION,
                RETAINED_QOS0,
                EXPIRING_PUBLICATION,
                WIDE_RETAINED_PUBLICATION,
                WIDE_RETAINED_QOS0,
                CLAIMED_PUBLICATION,
                MULTI_CLAIMED_PUBLICATION,
            ),
            target_limit=MAX_RETAINED_TARGETS,
        )
        recipe = AnnotatedPublication(inner, props)
    elif kind in (WILL_PUBLICATION, RETAINED_WILL_PUBLICATION, WIDE_RETAINED_WILL_PUBLICATION):
        wide = kind == WIDE_RETAINED_WILL_PUBLICATION
        operation, will, publication = reader.scoped(), reader.scoped(), reader.scoped()
        route_revision = reader.number(8)
        topic_length = reader.number(2)
        if not 1 <= topic_length <= MAX_WILL_TOPIC_BYTES:
            raise RecipeError("invalid Will topic length")
        topic = reader.take(topic_length)
        payload, length = reader.scoped(), reader.number(8)
        costs = tuple(reader.number(8) for _ in range(4))
        count = reader.number(2) if wide else reader.number(1)
        if wide:
            if not (LEGACY_MAX_TARGETS < count <= MAX_WILL_TARGETS):
                raise RecipeError("invalid wide Will target count")
        elif count > LEGACY_MAX_TARGETS:
            raise RecipeError("invalid Will target count")
        targets = tuple(Target(reader.number(8), reader.number(8), reader.number(4),
                               reader.number(1), reader.number(1)) for _ in range(count))
        if (not _valid_topic(topic) or length > MAX_WILL_PAYLOAD_BYTES
                or any(target.index != index or target.effective_qos not in (0, 1)
                       or target.cause != 1 for index, target in enumerate(targets))
                or any(left.session >= right.session for left, right in zip(targets, targets[1:]))):
            raise RecipeError("invalid Will publication field")
        if wide:
            live_retain = _decode_wide_mask(reader, count) if reader.boolean() else None
        elif kind == RETAINED_WILL_PUBLICATION:
            live_retain = reader.number(2)
            if live_retain >> count:
                raise RecipeError("invalid Will live RETAIN mask")
        else:
            live_retain = None
        recipe = WillPublication(operation, will, publication, route_revision, topic,
                                 payload, length, costs, targets, live_retain)
    elif kind == EXPIRING_PUBLICATION:
        admission = _nested(
            reader,
            (
                ADMIT_PUBLICATION,
                RETAINED_PUBLICATION,
                RETAINED_QOS0,
                WIDE_RETAINED_PUBLICATION,
                WIDE_RETAINED_QOS0,
            ),
            target_limit=MAX_RETAINED_TARGETS,
        )
        if isinstance(admission, Admit):
            if admission.retain or admission.qos not in (1, 2):
                raise RecipeError("noncanonical expiring admission")
            if any(target.effective_qos > 2 or target.cause != 1 for target in admission.targets):
                raise RecipeError("invalid expiring admission cause")
        recipe = ExpiringPublication(admission, reader.message_expiry())
    elif kind == TIMED_REPLAY_SUBSCRIPTIONS:
        recipe = TimedReplaySubscriptions(_nested(reader, (REPLAY_SUBSCRIPTIONS,)), reader.message_time())
    elif kind in (RETAINED_PUBLICATION, WIDE_RETAINED_PUBLICATION):
        wide = kind == WIDE_RETAINED_PUBLICATION
        admission = _nested(
            reader,
            (ADMIT_PUBLICATION,),
            target_limit=MAX_RETAINED_TARGETS if wide else LEGACY_MAX_TARGETS,
        )
        if wide and len(admission.targets) <= LEGACY_MAX_TARGETS:
            raise RecipeError("noncanonical wide targets")
        if admission.qos not in (1, 2) or not admission.retain:
            raise RecipeError("invalid retained admission")
        if wide:
            mask = _decode_wide_mask(reader, len(admission.targets))
        else:
            mask = reader.number(2)
            _retained_mask(mask, len(admission.targets))
        if any(target.effective_qos > 2 or target.cause != 1 for target in admission.targets):
            raise RecipeError("invalid retained target")
        recipe = RetainedPublication(admission, mask)
    elif kind in (RETAINED_QOS0, WIDE_RETAINED_QOS0):
        wide = kind == WIDE_RETAINED_QOS0
        operation, request, publication = reader.scoped(), reader.scoped(), reader.scoped()
        session, revision, connection, order, route_revision = (reader.number(8) for _ in range(5))
        topic = reader.take(reader.number(2))
        payload, length = reader.scoped(), reader.number(8)
        costs = tuple(reader.number(8) for _ in range(4))
        count = reader.number(2) if wide else reader.number(1)
        if wide:
            if not (LEGACY_MAX_TARGETS < count <= MAX_RETAINED_TARGETS):
                raise RecipeError("invalid wide retained target count")
        elif count > LEGACY_MAX_TARGETS:
            raise RecipeError("invalid retained target count")
        targets = tuple(Target(reader.number(8), reader.number(8), reader.number(4),
                               reader.number(1), reader.number(1)) for _ in range(count))
        if wide:
            mask = _decode_wide_mask(reader, count)
        else:
            mask = reader.number(2)
            _retained_mask(mask, count)
        if (not _valid_topic(topic)
                or order == 0 or any(target.effective_qos > 2 or target.cause != 1 for target in targets)):
            raise RecipeError("invalid retained QoS0 field")
        recipe = RetainedQos0(operation, request, publication, session, revision, connection,
                              order, route_revision, topic, payload, length, costs, targets, mask)
    elif kind == REPLAY_SUBSCRIPTIONS:
        update = _nested(reader, (UPDATE_SUBSCRIPTIONS, SUBSCRIPTION_OPTIONS))
        first = reader.scoped()
        if (first[:3] != update.operation[:3] or first[3] == 0
                or first[3] + MAX_RETAINED_REPLAYS - 1 > 0xFFFF_FFFF_FFFF_FFFF):
            raise RecipeError("invalid replay identity range")
        recipe = ReplaySubscriptions(update, first)
    elif kind == ADMIT_PUBLICATION:
        recipe = _admission(reader, _target_limit)
    elif kind in (CLAIMED_PUBLICATION, MULTI_CLAIMED_PUBLICATION):
        admission = _admission(reader, _target_limit)
        count = 1 if kind == CLAIMED_PUBLICATION else reader.number(2)
        claims = tuple(InlineClaim(reader.number(2), reader.number(2)) for _ in range(count))
        recipe = ClaimedPublication(admission, _checked_claims(admission, claims, kind))
    elif kind == RELEASE_INBOUND:
        recipe = Release(reader.scoped(), reader.number(8), reader.number(2), reader.number(8))
        if recipe.packet_id == 0:
            raise RecipeError("invalid field")
    elif kind in (CHANGE_SUBSCRIPTIONS, UPDATE_SUBSCRIPTIONS, SUBSCRIPTION_OPTIONS):
        operation = reader.scoped()
        session, revision, route_revision = (reader.number(8) for _ in range(3))
        count = reader.number(1)
        if not 1 <= count <= 8:
            raise RecipeError("invalid subscription count")
        changes = []
        for _ in range(count):
            subscribe = reader.number(1)
            options = reader.number(1) if kind == SUBSCRIPTION_OPTIONS else 0
            topic = reader.take(reader.number(2))
            valid_topic = _valid_filter(topic)
            if kind == CHANGE_SUBSCRIPTIONS:
                # Keep legacy kind3's exact-topic meaning and representation,
                # including literal historical $share/ names (not shared QoS).
                try:
                    topic.decode("utf-8", errors="strict")
                except UnicodeDecodeError:
                    valid_topic = False
                else:
                    valid_topic = bool(topic) and not any(byte in topic for byte in (0, 35, 43))
            if (subscribe not in ((0, 1) if kind == CHANGE_SUBSCRIPTIONS else (0, 1, 2, 3))
                    or not valid_topic or options & 0xc3 or (options >> 4) & 3 == 3
                    or subscribe == 0 and options != 0):
                raise RecipeError("invalid subscription filter or options")
            changes.append(SubscriptionChange(topic, bool(subscribe)) if kind == CHANGE_SUBSCRIPTIONS
                           else GrantedSubscriptionChange(topic, None if subscribe == 0 else subscribe - 1, options))
        if kind == SUBSCRIPTION_OPTIONS and all(change.options == 0 for change in changes):
            raise RecipeError("noncanonical subscription options")
        cls = Subscriptions if kind == CHANGE_SUBSCRIPTIONS else UpdateSubscriptions
        recipe = cls(operation, session, revision, route_revision, tuple(changes))
    elif kind in (CLAIM_DELIVERY, FINISH_DELIVERY, MARK_DELIVERY_STARTED, EXPIRE_DELIVERY, PREPARE_PUBREL):
        fields = (reader.scoped(), reader.scoped(), reader.number(4),
                  reader.number(8), reader.number(8), reader.number(2))
        if fields[-1] == 0:
            raise RecipeError("invalid outbound packet identifier")
        if kind == CLAIM_DELIVERY:
            recipe = Claim(*fields)
        elif kind == MARK_DELIVERY_STARTED:
            recipe = MarkDeliveryStarted(*fields, reader.scoped())
        elif kind == PREPARE_PUBREL:
            recipe = PreparePubrel(*fields, reader.scoped())
        else:
            finish = Finish(*fields, reader.scoped())
            if kind == FINISH_DELIVERY:
                recipe = finish
            else:
                expiry, observed = reader.message_expiry(), reader.message_time()
                if observed.domain != expiry.observed.domain:
                    raise RecipeError("message time is uncertain")
                if observed.tick < expiry.deadline:
                    raise RecipeError("message is not established expired")
                recipe = ExpireDelivery(finish, expiry, observed)
    elif kind == CONSUME_DELIVERY:
        recipe = Consume(reader.scoped(), reader.scoped(), reader.number(4),
                         reader.number(8), reader.number(8))
    elif kind == SESSION_LIFECYCLE:
        operation, action = reader.scoped(), reader.number(1)
        if action == 0:
            clean_start = reader.boolean()
            expiry = reader.number(1)
            if expiry not in (0, 1):
                raise RecipeError("invalid session expiry")
            expected = reader.session_guard() if reader.boolean() else None
            length = reader.number(2)
            if not 1 <= length <= MAX_SESSION_CLIENT_BYTES:
                raise RecipeError("invalid ClientID length")
            client = reader.take(length)
            try:
                client.decode("utf-8", errors="strict")
            except UnicodeDecodeError as error:
                raise RecipeError("invalid ClientID") from error
            if b"\x00" in client:
                raise RecipeError("invalid ClientID")
            recipe = SessionOpen(operation, clean_start, ("OnDisconnect", "Never")[expiry],
                                 expected, client)
        elif action == 1:
            guard = reader.session_guard()
            reason = reader.number(1)
            if reason > 3:
                raise RecipeError("invalid session end reason")
            recipe = SessionClose(operation, guard,
                                  ("Disconnect", "TransportLost", "Replaced", "Startup")[reason])
        elif action == 2:
            clean_start = reader.boolean()
            expiry = reader.session_expiry()
            expected = reader.session_guard() if reader.boolean() else None
            client = reader.client_id()
            recipe = SessionOpenAt(operation, clean_start, expiry, expected, client, reader.number(8))
        elif action == 3:
            guard = reader.session_guard()
            expiry = reader.session_expiry()
            observed_at = reader.number(8)
            if isinstance(expiry, SessionExpiryAfter) and observed_at + expiry.seconds * 1_000_000_000 > 0xFFFF_FFFF_FFFF_FFFF:
                raise RecipeError("invalid session deadline")
            recipe = SessionDetach(operation, guard, expiry, observed_at)
        elif action == 4:
            guard = reader.session_guard()
            detached_operation = reader.scoped()
            deadline = reader.number(8) if reader.boolean() else None
            recipe = SessionExpire(operation, guard, detached_operation, deadline, reader.number(8))
        elif action in (5, 7):
            clean_start = reader.boolean()
            expiry = reader.session_expiry()
            expected = reader.session_guard() if reader.boolean() else None
            client = reader.client_id()
            now = reader.number(8)
            will = reader.will_spec(action == 7) if reader.boolean() else None
            if action == 7 and will is None:
                raise RecipeError("retained Will descriptor is required")
            recipe = SessionOpenWillAt(operation, clean_start, expiry, expected, client, now, will)
        elif action == 6:
            guard = reader.session_guard()
            expiry = reader.session_expiry()
            observed_at = reader.number(8)
            if isinstance(expiry, SessionExpiryAfter) and observed_at + expiry.seconds * 1_000_000_000 > 0xFFFF_FFFF_FFFF_FFFF:
                raise RecipeError("invalid session deadline")
            recipe = SessionEndWill(operation, guard, expiry, observed_at, reader.boolean())
        else:
            raise RecipeError("invalid session action")
    else:
        raise RecipeError("unknown kind")
    if reader.at != len(data):
        raise RecipeError("trailing bytes")
    return recipe
