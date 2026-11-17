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
MAX_RETAINED_REPLAYS = 256
MAX_RETAINED_TARGETS = 16
MAX_SESSION_CLIENT_BYTES = 1024


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
class ExpireDelivery:
    finish: Finish
    expiry: MessageExpiry
    observed: MessageTime

    kind = "ExpireDelivery"

    @property
    def operation(self) -> tuple:
        return self.finish.operation


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


def _retained_mask(mask: int, count: int) -> None:
    if count > MAX_RETAINED_TARGETS or mask >> count:
        raise RecipeError("invalid retain targets")


def _nested(reader: _Reader, allowed: tuple[int, ...]):
    # Inspect every child tag before calling decode. A temporal envelope may
    # contain one legacy envelope, whose own child is strictly a base recipe;
    # no attacker-controlled temporal recursion can consume Python stack space.
    nested = reader.take(reader.number(4))
    if len(nested) < 7 or nested[6] not in allowed:
        raise RecipeError("invalid retained nested kind")
    return decode(nested, _target_limit=MAX_RETAINED_TARGETS)


def decode(data: bytes, *, _target_limit: int | None = None):
    """The recipe ``data`` holds, or ``RecipeError``."""
    reader = _Reader(data)
    if reader.take(4) != MAGIC:
        raise RecipeError("bad magic")
    if reader.number(2) != FORMAT:
        raise RecipeError("unknown format")
    kind = reader.number(1)
    if kind == EXPIRING_PUBLICATION:
        admission = _nested(reader, (ADMIT_PUBLICATION, RETAINED_PUBLICATION, RETAINED_QOS0))
        if isinstance(admission, Admit):
            if admission.retain or admission.qos not in (1, 2):
                raise RecipeError("noncanonical expiring admission")
            if any(target.effective_qos > 2 or target.cause != 1 for target in admission.targets):
                raise RecipeError("invalid expiring admission cause")
        recipe = ExpiringPublication(admission, reader.message_expiry())
    elif kind == TIMED_REPLAY_SUBSCRIPTIONS:
        recipe = TimedReplaySubscriptions(_nested(reader, (REPLAY_SUBSCRIPTIONS,)), reader.message_time())
    elif kind == RETAINED_PUBLICATION:
        admission = _nested(reader, (ADMIT_PUBLICATION,))
        mask = reader.number(2)
        if admission.qos not in (1, 2) or not admission.retain:
            raise RecipeError("invalid retained admission")
        _retained_mask(mask, len(admission.targets))
        if any(target.effective_qos > 2 or target.cause != 1 for target in admission.targets):
            raise RecipeError("invalid retained target")
        recipe = RetainedPublication(admission, mask)
    elif kind == RETAINED_QOS0:
        operation, request, publication = reader.scoped(), reader.scoped(), reader.scoped()
        session, revision, connection, order, route_revision = (reader.number(8) for _ in range(5))
        topic = reader.take(reader.number(2))
        payload, length = reader.scoped(), reader.number(8)
        costs = tuple(reader.number(8) for _ in range(4))
        count = reader.number(1)
        if count > MAX_RETAINED_TARGETS:
            raise RecipeError("invalid retained target count")
        targets = tuple(Target(reader.number(8), reader.number(8), reader.number(4),
                               reader.number(1), reader.number(1)) for _ in range(count))
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
        operation, request, publication = reader.scoped(), reader.scoped(), reader.scoped()
        session, revision = reader.number(8), reader.number(8)
        packet_id, generation = reader.number(2), reader.number(8)
        route_revision = reader.number(8)
        qos, retain = reader.number(1), reader.number(1)
        topic = reader.take(reader.number(2))
        payload, length = reader.scoped(), reader.number(8)
        costs = tuple(reader.number(8) for _ in range(4))
        count = reader.number(2)
        if _target_limit is not None and count > _target_limit:
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
        recipe = Admit(
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
            if (subscribe not in ((0, 1) if kind == CHANGE_SUBSCRIPTIONS else (0, 1, 2))
                    or not valid_topic or options & 0xc3 or (options >> 4) & 3 == 3
                    or subscribe == 0 and options != 0):
                raise RecipeError("invalid subscription filter or options")
            changes.append(SubscriptionChange(topic, bool(subscribe)) if kind == CHANGE_SUBSCRIPTIONS
                           else GrantedSubscriptionChange(topic, None if subscribe == 0 else subscribe - 1, options))
        if kind == SUBSCRIPTION_OPTIONS and all(change.options == 0 for change in changes):
            raise RecipeError("noncanonical subscription options")
        cls = Subscriptions if kind == CHANGE_SUBSCRIPTIONS else UpdateSubscriptions
        recipe = cls(operation, session, revision, route_revision, tuple(changes))
    elif kind in (CLAIM_DELIVERY, FINISH_DELIVERY, MARK_DELIVERY_STARTED, EXPIRE_DELIVERY):
        fields = (reader.scoped(), reader.scoped(), reader.number(4),
                  reader.number(8), reader.number(8), reader.number(2))
        if fields[-1] == 0:
            raise RecipeError("invalid outbound packet identifier")
        if kind == CLAIM_DELIVERY:
            recipe = Claim(*fields)
        elif kind == MARK_DELIVERY_STARTED:
            recipe = MarkDeliveryStarted(*fields, reader.scoped())
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
        else:
            raise RecipeError("invalid session action")
    else:
        raise RecipeError("unknown kind")
    if reader.at != len(data):
        raise RecipeError("trailing bytes")
    return recipe
