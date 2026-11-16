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


def decode(data: bytes):
    """The recipe ``data`` holds, or ``RecipeError``."""
    reader = _Reader(data)
    if reader.take(4) != MAGIC:
        raise RecipeError("bad magic")
    if reader.number(2) != FORMAT:
        raise RecipeError("unknown format")
    kind = reader.number(1)
    if kind == ADMIT_PUBLICATION:
        operation, request, publication = reader.scoped(), reader.scoped(), reader.scoped()
        session, revision = reader.number(8), reader.number(8)
        packet_id, generation = reader.number(2), reader.number(8)
        route_revision = reader.number(8)
        qos, retain = reader.number(1), reader.number(1)
        topic = reader.take(reader.number(2))
        payload, length = reader.scoped(), reader.number(8)
        costs = tuple(reader.number(8) for _ in range(4))
        targets = tuple(
            Target(
                reader.number(8),
                reader.number(8),
                reader.number(4),
                reader.number(1),
                reader.number(1),
            )
            for _ in range(reader.number(2))
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
    elif kind == CHANGE_SUBSCRIPTIONS:
        operation = reader.scoped()
        session, revision, route_revision = (reader.number(8) for _ in range(3))
        count = reader.number(1)
        if not 1 <= count <= 8:
            raise RecipeError("invalid subscription count")
        changes = []
        for _ in range(count):
            subscribe = reader.number(1)
            topic = reader.take(reader.number(2))
            try:
                topic.decode("utf-8", errors="strict")
            except UnicodeDecodeError as error:
                raise RecipeError("invalid exact filter") from error
            if subscribe not in (0, 1) or not topic or any(byte in topic for byte in (0, 35, 43)):
                raise RecipeError("invalid exact filter")
            changes.append(SubscriptionChange(topic, bool(subscribe)))
        recipe = Subscriptions(operation, session, revision, route_revision, tuple(changes))
    elif kind in (CLAIM_DELIVERY, FINISH_DELIVERY):
        fields = (reader.scoped(), reader.scoped(), reader.number(4),
                  reader.number(8), reader.number(8), reader.number(2))
        if fields[-1] == 0:
            raise RecipeError("invalid outbound packet identifier")
        recipe = Claim(*fields) if kind == CLAIM_DELIVERY else Finish(*fields, reader.scoped())
    else:
        raise RecipeError("unknown kind")
    if reader.at != len(data):
        raise RecipeError("trailing bytes")
    return recipe
