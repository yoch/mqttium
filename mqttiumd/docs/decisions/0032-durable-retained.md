# 0032 — Durable retained values and frozen replay

Status: **Proposed**

## Decision

A topic has at most one retained reference to an immutable publication version.
The reference owns one payload reference independently of sessions and live
recipients. It retains the bounded producer ClientID, so No Local remains valid
after session destruction. Replacing or clearing releases only that reference.
An empty retained publication clears the entry and is still distributed live.
Non-retained publications never mutate it. QoS2 mutates at admission, not PUBREL.
QoS0 retained uses a durable operation with a volatile origin and no MQTT Packet
Identifier, acknowledgement, exchange slot or Receive Maximum credit. Ordinary
QoS0 remains volatile. Retained QoS0 uses the common durable live obligations
(consumed before one QoS0 emission), including bounded offline responsibilities.
All mutations, live responsibilities and receipts commit
atomically; unknown outcomes retain the exact operation and canonical bytes.

Inventory: 32 exact topics, 1 MiB current retained bytes; existing 4 MiB payload,
128 reference and 8192 receipt quotas remain in force. Old values still owed to
recipients remain charged. Net replacement/clear is checked transactionally.

| Ordered subscription step | Replay decision |
| --- | --- |
| RH0, new or existing filter | Replay each matching eligible retained value |
| RH1, filter absent at this step | Replay, then the filter exists |
| RH1, filter already present | No replay |
| RH2 | No replay |
| Same filter repeated | Evaluate each occurrence against prior steps |
| Overlapping filters | One replay per eligible filter occurrence and value |
| No Local | Remove that contribution when producer ClientID equals target |
| RAP on replay | Replay always has RETAIN=1 |
| RAP on overlapping live matches | One delivery/session; OR RAP across eligible contributions |

The maximum transaction examines eight filters times 32 retained values (256).
It either materializes every selected replay and route change, or refuses the
whole update; no positive SUBACK with a truncated replay. The historical 16
limit counts destinations of one publication, not values replayed to one target.
Each replay gets its own never-reused publication identity, a frozen source
version, shared immutable payload and delivery. The owner reserves 256 causal
publication identities before submission; unused identities are burned. Receipt
frontiers close the entire reserved range with the operation. Work is then
paged and drained by the existing claim/Finish or consume-before-QoS0-emission
machines. Later retained mutations cannot rewrite admitted work.

Recipe tags 10 (retained QoS1/2), 11 (retained QoS0) and 12 (subscription plus
replay) extend format1. Tags1–9 retain their exact historical semantics and
bytes. Live RETAIN decisions are frozen in the new admission's target bitmap;
work pages and outgoing effects carry the frozen flag. SQLite schema8 explicitly
refuses historical schemas before mutation; no migration/reset is supplied.
Retained rows, causes, flags, producer fields and cross references are read under
the existing typed, pre-allocation budgets plus the inventory bounds.

## Scope

No Message Expiry, Will, shared subscriptions, Subscription Identifiers or
outgoing QoS2. Replay duplicates from distinct requested filter occurrences are
intentional; live overlap remains aggregated. Local QoS0 publish completion is
not evidence of commit or subscriber receipt. New receptions are system tests,
not an extension of the oracle's historical qualifying QoS2 scope.
