# 0034 — Durable zero-delay QoS1 Will

Status: Proposed

A successful MQTT5 CONNECT may install a non-retained QoS1 Will with delay
absent or zero. All other Will properties, including Message Expiry, are
explicitly unsupported in this increment. MQTT5 §§3.1.2.5–8, 3.1.3.2.2,
3.1.4, 3.14 and 4.1 define the network behavior.

## Authority and order

ClientID is a reusable name; SessionId, the opening OperationId and transport
generation are separate identities. The opening OperationId identifies the Will
occurrence. New kind8/action5 OpenWillAt atomically installs Armed state before
its known result can authorize CONNACK. None explicitly installs no Will.
Kind8/action6 EndWill conditionally cancels or makes that exact attachment
Pending and applies its existing session retention/cleanup. Historical action
subtags0–4 and recipe kinds1–16 keep their exact bytes and meaning.

| Event after accepted Open | Will decision |
| --- | --- |
| Normal DISCONNECT0 received | Durable cancellation |
| DISCONNECT4 | Pending publication |
| EOF, RST, transport failure, confirmed Keep Alive, protocol close | Pending publication |
| Refused or malformed CONNECT | No installed intention |
| Takeover, including Clean Start0 | Old occurrence Pending before new Open |
| Server failure or shutdown | Surviving Armed occurrences become Pending on restart |
| Late old Close/result | Exact attachment guard; no effect on its successor |

Normal cancellation is authoritative when its transaction commits. A crash
before that commit cannot recover an unrecorded packet receipt. Graceful broker
shutdown may defer Wills until restart, as permitted by §3.1.2.5. An accepted
Open whose CONNACK was not received can also survive as a Will intention.

## Admission independent of source lifetime

Pending intentions are independent of the source session. Schema10 preserves
its ClientID for No Local and its immutable bounded topic/payload. Startup and
takeover mark Armed intentions Pending before ephemeral session cleanup.
New recipe kind17 WillPublication fixes the route cut and target set, admits an
ordinary publication and references, consumes the exact Pending intention and
records WillPublished in one transaction. It has no fabricated inbound Packet
Identifier or QoS2 marker. Source cleanup cannot cancel target obligations.
An unknown command keeps the same OperationId and bytes; receipt retirement
cannot make a consumed intention executable again. A missing intention refuses
new admission. QoS1 wire retransmission is distinct from admission uniqueness.

## Capacity and progress

Existing session, message payload, reference, fanout and receipt quotas remain
unchanged. A separate reserve holds at most256 Will intentions and8MiB of Will
payload (at most32768 bytes each, topic at most1024 bytes), sufficient for128
session-associated slots plus128 pending occurrences. Product connections remain
limited to16. Full Will storage refuses a new Will CONNECT before success; no
existing intention is evicted. Temporary message-capacity or fanout failure
keeps Pending state, with observable deferral and bounded fair rediscovery.
One Will read/admission flight uses the existing control receipt reserve.
Maintenance reads one intention per pass; notifications only accelerate a
periodic bounded scan. There is no permanent row per historical client.
