# ADR 0041 — Durable outgoing QoS 2

Status: **Proposed**

On both protocol levels an authorized SUBSCRIBE grants its requested QoS.
The effective QoS is the smaller of the subscription and publication QoS.
Requests consume route capacity in packet order. A refused filter leaves
capacity and existing routes available to the next filter. A successful
SUBACK remains fenced by the subscription transaction.

Under ACK-004 a claim persists the exact delivery and Packet Identifier
before PUBLISH. A positive PUBREC prepares PUBREL in a separate guarded
transaction for that same claim. Only its known commit authorizes PUBREL.
The claim, obligation and window entry remain occupied through this
transition and through an unknown finish. After a known finish, the
Packet Identifier can be allocated to another delivery.

Recovery sends PUBLISH with DUP=1 for an unfinished first leg and sends
PUBREL with fixed header 0x62 for the second leg. Both use the original
Packet Identifier. MQTT 5 PUBREC with a negative reason finishes the
delivery without PUBREL. PUBCOMP finishes the second leg. The adapter
captures the exact queued occurrence and connection on acknowledgement.
Repeated positive PUBREC can repeat the already justified PUBREL.
Message expiry can prevent a first transmission. It cannot cancel a
delivery after definitive transmission progress or suppress its second leg.

Existing SQLite rows and schemas 12 and 13 remain readable by this
candidate. The obligation phase integer 2 now identifies AwaitPubcomp for
QoS 2 with a live claim. Integer 1 keeps its previous Pending meaning.
Prepared PUBREL uses canonical recipe tag 19 and stored outcome tag 13.
The existing subscription formats add grant discriminator 3 for QoS 2.
The x86_64 and AArch64 preview receipts cover both protocol levels with
actual process SIGKILL. The previous binaries are unsuitable for opening
a database after enabling outgoing QoS 2.
