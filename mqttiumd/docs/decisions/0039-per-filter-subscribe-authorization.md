# ADR 0039 — Per-filter SUBSCRIBE authorization

Status: **Proposed**

The secure adapter authorizes each decoded SUBSCRIBE filter independently.
The result is a bounded denial mask indexed in request order. The owner
includes only granted filters in its route and retained-replay transaction.
It preserves each denied position in the result, including when storage
rejects the allowed changes.

SUBACK reports Not Authorized (0x87) for a denied MQTT 5 filter and Failure
(0x80) for a denied MQTT 3.1.1 filter. An entirely denied SUBSCRIBE has no
route transaction. Granted positions become eligible for acknowledgement
through the existing commit fence. The durable representation of a route
and the stored recipes keep their existing formats.

The packaged reception exercises both orders of mixed authorization,
individual filters, positive SUBACK ordering, delivery and session recovery.
