# Targeted private-domain security reception

The actual extracted normal executable was source 51be4079, binary
`9ef097977c2cb86669e11e98d7fbceeac87a4f86fd6a24c7deb76ea9c31bf119`.
The six security groups passed locally and in the frozen candidate's CI. Public
mqttium 1.3.0, Paho 2.1.0 and Mosquitto 2.0.21 clients were exercised by the named
receivers and packaged demonstrations. The long traffic generator used Paho.

| Control | Observed outcome |
| --- | --- |
|TLS 1.3, server chain and client certificate|Successful public-client connections with verified chains and allowed leaf pins|
|No certificate, unknown CA, expired certificate, unknown leaf pin|Refused before an accepted MQTT session|
|Foreign ClientID, hostile Clean Start and takeover|MQTT 0x87, legitimate session continued and no parasitic authority was created|
|Forbidden QoS 0/1/2 publish, retained publish, wildcard filter and Will topic|Refused, targeted durable business state preserved|
|Credential rotation within the same principal|Session Present 1 after SIGKILL/restart with the newly authorized leaf|
|Authorization change without offline transition|Startup refused by the recorded policy fingerprint|
|Eight incomplete handshakes and transport capacity|Bounded expiry/refusal while an independent established client progressed|
|TLS close_notify without MQTT DISCONNECT|Abnormal MQTT loss, with the supported Will behavior|
|Ordinary MQTT DISCONNECT|Will cancellation. Healthy long-run eight cancellations produced zero Wills|

The independent trust review traced the actual certificate verifier, durable
ClientID ownership, ACL containment and offline authority transaction. It found
no confirmed cross-principal admission path in its stated coverage. Its separate
runtime race findings were tested and corrected. The retained-presence packaged
demo passed online→sensor SIGKILL→offline retained→late dashboard, then broker
SIGKILL→same replay→new online and ordinary Will cancellation. Real killed PIDs
and their wait statuses are in the receipts.

The security source harness needs Git to identify its imported client source.
An initial run under the deliberately restricted runtime PATH completed startup
and certificate checks, then reported that missing provenance dependency. It is
recorded as a harness failure. Its completed rerun used the full tool PATH.
The actual downloaded executable, long load and packaged presence demo executed
with Git and Cargo absent from PATH. The operator receptions do not rebuild it.

The domain remains statically configured TLS 1.3 with mandatory client
certificates and principal/topic grants. Runtime configuration loading, session
ownership and authorizations have fixed bounds. Certificate and ACL changes
require their stated offline/restart procedures. Demo keys are generated locally
and excluded from artifacts. Logs and receipts use synthetic device identities
and test message data. Private signing and device keys are kept outside evidence.

Username/password authentication, dynamic revocation, hot reload, remote
administration and full multi-tenant policy are open limits of this profile.
The reference did not establish CLOCK_BOOTTIME domain continuity. Message Expiry
keeps quarantine-on-unknown-origin, and the long traffic used no message TTL.
These results establish a bounded targeted reception on the named machine.
