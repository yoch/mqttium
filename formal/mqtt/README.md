# MQTT inbound session ownership model

This directory contains the bounded TLA+ state-machine specification used to
review inbound QoS/session ownership around issues #505 and #506.

The model deliberately separates:

- durable MQTT Session State;
- QoS>0 PUBLISH ownership of the current Network Connection's Receive Maximum;
- manual QoS 1 acknowledgement order/intent;
- QoS 2 phase progression;
- connection liveness.

The executable differential-refinement campaign used during the investigation
also replayed generated traces against the exact CI-built `1.0.0rc15` wheel at
`main@5774db38b94615417f2c3c6106254429eb52b6f8`.

TLC was not executed in that investigation environment because `tla2tools.jar`
was unavailable. Do not treat the presence of this file as a TLC proof. The
model is included as an auditable specification and a future CI/tooling target;
the accompanying regression tests are executable today.
