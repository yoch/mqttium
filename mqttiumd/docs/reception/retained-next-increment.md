# Next functional increment: durable retained messages

The received preview has no retained store and advertises Retain Available0.
The next first executable result should publish a retained QoS1 value, disconnect
its producer, create a new wildcard subscriber, and observe one retained replay
from the same SQLite file after a broker restart.

The real integration points are the frozen AdmitPublication recipe and its
atomic payload/reference accounting, UpdateSubscriptions' known-commit routing
cut, the common hierarchical selector, and the existing QoS0 consumption/QoS1
claim delivery machines. A topic-keyed retained reference needs a distinct
lifetime from recipient obligations: replacing/clearing it must release only its
own payload reference. Subscription Retain Handling must choose replay at the
subscription decision, with No Local and overlap aggregation applied correctly;
RAP affects subsequent live delivery, while retained replay sets RETAIN as the
protocol requires. Already admitted obligations stay immutable.

Start with exact retained topic storage and reuse the matcher for replay
selection. Define bounded enumeration, capacity refusal, canonical operation
identity and restart before enabling the CONNACK capability. Empty retained
payload clearing, replacement, shared references and an interrupted commit are
its first discriminating cases. No general infrastructure or implementation is
introduced by this note.
