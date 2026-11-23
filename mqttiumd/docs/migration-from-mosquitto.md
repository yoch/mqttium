# Migrating a Mosquitto deployment: what is open

`mqttiumd` replaces Mosquitto for one deployment profile: a single listener,
plain TCP on loopback with anonymous access or TLS 1.3 with client
certificates and explicit identities, MQTT 3.1.1 and 5.0, the features of
record 0049. A deployment outside that profile needs work before it can
move. This page lists that work as decisions to take. It commits to none of
them.

The list comes from the external audit of `47e1d12` against Mosquitto 2.1.4
(reports 0031 to 0034) and from the tests written after it. "Observed" is
what the audit or a test of this repository saw; nothing here was measured
on another deployment.

## How to read a row

- **Today**: what the broker does, and where it is tested or recorded.
- **Who needs it**: the configuration or the client that cannot move
  without it.
- **Decision**: the question to answer. A refusal that is a product choice
  stays a refusal until someone decides otherwise.

## Configuration

| Feature | Today | Who needs it | Decision |
| - | - | - | - |
| `password_file`, usernames and passwords | Blocking directive. A CONNECT with a username is refused on the anonymous listener (CONNACK 4 in 3.1.1, 0x83 in 5) | Any deployment that authenticates by password | Offer password authentication, or keep certificates as the only identity. It needs a durable identity model beside record 0035 |
| `acl_file` | Blocking directive. Grants are native, per identity (record 0035); `preview/mosquitto-config.py` converts static per-user rules | Deployments with ACL files using patterns (`%u`, `%c`), roles or per-listener rules | Extend the converter, or read the file natively. Patterns need a grant model the broker does not have |
| Several listeners, `per_listener_settings` | One listener per broker; a second is refused | Deployments that serve plain and TLS, or several networks, from one broker | Several listeners with one policy, or one broker per listener |
| WebSocket | Not offered | Browser clients | A transport beside TCP and TLS |
| Plugins, Dynamic Security | Not offered | Deployments that manage identities at run time | An administration surface. None exists |
| Bridges | Not offered | Deployments that forward between brokers | Out of the profile unless a use is named |
| `$SYS` topics | A filter on `$SYS` is granted and receives nothing | Monitoring built on `$SYS` | Publish a subset, or point monitoring at the event journal |
| MQTT 3.1 (`MQIsdp`) | Closed before CONNACK | Old devices | Probably none; say so |

`mqttiumd check-config FILE` classifies every directive and names in
`launch_requires` what the command line must still give
([ADR 0054](decisions/0054-mosquitto-config-subset.md)).

## MQTT 5 capabilities

| Feature | Today | Who needs it | Decision |
| - | - | - | - |
| Shared subscriptions | Announced unavailable; `$share/…` is DISCONNECT 0x9E | Consumers that share a load | A delivery policy among members and its durable state |
| Subscription identifiers | Announced unavailable; DISCONNECT 0xA1 | Clients that route on the identifier | Store and return the identifier per subscription |
| Topic aliases | Topic Alias Maximum 0; an alias is DISCONNECT 0x94 | Clients that send aliases without reading the CONNACK | Accept inbound aliases. The audit saw Mosquitto announce 10 |
| Receive Maximum | 8, fixed; the audit saw Mosquitto announce 20 | A 3.1.1 client with many QoS 2 exchanges in flight: 41 complete, more stall until Keep Alive (record 0060) | Raise the window, make it configurable, or keep it. It sizes the exchange table (record 0048) |
| Maximum Packet Size | 1,048,584 bytes; the audit saw Mosquitto announce 2,000,000 | Publishers of payloads over 1 MiB | Raise `MAX_PAYLOAD` and what is sized from it |
| Empty ClientID | Refused (CONNACK 2 in 3.1.1, 0x85 in 5); the Mosquitto command-line clients need `-i` | Clients that let the broker name them | Assign an identifier, as MQTT 5 allows |

## Will

| Feature | Today | Who needs it | Decision |
| - | - | - | - |
| Will QoS 0 and 2 | Refused at CONNECT (0x83); only QoS 1 is stored (record 0034) | Clients whose library defaults to a QoS 0 Will | Admit QoS 0 as a volatile Will, QoS 2 as a durable one |
| Will Delay Interval | A non-zero delay is refused | Clients that want a reconnection to cancel the Will | A durable timer per Will |
| Will properties | Refused, Message Expiry included | Clients that set Content Type or User Properties on the Will | Carry them as record 0053 does for PUBLISH |

## Operation

| Matter | Today | Who needs it | Decision |
| - | - | - | - |
| SIGHUP | Ends the process with status 0. Mosquitto reloads its configuration and goes on | A supervisor or a log rotation that sends SIGHUP | Ignore the signal, or reload. Ending on it surprises an operator; ignoring it is the smallest change |
| Durability of a DISCONNECT | Durable at the commit of the end of the session; the end of the stream is not an acknowledgement (record 0059) | Clients that rely on a Will not being published after a normal disconnection | Decided: keep. Record 0059 says what closing after the commit would cost |
| One session can hold every inbound exchange | Open (record 0060, open conditions) | Any listener an untrusted client can reach | A bound per session in the core |

## Policy differences a client can see

These are not gaps. Both behaviours are allowed; a client written against
one can be surprised by the other.

| Situation | This broker | Mosquitto 2.1.4, as the audit saw it |
| - | - | - |
| A PUBLISH the policy forbids | Closes the connection in 3.1.1; DISCONNECT 0x87 in MQTT 5 | Acknowledges without distributing in 3.1.1; in MQTT 5 keeps the connection, ignores QoS 0, answers 0x87 to QoS 1 and 2 |
| A subscription wider than the grants | Refused in the SUBACK | Can be granted, the deliveries filtered |
| A Will the policy forbids | CONNECT refused (5 in 3.1.1, 0x87 in 5) | CONNECT accepted; the Will is not delivered |
| Overlapping subscriptions | One copy, at the highest QoS that applies (record 0049) | Can deliver one copy per subscription |
| A DISCONNECT with reason 0x80 or 0x83 | The Will is published (record 0058) | The Will is not published |
| A PUBLISH without subscriber | PUBACK 0x10; PUBREC plain | Not compared |

## Not compared

No differential run against Mosquitto 2.1.4 was made in this repository:
the compatibility bench is pinned to 2.0.21. The "as the audit saw it"
column is the audit's, on its host.
