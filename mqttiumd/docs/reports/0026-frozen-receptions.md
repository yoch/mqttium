# Frozen receptions of the bounded preview

ADR 0044 freezes the system receptions and operating kits of the bounded
preview. This report indexes them. It adds no measurement.

## What is frozen

The qualified executable is the one built from
`51be4079f7ee42f46500e993315eddc971f0a8d4`, SHA-256
`9ef097977c2cb86669e11e98d7fbceeac87a4f86fd6a24c7deb76ea9c31bf119`
(`docs/pilot/decision.md`). The last revision at which every job below was
green on the pull request is `bf9f702e8afb4e42433e164c9752f23db6a5315d`.

| Evidence | Driver | Workflow job, now on manual dispatch |
| - | - | - |
| QoS 0 volatile path | `tools/run_qos0_reception.sh` | `mqttiumd-frozen-receptions.yml` / `qos0-product` |
| Live receipt collection | `tools/run_receipt_reception.sh` | `receipts-product` |
| Session lifecycle | `tools/run_session_reception.sh` | `sessions-product` |
| Finite session expiry | `tools/run_expiry_reception.sh` | `expiry-product` |
| Outgoing identifier reuse | `tools/run_pi_reception.sh` | `outgoing-pi-product` |
| Durable exact pub/sub | `tools/run_pubsub_reception.sh` | `pubsub-product` |
| TCP QoS 1 and QoS 2 | `tools/run_tcp_reception.sh`, `tools/run_qos1_reception.sh` | `tcp-product` |
| Hierarchical subscriptions | `tools/run_wildcard_reception.sh` | `wildcard-preview` |
| Durable retained | `tools/run_retained_reception.sh` | `retained-preview` |
| Message Expiry | `tools/run_message_expiry_reception.sh` | `message-expiry-preview` |
| Durable Will, retained Will | `tools/run_will_reception.sh`, `tools/run_retained_will_reception.sh` | `will-preview`, `secure-preview` |
| Private mTLS and offline policy | `tools/run_secure_reception.sh`, `tools/run_policy_transition_reception.sh` | `secure-preview` |
| Sealed pre-pilot package | `tools/run_prepilot_reception.sh` | `prepilot-package` |
| ARM64 preview and LAN restart | `preview/lan/` | `mqttiumd-arm64-preview.yml`, `mqttiumd-lan-restart.yml` |
| MQTT 3.1.1 interoperability | `preview/mqtt311-demo.py` | `mqttiumd-mqtt311-preview.yml` |
| Reactor A/B | `tools/run_progression_ab.py` | `mqttiumd-progression-ab.yml` |
| Site, split, forward and cutover kits | `tools/run_site_reception.sh`, `tools/run_split_reception.sh`, `tools/run_forward_reception.sh`, `preview/cutover_demo.py` | Run by hand from the package |

## What a frozen reception means from now on

Each one asserts the quotas, the event stream and the packaging of the
bounded preview: 16 connections, 128 references, the per-frame events. A
failure after the runtime or a limit changes is expected and is not a
detection. The reports 0007 to 0025, the journals under `docs/reception/`
and the pilot documents keep describing the executable they measured.

The behaviours these receptions exercised are carried forward by the
product tests of `crates/mqttiumd/` and by the compatibility scenarios
under `tests/compat/`. The core campaigns, the models and the pilot proof
are not frozen: they remain required checks.
