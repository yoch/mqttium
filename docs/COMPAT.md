# Compatibilité Paho — choix, écarts, rejets

Ce document fige la politique de `mqttium.compat.paho` et explique ce qui
est volontairement **refusé**. La façade est une couche **additive** : le cœur
reste `AsyncClient` / `ProtocolEngine`.

## Objectif

Meilleure compatibilité **pratique** avec Paho `CallbackAPIVersion.VERSION2`
pour migration, sans reproduire les anti-patterns et la dette du monolithe.

## Surface supportée (VERSION2)

| API | Statut | Notes |
| --- | --- | --- |
| `Client(CallbackAPIVersion.VERSION2, …)` | Oui | Seule version de callbacks |
| `loop_start` / `loop_stop` | Oui | Thread + event loop dédiée |
| `connect` / `disconnect` / `reconnect` | Oui | Sync, bloquant |
| `publish` → `MQTTMessageInfo` | Oui | `wait_for_publish` propage les erreurs |
| `subscribe` / `unsubscribe` | Oui | Forme simple `(rc, mid)` |
| `on_connect(client, userdata, flags, reason_code, properties)` | Oui | |
| `on_disconnect(client, userdata, flags, reason_code, properties)` | Oui | `DisconnectFlags` |
| `on_message` / `message_callback_add` | Oui | Topic **str** ; filtre exclusif comme Paho |
| `on_publish(…, mid, reason_code, properties)` | Partiel | reason/properties simplifiés |
| `username_pw_set` / `will_set` / `user_data_set` | Oui | Avant `connect` |
| `is_connected` | Oui | |

Helpers one-shot : préférer `mqttium.helpers` (async natif) plutôt que
`paho.mqtt.publish` / `subscribe`.

## Écarts volontaires (compat partielle assumée)

| Sujet | Paho | mqttium | Pourquoi |
| --- | --- | --- | --- |
| Callback VERSION1 | Supporté | **Refusé** | API morte, signatures ambiguës v3/v5 |
| `loop_forever` / `loop(timeout)` | Oui | Non | Anti-pattern event-loop ; `loop_start` suffit |
| `connect_async` | Oui | Non | Confusion sync/async ; utiliser `AsyncClient` |
| Republish QoS>0 non conforme sur clean session | Comportement historique flou | **Strict MQTT** | Correctness > bug-compat |
| MID pour QoS 0 | Alloué | `None` | Pas d’identifiant protocolaire ; info locale inutile |
| Appels bloquants **depuis** un callback réseau | Souvent « ça passe » | **Interdit** (RuntimeError) | Deadlock certain avec notre writer unique |
| `publish()` hors thread réseau | File interne + retour immédiat | QoS0 : file façade coalescée consommée par le loop ; QoS1/2 : handoff court pour le MID | Aucun publish n’attend le writer ; le moteur reste possédé par le thread réseau |
| WebSocket / proxy / socks | Large surface | WS via `AsyncClient.connect_ws` ; pas via façade sync | Couche transport séparée ; pas de monolithe |
| Persistence fichier Paho | Formats historiques | `SqliteInflightStore` sur `AsyncClient` | Pas de format binaire Paho |
| `suppress_exceptions` | Oui | Non | Les erreurs doivent remonter |
| `max_inflight_messages` public Paho | Couplé MID | `local_receive_maximum` + `FlowControl` | Receive Maximum ≠ espace MID (bug gmqtt évité) |

## Rejets explicites (impossible / mauvais design)

### 1. Reproduire le monolithe `client.py` (~200 KiB)

**Rejeté.** Coût de maintenance et mélange I/O / état / callbacks. mqttium
sépare engine sync et adaptateur async — leçon principale de l’audit Paho.

### 2. Faire de Receive Maximum un plafond de packet identifiers

**Rejeté (anti-pattern gmqtt).** Les MID sont 1…65535 ; la fenêtre est
`FlowControl`. Les confondre casse le pipelining QoS 1/2.

### 3. Timer artificiel pour « remplir » un batch d’écriture

**Rejeté.** L’audit Paho a montré que le batchage doit suivre la readiness /
les ACK, pas un sleep. Notre writer coalesce ce qui est déjà en file.

### 4. Callbacks sync qui ré-entrent le client sur le même thread

**Rejeté techniquement** avec l’architecture writer unique + `run_coroutine_threadsafe().result()`.
Paho s’en sort via des locks internes coûteux. Pour mqttium : planifier hors
thread réseau, ou passer à `AsyncClient`.

### 5. `clean_session=True` + retransmission QoS>0 « magique »

**Rejeté.** Non conforme. Session durable = `clean_start=False` (v3) ou
`session_expiry_interval > 0` (v5) ; reconnect utilise alors Clean Start 0.

### 6. Topic cache / auto topic aliases cachés

**Rejeté** (audit Paho : NO GO). Aliases **explicites** uniquement.

### 8. Libérer Receive Maximum local au PUBREC sous charge

**Rejeté en pratique (pour l’instant).** MQTT 5 le permet, mais la libération
anticipée a provoqué des stalls intermittents (×20–30) via accumulation de
`WAIT_PUBCOMP` + pression sur la file writer. Fenêtre locale tenue jusqu’au
PUBCOMP = correct + stable. Réévaluer avec métriques avant de rouvrir.

## Migration recommandée

1. Nouveau code → `mqttium.api.AsyncClient`
2. Legacy sync VERSION2 → `mqttium.compat.paho.Client`
3. One-shot → `mqttium.helpers.publish` / `subscribe`
4. Voir aussi `docs/MIGRATION.md`

## Tests de non-régression

- `tests/unit/test_compat_paho.py` — connect/publish/callbacks/filtres
- `tests/unit/test_compat_lib_subset.py` — jalon D (miroir comportemental `tests/lib`)
- `tests/unit/test_compat_publish_perf.py` — ordre des effets, réutilisation des MID et coalescence QoS0
- `tests/integration/test_compat_publish_perf.py` — callbacks et livraison QoS0 de bout en bout