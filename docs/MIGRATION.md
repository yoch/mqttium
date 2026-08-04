# Migration Paho / gmqtt → mqttium

Ce document décrit comment migrer vers l’API native async et, le cas échéant,
vers la façade de compatibilité Paho.

## Choix d’API

| Besoin | API |
| --- | --- |
| Nouveau code / asyncio | `mqttium.api.AsyncClient` (recommandé) |
| Legacy sync style Paho VERSION2 | `mqttium.compat.paho.Client` |
| One-shot pub/sub | `mqttium.helpers.publish` / `mqttium.helpers.subscribe` |

## Depuis Paho

```python
# Avant (Paho)
from paho.mqtt.client import Client, CallbackAPIVersion
c = Client(CallbackAPIVersion.VERSION2, "id")
c.connect("localhost")
c.loop_start()
c.publish("t", b"x", qos=1)

# Après — façade compat (changements minimaux)
from mqttium.compat.paho import Client, CallbackAPIVersion
c = Client(CallbackAPIVersion.VERSION2, "id")
c.connect("localhost")
c.loop_start()
c.publish("t", b"x", qos=1)

# Après — natif async (recommandé)
from mqttium.api import AsyncClient
client = AsyncClient("id")
await client.connect("localhost")
receipt = await client.publish("t", b"x", qos=1)
await receipt.wait()
```

### Écarts volontaires

- Seul `CallbackAPIVersion.VERSION2` est supporté.
- Pas de republication QoS>0 non conforme sur session clean.
- `connect_async` historique n’existe pas ; utiliser `await connect()` ou la
  façade sync.
- Persistence : passer `store=SqliteInflightStore(path)` à `AsyncClient`.
- WebSocket : `await client.connect_ws("ws://host:9001/mqtt")` (pas via la façade sync).
- AUTH MQTT 5 : `AsyncClient(..., auth_handler=...)` ou `await client.auth(...)`.

## Depuis gmqtt

```python
# gmqtt
client = gmqtt.Client("id")
client.set_auth_credentials(user, password)
await client.connect("localhost")
client.publish("t", b"x", qos=1)
await client.disconnect()

# mqttium
client = AsyncClient("id", username=user, password=password)
await client.connect("localhost")
receipt = await client.publish("t", b"x", qos=1)
await receipt.wait()
await client.disconnect()
```

Points protocolaires corrigés vs gmqtt :

- MID conservé jusqu’au PUBCOMP (QoS 2)
- `Receive Maximum` ≠ espace de packet identifiers
- Parser incrémental non allocateur par octet

## Limites d’admission bornées par défaut

Depuis la série non publiée, toutes les files qui croissent avec la charge
applicative sont bornées par défaut. Auparavant un producteur QoS 1/2 pouvait
empiler jusqu’à épuisement de l’espace des 65 535 identifiants de paquet.

```python
client = AsyncClient(
    max_pending_outbound_messages=10_000,      # publications QoS 1/2 non terminées
    max_pending_outbound_bytes=64 * 1024**2,   # leur taille logique topic+payload+propriétés
    max_pending_delivery_bytes=64 * 1024**2,   # messages entrants en attente de consommateur
    publish_backpressure="wait",               # ou "error" pour refuser immédiatement
)
```

Conséquences pour le code existant :

- `publish()` attend la capacité par défaut. Sous `publish_backpressure="error"`
  ou avec `nowait=True`, il lève `FlowControlError`. Le refus est atomique :
  ni identifiant de paquet alloué, ni enregistrement de store écrit.
- Un `publish()` garé sur la capacité échoue si la connexion est définitivement
  perdue ; il continue d’attendre pendant une reconnexion en cours.
- `publish_many()` ne retient au plus que `max_failure_details` (128) détails
  d’échec. Les totaux restent exacts via `PublishBatchError.failure_count` et
  `failure_counts` ; utiliser `failure_sink=` pour tout capturer.
- Passer `None` sur une limite restaure le comportement non borné d’avant.
- Façade Paho : la saturation renvoie `MQTT_ERR_QUEUE_SIZE` (15), et
  `max_queued_messages_set()` / `max_queued_bytes_set()` ajustent ces limites.

## Implémentations tierces d’`InflightStore`

Le Protocol `InflightStore` gagne trois méthodes de pagination : `out_pages()`,
`out_summary_pages()` et `in_pages()`. Elles servent à réhydrater une session
persistante sans matérialiser tous les payloads en même temps —
`out_summary_pages()` en particulier ne sélectionne jamais la colonne payload.

Un store tiers qui ne les implémente pas **continue de fonctionner**, mais
retombe silencieusement sur le chemin eager (`out_items()` intégral). Sur un
jeu de 6 000 messages de 4 KiB, cela représente la différence entre ~4,5 MiB et
~50 MiB de pic alloué à la reconnexion. Implémenter les trois méthodes en
suivant `SqliteInflightStore` (pagination par clé `WHERE seq > ? ORDER BY seq
LIMIT ?`) si la volumétrie le justifie.

## Helpers one-shot

```python
from mqttium.helpers import publish, subscribe

await publish.single("t", b"hello", qos=1, hostname="127.0.0.1")
msg = await subscribe.simple("t/#", hostname="127.0.0.1")
```

## Licence

Code from-scratch : **Apache License 2.0** (voir `LICENSE`). Les analyses
documentaires citent Paho/gmqtt sans en copier le moteur.
