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

## Helpers one-shot

```python
from mqttium.helpers import publish, subscribe

await publish.single("t", b"hello", qos=1, hostname="127.0.0.1")
msg = await subscribe.simple("t/#", hostname="127.0.0.1")
```

## Licence

Code from-scratch : **Apache License 2.0** (voir `LICENSE`). Les analyses
documentaires citent Paho/gmqtt sans en copier le moteur.
