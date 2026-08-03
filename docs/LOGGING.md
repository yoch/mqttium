# Logging & observabilité — décision

## Décision : pas de logging dans la bibliothèque

`mqttium` n’émet **aucun** log (`logging` stdlib absent de `src/`). C’est
volontaire et mesuré.

### Pourquoi

1. **Le logging utile serait sur le hot path** (publish / message / ACK), pas
   sur les événements rares. Or même **désactivé**, un guard
   `log.isEnabledFor(DEBUG)` coûte ~**1.6 %** (médiane, 0.5–3 %) par publish —
   et ça se cumule par point de log. **Activé**, formater + émettre par message
   détruit le débit et peut **fuiter des données** (payloads, topics, secrets).
2. La bibliothèque est **déjà observable sans logging** (voir ci-dessous).
3. Moins de surface = moins de bugs, moins de config, pas de handler global
   imposé à l’application.

Mesuré (ABBA, loop unique, 15 rounds, QoS0 publish) : overhead médian d’un
guard désactivé ≈ 1.65 %. Référence : `benchmarks/perf_sprint.py`.

## Comment observer sans logging

La lib expose déjà tout ce qu’il faut — **coût nul quand inutilisé** :

| Besoin | Mécanisme existant |
| --- | --- |
| Completion / erreur d’un publish | `await receipt.wait()` / `receipt.is_done()` |
| Cycle de vie connexion | callbacks `on_connect` / `on_disconnect(exc)` |
| Messages entrants | `on_message` / `async for msg in client.messages()` |
| État courant | `client.is_connected`, `client.state`, `client.negotiated` |
| Erreurs protocolaires | exceptions typées (`ProtocolError`, `MQTTTimeoutError`, …) |
| Comportement broker | `DisconnectInfo` (reason_code, properties, from_broker) |

### Recette : instrumentation par l’application (recommandé)

Wrapper fin côté app — aucune modification de la lib, aucun coût pour les
autres utilisateurs :

```python
import logging, time
from mqttium.api import AsyncClient

log = logging.getLogger("myapp.mqtt")

class ObservedClient:
    def __init__(self, client: AsyncClient) -> None:
        self._c = client
        self.published = 0
        self.errors = 0
        client.on_disconnect = self._on_disc

    async def publish(self, topic, payload, **kw):
        t0 = time.monotonic()
        try:
            r = await self._c.publish(topic, payload, **kw)
            self.published += 1
            return r
        except Exception:
            self.errors += 1
            log.warning("publish failed topic=%s", topic)
            raise
        finally:
            # métrique timing si besoin
            _ = time.monotonic() - t0

    def _on_disc(self, exc):
        log.info("mqtt disconnected: %r", exc)

    def __getattr__(self, name):  # délègue le reste
        return getattr(self._c, name)
```

Points clés :
- **Compteurs** (`published`, `errors`) plutôt que des strings par message.
- Logs **uniquement** sur les transitions/erreurs que *l’app* juge utiles.
- Le hot path de la lib reste intact (aucun guard ajouté côté mqttium).

### Si un jour la lib doit s’instrumenter

Le seul mécanisme acceptable (coût strictement nul quand inactif) serait un
**hook optionnel** dans l’esprit des callbacks existants :

```python
client.on_event = my_probe   # None par défaut → un seul `if hook is None`
```

Jamais de `logging` global, jamais de formatage par message. À réévaluer
uniquement avec un microbench prouvant < 0.5 % d’overhead désactivé.
