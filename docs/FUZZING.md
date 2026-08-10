# Fuzzing — mqttium (jalon E)

Fuzzer **sans dépendance**, reproductible par seed, avec oracles d’invariants.

## Cibles

| Cible | Ce qui est fuzzé | Oracle |
| --- | --- | --- |
| `codec` | properties, PUBLISH, frames typées mutées | seules des `MQTTError`/`ValueError`/`struct.error` sortent |
| `engine` | séquences protocolaires à états (CONNACK dup, ACK storms, alias, reconnect, manual_ack) | invariants : flow borné, mids cohérents, pas de compteur négatif |
| `websocket` | headers de frames (longueurs 64-bit, contrôle, fragmentation) | bornes mémoire, `ConnectionError` sur dépassement |

## Deux harness complémentaires

| Harness | Fichier | Force |
| --- | --- | --- |
| Seedé, sans dépendance | `tests/fuzz/fuzz.py` | Reproductible (`--seed`), CI simple, invariants custom |
| Hypothesis (guidé + shrinking) | `tests/fuzz/test_hypothesis_fuzz.py` | Génération property-based, shrinking auto des cas d’échec |

## Lancer

```bash
cd /workspace
# Smoke (dans la suite pytest)
PYTHONPATH=mqttium/src python3 -m pytest mqttium/tests/unit/test_fuzz_smoke.py -q

# Seedé, borné
PYTHONPATH=mqttium/src python3 mqttium/tests/fuzz/fuzz.py --seed 1 --iterations 20000

# Hypothesis (profil CI par défaut)
pip install -e "mqttium[fuzz]"
PYTHONPATH=mqttium/src python3 -m pytest mqttium/tests/fuzz/test_hypothesis_fuzz.py -q

# Hypothesis agressif (3000 exemples / test)
cd mqttium && HYPOTHESIS_PROFILE=aggressive python3 -m pytest tests/fuzz/test_hypothesis_fuzz.py -q
```

## CI

Le job `fuzz` de `.github/workflows/ci.yml` exécute le fuzzer seedé (3×20k) et
Hypothesis (profil `ci`) sur chaque changement. Le workflow
`.github/workflows/fuzz-campaign.yml` ajoute trois niveaux bornés :

- une minute sur les PR qui touchent les cibles ou le harness ;
- trois shards de 20 minutes chaque nuit ;
- sur déclenchement manuel, une campagne de release candidate de cinq shards
  de 288 minutes, soit 1 440 minutes (24 CPU-heures) de fuzzing mono-processus.

La limite de chaque job RC est de 300 minutes : les douze minutes restantes
sont réservées à l'installation et à l'upload. La durée de 288 minutes inclut
le passage Hypothesis agressif, puis des lots seedés qui alternent `codec`,
`engine` et `websocket` jusqu'à l'échéance.

## Reproductibilité

- Seedé : `--seed N` reproduit exactement la séquence.
- Hypothesis : tout échec est **shrinké** automatiquement et rejouable via la
  base d'exemples (`.hypothesis/`) ou `@reproduce_failure`.
- Campagnes longues : le seed du shard et chaque seed de lot figurent dans
  `metadata.json` et `campaign.log`. La quantité de lots achevés dépend de la
  vitesse du runner, mais chaque lot reste exactement rejouable. Le corpus
  Hypothesis, les inputs fautifs et les logs sont uploadés même après un échec.

## Logging temps réel & rejouabilité

Le fuzzer seedé logue sur **stderr** (flush immédiat, parseable) :

```
[START] target=engine seed=1 iterations=20000
[PROGRESS] target=engine iter=2001/20000 rate=87,590/s elapsed=0.0s
[FAIL] target=engine iter=50 kind=crash seed=7 elapsed=0.00s
[ARTIFACT] mqttium/tests/fuzz/artifacts/engine-seed7-iter50.bin
[DONE] target=engine status=FAIL iters=20000 crashes=1 ... elapsed=0.1s
```

- `--progress-every N` : cadence des lignes de progression (débit + elapsed).
- `--artifacts-dir DIR` : chaque input fautif est écrit pour replay.
- `--quiet` : coupe les logs temps réel (résumé final conservé).
- Exit code `1` dès qu’une cible a un crash ou une violation d’invariant.

Rejouer un cas : `--seed N` reproduit la séquence ; l’artefact `.bin` est
l’input exact à renvoyer dans la cible.

## Conservation

Les artefacts nightly sont conservés 30 jours. Les cinq artefacts d'une
campagne RC sont conservés 90 jours et contiennent le SHA, le profil, les
seeds, le corpus Hypothesis, les inputs fautifs et le log complet.
