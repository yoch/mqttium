# Benchmarking contract

Benchmark results are build artefacts, not source-code claims.

- `perf_sprint.py` detects local implementation regressions.
- `compare_libs.py` compares only equivalent public contracts. A library is
  reported as `N/A` instead of receiving artificial synchronization barriers.
- `realworld.py` uses fresh publisher processes plus an independent
  `mosquitto_sub`, verifies every sequence number, and reports broker ACK and
  confirmed delivery separately.
- `application_stress.py` measures ordered callbacks, iterator backpressure,
  and memory/SQLite inflight persistence, including batched versus autocommit
  transactions.

The scheduled workflow covers local TCP, TLS, and a controlled `netem` profile.
Every result records Python, platform, package versions, payload, QoS, inflight
window, transport/profile, CPU, RSS and latency percentiles. PUBACK confirms
broker acceptance; it never proves consumer delivery.

Comparisons must use equivalent public completion semantics and rotate execution
order where warm-up could matter. A result is omitted rather than manufactured
with library-specific barriers. CI uploads JSON artefacts and never commits or
pushes generated numbers.

`runner_probe.py` records CPU affinity/model/governor, load, temperature, Python
and broker metadata. Hosted CI records this context only. A dedicated performance
runner must use `--enforce`; an ineligible machine produces no gate evidence.
Paired network repeats must be even so each scenario completes exact ABBA cycles.
The targeted QoS 1 run records receipt and callback completion separately plus
the existing EffectPump decision counters.

## Interprétation de la latence

`realworld.py` horodate immédiatement avant l'appel applicatif à `publish()`.
La latence publiée inclut donc l'admission locale et le temps passé dans les
files, pas seulement le trajet réseau. Une grande fenêtre inflight augmente le
débit en permettant le batching, mais peut mécaniquement augmenter les
percentiles de latence. Le paramètre `--window` doit être balayé avant de
qualifier une variation de régression.

Une calibration appariée avec un code identique des deux côtés a confirmé cette
relation : la médiane locale reste sous la milliseconde aux fenêtres 1 et 5,
augmente à la fenêtre 20, puis devient à la fois plus élevée et beaucoup plus
bruitée à la fenêtre 100, surtout pour les payloads de 4 Kio. Une valeur isolée
à grande fenêtre mesure donc principalement la résidence dans le pipeline et la
variabilité du runner ; elle ne suffit pas à établir une régression du moteur.

Pour une modification sensible aux chemins chauds,
`paired_regression.py` et `paired_network.py` exécutent `main` et le candidat
sur le même runner, en ordre alterné et dans des interpréteurs frais. Les
mesures réseau à fenêtre élevée restent sujettes aux pauses du runner et du
subscriber ; les ratios micro appariés et les tendances sur plusieurs fenêtres
priment sur une valeur isolée.

## Par où commencer quand on cherche à optimiser

Cette section n'ajoute pas de règle à respecter : elle décrit l'ordre de travail
qui, en pratique, a trouvé le plus de choses pour le moins d'effort. Le
raisonnement de fond est que **toutes les mesures ne sont pas également
sensibles à la charge de la machine**, et qu'il est donc payant de commencer par
celles qui n'y sont pas du tout.

| Mesure | Sensible à la charge | Ce qu'elle répond |
| --- | --- | --- |
| Nombre d'appels par opération (`cProfile`, `ncalls`) | **Non**, exact | « Combien de fois fait-on ceci ? » |
| Allocations, objets créés par opération | **Non**, exact | « Que garde-t-on, que jette-t-on ? » |
| `timeit` sur une fonction pure | Peu | « Combien coûte cette opération isolée ? » |
| A/B apparié bout en bout | **Beaucoup** | « Est-ce que ça se voit sur le scénario réel ? » |

### 1. Compter avant de chronométrer

Un profil déterministe se lit sur une machine chargée, et il répond à la
question la plus rentable : *qu'est-ce qui est fait plusieurs fois ?* Les
redondances se voient immédiatement — un compteur à `2.00/op` pour une opération
qui n'a de sens qu'une fois par message est un défaut, pas une hypothèse.
L'attribution par appelant (`pstats` expose les `callers`) dit ensuite *qui*
duplique, ce qu'une simple liste triée par temps ne dit jamais.

### 2. Chronométrer l'opération suspecte isolément

Une fois la redondance identifiée, `timeit` sur la fonction seule donne son coût
avec beaucoup moins de bruit qu'une mesure bout en bout, parce qu'on mesure
quelques centaines de nanosecondes au lieu d'en mesurer quelques milliers dont
elles font partie. C'est aussi ce qui permet de dire honnêtement *combien* on
gagne, plutôt que d'attribuer au correctif tout ce qui bouge.

### 3. Confirmer en A/B apparié, machine au repos

`paired_regression.py` et `paired_network.py` en dernier, sur une machine
réellement au repos — un hôte qui exécute autre chose produit des coefficients
de variation à deux chiffres, et un CV de base au-dessus de 5 % invalide la
cellule quel que soit le ratio affiché. Regarder la répartition paire par paire,
pas seulement la médiane : une médiane favorable portée par une seule valeur
aberrante, avec des cycles traversant le neutre, ne démontre rien.

### 4. Prévoir un témoin

Inclure un scénario qui **ne doit pas** bouger. C'est lui qui rend le reste
lisible : si le témoin bouge autant que la cible, la mesure ne dit rien. C'est
aussi la vérification falsifiable d'une attribution — si l'on pense qu'une
régression vient d'un composant, l'annuler doit ramener précisément le scénario
qui l'exerce, et lui seul.

### 5. La CI en dernier, pour ce qu'elle sait faire

Un runner hébergé ne peut pas parler de latence absolue (pas de gouverneur
`performance`) et son balayage réseau est consultatif (voir plus haut). Il reste
utile pour les ratios micro appariés, qui s'y révèlent stables.

### Ce que cet ordre a produit, et ce qu'il a écarté

Il vaut la peine de noter *quelle sorte* d'optimisation a survécu à cette
séquence. Ont tenu : les changements qui suppriment un **appel** — une trame
constante reconstruite par l'encodeur générique, une conversion d'enum sur une
valeur déjà convertie, un encodage UTF-8 dont le résultat était jeté, une boucle
interprétée remplacée par des scans C. Ont été annulés après mesure : les
changements argumentés par un **comptage d'allocations**, qui échangeaient tous
une opération bon marché contre une plus chère.

Ce n'est pas une loi, mais c'est une heuristique utile : un argument
d'allocation est une hypothèse, pas une preuve, et il demande une mesure avant
d'être retenu — pas après.

## Memory regression thresholds

`benchmarks/memory_profile.py` is guarded by `benchmarks/check_memory_thresholds.py`,
which the benchmarks workflow runs immediately after the profile and which fails
the build on a breach.

`benchmarks/memory_thresholds.json` is versioned deliberately. It holds
*limits*, not measurements, so it does not violate the artefact-only rule above:
no generated number is committed. It bounds the tracemalloc peak — a count of
Python allocations, comparable across runners, unlike absolute RSS — and
asserts each scenario's logical counters exactly, so a benchmark that quietly
stopped doing equivalent work cannot pass as an improvement.

Reference values live in [`reports/MEMORY-RESULTS.md`](reports/MEMORY-RESULTS.md). Raising a threshold is a
reviewable change and needs a reason.
