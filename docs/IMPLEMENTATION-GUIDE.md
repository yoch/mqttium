# Guide d'implémentation — contrats détaillés

Ce document complète `DESIGN.md` (architecture) et `ROADMAP.md` (séquencement).
Il fixe les contrats **précis** dont un implémenteur a besoin pour les phases
1–3 sans avoir à réinterpréter la spec MQTT ni les audits. En cas de conflit,
l'ordre d'autorité est : spec MQTT (3.1.1 / 5.0) > ce guide > DESIGN.md.

## 1. Invariants globaux (toutes phases)

1. **Writer unique** : un seul task écrit sur le transport, alimenté par une
   file FIFO. L'ordre wire == l'ordre d'émission des effets `SEND` du moteur.
   Aucune autre coroutine n'appelle `transport.write`.
2. **Engine sans I/O** : `ProtocolEngine` ne fait aucun await, ne touche ni
   socket ni horloge murale. Les timers (keepalive, timeouts) vivent dans la
   couche async et injectent des *commandes* dans le moteur.
3. **Receipts avant wire** : tout futur/receipt attaché à un MID est enregistré
   **avant** que le paquet correspondant puisse partir (l'ACK peut arriver
   pendant le premier `await` qui suit).
4. **Ownership des bytes** : le moteur ne reçoit que des `bytes` possédés
   (jamais un `memoryview` d'un buffer réutilisable). Le décodeur copie à la
   frontière paquet.
5. **MID ≠ fenêtre** : `PacketIdPool` couvre `1..65535` en permanence ;
   `FlowControl` borne uniquement les PUBLISH QoS 1/2 non terminés.
   `Receive Maximum` (CONNACK) ajuste `FlowControl.limit`, jamais le pool.
6. **Une source de vérité** : l'état QoS vit dans `InflightStore` + les états
   `OutboundQoSState`/`InboundQoSState`. `_queued` n'est qu'un index ordonné
   des messages en état `QUEUED` — tout message de `_queued` est aussi dans
   le store avec cet état, et réciproquement.
7. **Propriétaire unique des ressources sortantes** : une publication QoS 1/2
   acquiert quatre ressources — budget d'admission, MID, ligne de store, slot
   `FlowControl`. `OutboundSession` (`protocol/outbound.py`) est le seul à les
   acquérir et à les restituer ; `ProtocolEngine` ne touche jamais les
   compteurs. Toute nouvelle opération sortante doit passer par lui, sans quoi
   un rollback partiel redevient possible.
8. **Admission tout-ou-rien, une seule primitive d'annulation** :
    `queue_publish()` et `queue_publish_many()` défont leurs acquisitions via
    `OutboundSession._rollback()`, dans un ordre unique — effets, index de file,
    slots de flow, lignes de store, MID, budget. L'appelant ne fait que
    *snapshoter* (trois lectures locales), de sorte que le chemin nominal ne
    paie rien et que les deux voies ne peuvent pas diverger. Le budget est
    restauré en bloc depuis le snapshot et jamais relâché enregistrement par
    enregistrement : un store transactionnel a déjà annulé son batch quand
    l'`except` s'exécute, donc les tailles unitaires sont perdues et une seconde
    libération unitaire compterait deux fois. Contrat vérifié par injection de
    faute après chaque acquisition dans
    `tests/unit/test_outbound_transaction.py`, contre store mémoire *et* SQLite.
9. **Propriétaire unique de l'état entrant** : `InboundSession`
    (`protocol/inbound.py`) possède les alias entrants, le compteur local
    `Receive Maximum`, les lignes QoS entrantes, l'ACK manuel et le replay. Les
    handlers `PUBLISH` / `PUBREL` lui sont liés directement, sans wrapper moteur
    sur le chemin chaud. Il émet néanmoins via l'unique flux d'effets de
    `ProtocolEngine`, afin que l'ordre avec `CONNACK` et `DISCONNECTED` reste
    observable en un seul endroit.
10. **Callbacks hors section critique** : aucun verrou/état moteur tenu pendant
   un callback ; un callback peut appeler `publish()` sans deadlock.
11. **Pas de retransmission sur connexion vivante** : les PUBLISH/PUBREL ne sont
   rejoués **qu'à la reconnexion** (session présente), avec DUP=1 pour les
   PUBLISH. Jamais de timer de retransmission en cours de session (conforme
   MQTT ≥3.1.1).

## 2. MQTT 5 — table complète des propriétés (phase 1)

Types : B=Byte, 2I=uint16, 4I=uint32, VBI=Variable Byte Integer,
S=UTF-8 string, SP=paire UTF-8, BIN=binary data. « Will » = will properties
du CONNECT. Sauf mention, une propriété ne peut apparaître **qu'une fois**
par paquet (sinon → erreur protocole, DISCONNECT 0x82).

| ID | Nom (clé API) | Type | Paquets | Multiple |
|---|---|---|---|---|
| 0x01 | `payload_format_indicator` | B | PUBLISH, Will | non |
| 0x02 | `message_expiry_interval` | 4I | PUBLISH, Will | non |
| 0x03 | `content_type` | S | PUBLISH, Will | non |
| 0x08 | `response_topic` | S | PUBLISH, Will | non |
| 0x09 | `correlation_data` | BIN | PUBLISH, Will | non |
| 0x0B | `subscription_identifier` | VBI | PUBLISH, SUBSCRIBE | PUBLISH : oui / SUBSCRIBE : non, et valeur 0 interdite |
| 0x11 | `session_expiry_interval` | 4I | CONNECT, CONNACK, DISCONNECT | non |
| 0x12 | `assigned_client_identifier` | S | CONNACK | non |
| 0x13 | `server_keep_alive` | 2I | CONNACK | non |
| 0x15 | `authentication_method` | S | CONNECT, CONNACK, AUTH | non |
| 0x16 | `authentication_data` | BIN | CONNECT, CONNACK, AUTH | non |
| 0x17 | `request_problem_information` | B | CONNECT | non |
| 0x18 | `will_delay_interval` | 4I | Will | non |
| 0x19 | `request_response_information` | B | CONNECT | non |
| 0x1A | `response_information` | S | CONNACK | non |
| 0x1C | `server_reference` | S | CONNACK, DISCONNECT | non |
| 0x1F | `reason_string` | S | CONNACK, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK, UNSUBACK, DISCONNECT, AUTH | non |
| 0x21 | `receive_maximum` | 2I | CONNECT, CONNACK | non ; 0 interdit |
| 0x22 | `topic_alias_maximum` | 2I | CONNECT, CONNACK | non |
| 0x23 | `topic_alias` | 2I | PUBLISH | non ; 0 interdit |
| 0x24 | `maximum_qos` | B | CONNACK | non ; ∈ {0,1} |
| 0x25 | `retain_available` | B | CONNACK | non |
| 0x26 | `user_property` | SP | tous + Will | **oui**, ordre préservé |
| 0x27 | `maximum_packet_size` | 4I | CONNECT, CONNACK | non ; 0 interdit |
| 0x28 | `wildcard_subscription_available` | B | CONNACK | non |
| 0x29 | `subscription_identifier_available` | B | CONNACK | non |
| 0x2A | `shared_subscription_available` | B | CONNACK | non |

Contrats codec :

- Encodage : table `dict[str, (id, type)]` précalculée au module ; fast path
  « aucune propriété » = octet `0x00` (déjà en place phase 0).
- Décodage : curseur par offsets (`unpack_from`), pas de slicing en cascade ;
  vérifier que la longueur annoncée des properties correspond exactement aux
  octets consommés, sinon `MalformedPacketError`.
- Validation par paquet à l'encodage ET au décodage (table ID → paquets
  autorisés). Propriété inconnue en réception : `MalformedPacketError`
  (spec : malformed packet). Propriété dupliquée non répétable : idem.
- API : singletons stockés directement, répétables (`user_property`,
  `subscription_identifier` PUBLISH entrant) en listes.

## 3. Négociation CONNACK (phase 1)

À l'issue d'un CONNACK succès, construire un objet `NegotiatedSettings` figé,
distinct des propriétés demandées :

| Champ | Source | Défaut si absent | Application |
|---|---|---|---|
| `receive_maximum` | CONNACK 0x21 | 65535 | `flow.limit = min(local_max, valeur)` |
| `maximum_packet_size` | CONNACK 0x27 | illimité | refuser à `publish()` tout paquet encodé plus grand (`PacketTooLargeError`, avant mise en file) |
| `maximum_qos` | CONNACK 0x24 | 2 | rétrograder ou refuser `publish(qos>max)` → refuser avec `ProtocolError` (pas de dégradation silencieuse) |
| `retain_available` | CONNACK 0x25 | 1 | `publish(retain=True)` → `ProtocolError` si 0 |
| `topic_alias_maximum` | CONNACK 0x22 | 0 | borne des alias **sortants explicites** ; 0 = interdits |
| `server_keep_alive` | CONNACK 0x13 | keepalive demandé | remplace la période keepalive |
| `assigned_client_identifier` | CONNACK 0x12 | client_id local | exposé en lecture (`client.effective_client_id`) |
| `session_expiry_interval` | CONNACK 0x11 | valeur CONNECT | source de vérité pour la session |
| `wildcard/shared/subid available` | CONNACK 0x28/0x2A/0x29 | 1 | valider localement `subscribe()` et lever avant émission |

Côté réception, notre `maximum_packet_size` local (envoyé dans CONNECT) est
appliqué par `IncrementalDecoder.max_packet_size`.

Alias topic **entrants** : table `dict[int, str]` remise à zéro à chaque
connexion réseau ; alias 0 ou > notre `topic_alias_maximum` annoncé →
DISCONNECT 0x94 (Topic Alias invalid). Alias inconnu avec topic vide → idem.
Alias **sortants** : uniquement explicites (`publish(..., topic_alias=n)`),
jamais automatiques (audit Paho §24 NO GO).

## 4. Keepalive (phase 1)

Période effective `K` = `server_keep_alive` sinon keepalive CONNECT.
`K == 0` → désactivé (aucun timer).

Algorithme (deadlines monotoniques, pas de tick fixe — audit §20) :

1. Maintenir `last_outbound` (mis à jour par le writer à chaque write réussi).
2. Timer armé sur `last_outbound + K` : à l'échéance, si aucun paquet sortant
   depuis `K`, émettre PINGREQ et marquer `ping_pending = True` avec deadline
   `now + max(K/2, 5s)` (configurable `ping_timeout`).
3. PINGRESP (ou tout paquet entrant ? **non** : seul PINGRESP) efface
   `ping_pending`. Un PINGRESP non attendu est ignoré.
4. `ping_pending` à échéance → fermer transport avec `MQTTTimeoutError`
   (déclenche la politique de reconnexion).
5. Le PINGREQ passe par la même file writer (ordre wire préservé).

Rationale : baser la détection sur PINGRESP explicite corrige la faiblesse
gmqtt (« toute donnée entrante vaut pong ») ; côté serveur la fenêtre morte
est 1.5×K, notre détection cliente doit être plus courte.

## 5. Reconnexion (phase 1)

`ReconnectPolicy` (dataclass configurable) :

- `enabled: bool = True` (False = `connect()` one-shot)
- `initial_delay: float = 1.0`, `multiplier: float = 2.0`,
  `max_delay: float = 60.0`, jitter uniforme `[0.5, 1.0)` × délai
  (full-jitter borné), `max_retries: int | None = None`
- reset du backoff après une connexion restée stable ≥ `stable_after = 30s`.

**Codes terminaux** (ne PAS retenter, remonter l'erreur) :

- v3.1.1 CONNACK : 1 (protocol version), 2 (identifier rejected),
  4 (bad user/password), 5 (not authorized). Code 3 (server unavailable)
  est retryable.
- v5 CONNACK/DISCONNECT : 0x84 (unsupported protocol), 0x85 (client id
  invalid), 0x86 (bad user/pass), 0x87 (not authorized), 0x8C (bad auth
  method), 0x9D (server moved — exposer `server_reference`, ne pas suivre
  automatiquement). 0x88/0x89 (unavailable/busy) et erreurs réseau sont
  retryables ; 0x9C (use another server) : retryable seulement si l'app gère
  `server_reference`, sinon terminal.

À chaque tentative : nouveau transport, `decoder.clear()`, CONNECT avec
`clean_start=False` si l'utilisateur avait demandé une session persistante
(sinon sa valeur d'origine — ne pas forcer False comme gmqtt).
Timeout TCP+CONNACK par tentative : `connect_timeout` (défaut 30 s).
Les receipts en attente **survivent** aux reconnexions tant que la session
survit ; ils échouent avec `SessionDiscardedError` si le broker répond
`session_present=0` (déjà implémenté phase 0).

## 6. Timeouts et libération des MID (phase 1)

| Opération | Timeout défaut | À l'échéance |
|---|---|---|
| TCP connect + CONNACK | 30 s | fermer, `MQTTTimeoutError`, politique reconnect |
| SUBACK / UNSUBACK | 30 s | `release(mid)` + lever `MQTTTimeoutError` sur le futur |
| PUBACK/PUBREC/PUBCOMP | **aucun** (session) | jamais : la retransmission a lieu à la reconnexion |
| PINGRESP | `max(K/2, 5s)` | fermer transport |
| `disconnect()` drain | 5 s | fermeture forcée |

SUBACK/UNSUBACK deviennent awaitables : `subscribe()` retourne le résultat
complet (`SubAck` avec reason codes par filtre) via un futur enregistré
**avant** flush (même règle que les receipts). Un reason code ≥ 0x80 par
filtre n'est pas une exception : il est exposé dans le résultat.

## 7. Validation topics et filtres (phase 1)

À l'émission (lever `ValueError`/`ProtocolError` avant toute mutation d'état) :

- Topic PUBLISH : non vide (sauf alias v5 valide), ≤ 65535 octets UTF-8,
  sans `+` ni `#`, sans U+0000 ; les règles UTF-8 MQTT (pas de surrogates,
  pas de U+FEFF) sont déjà dans `codec.primitives`.
- Filtre SUBSCRIBE : non vide ; `#` seulement en dernier niveau et seul dans
  son niveau ; `+` seul dans son niveau ; `$share/{group}/{filter}` → group
  non vide, sans `/`, `+`, `#`, et `{filter}` validé récursivement ;
  refuser localement si `shared_subscription_available == 0`.
- En réception, topic PUBLISH contenant un joker → `MalformedPacketError`
  (DISCONNECT 0x81).

## 8. QoS — décisions figées

- **QoS 2 entrant : livraison à la réception du PUBLISH** (méthode « deliver
  on PUBLISH », celle de Paho), avec déduplication par MID jusqu'au PUBREL
  (implémenté phase 0). Ne pas basculer vers « deliver on PUBREL ».
- PUBREC avec reason ≥ 0x80 : transaction terminée en échec, MID libéré,
  receipt en erreur (`ProtocolError` portant le reason code) — phase 0
  émet `PUBLISH_COMPLETE`, à raffiner en `PUBLISH_FAILED` + reason en phase 1.
- PUBREC orphelin : répondre PUBREL (v5 : reason 0x92 Packet Identifier not
  found) sans toucher au pool sortant.
- PUBREL entrant orphelin : répondre PUBCOMP (idempotent) — implémenté.
- DUP sortant : uniquement lors du replay de reconnexion. PUBREL n'a jamais
  de DUP (flags fixes 0x02).
- `manual_ack` (phase 2) : ne diffère que l'envoi de PUBACK (QoS 1) /
  PUBCOMP (QoS 2) après `client.ack(message)` ; PUBREC reste immédiat.

## 9. Sémantique `PublishReceipt` / erreurs

- QoS 0 : résolu dès l'acceptation en file writer (pas de garantie réseau).
- QoS 1 : résolu au PUBACK ; QoS 2 : au PUBCOMP.
- Échecs possibles : `SessionDiscardedError` (clean CONNACK a jeté
  l'inflight), erreur transport si reconnexion désactivée/épuisée,
  `ProtocolError` (reason ≥ 0x80).
- `receipt.wait()` est annulable ; l'annulation n'affecte pas la transaction
  protocolaire sous-jacente.

Taxonomie (module `errors`) : `MQTTError` → `MalformedPacketError`,
`ProtocolError` (→ `PacketTooLargeError`), `FlowControlError`,
`NotConnectedError`, `MQTTTimeoutError`, `SessionDiscardedError`.
Ne jamais masquer les builtins (`TimeoutError` a été renommé).

## 10. Backpressure (phase 2)

- File writer bornée en **octets** (`max_outbound_bytes`, défaut 1 MiB) et
  en messages ; `publish()` async attend la place (pas d'exception par
  défaut), mode `nowait` optionnel qui lève `FlowControlError`.
- `messages()` : file bornée (`max_pending_messages`, défaut 65536) ; si
  pleine, suspendre la lecture socket (backpressure TCP naturelle) plutôt
  que de jeter des messages.
- Payload ≥ 1 MiB : émettre `(header, payload)` sans concaténation
  (audit §18), le writer fait deux writes consécutifs sous sa boucle unique.

## 11. Points d'API restants (phase 1)

- `subscribe()` accepte `str | Iterable[tuple[str, SubscribeOptions]]` ;
  `SubscribeOptions(qos, no_local, retain_as_published, retain_handling)` ;
  encodage v5 de l'octet d'options : `qos | no_local<<2 | rap<<3 | rh<<4`.
  En v3.1.1, seuls `qos` est encodé ; options v5 → `ProtocolError`.
- Will : paramètres du constructeur (`will=Message(...)` + will properties
  v5, table §2). Payload format/expiry validés comme un PUBLISH.
- AUTH (0xF0) : `AuthPacket` + `AsyncClient.auth_handler` / `auth()` ; sans
  handler le moteur refuse avec DISCONNECT 0x8C (comportement stub historique).
- `messages()` doit remplacer le polling 0.5 s par un sentinel de fermeture
  poussé dans la file (dette phase 0 assumée).

## 12. Matrice de tests obligatoire (avant de déclarer une phase finie)

| Domaine | Cas |
|---|---|
| Codec properties | roundtrip chaque propriété ; dupliqué interdit ; paquet interdit ; longueur incohérente ; VBI subscription_identifier=0 |
| Négociation | chaque champ CONNACK appliqué ; publish > max_packet_size refusé ; qos > maximum_qos refusé ; retain refusé |
| Keepalive | PINGREQ à K d'inactivité sortante ; fermeture sur PINGRESP manquant ; server_keep_alive prioritaire ; K=0 désactivé |
| Reconnect | backoff+jitter borné ; codes terminaux stoppent ; receipts survivent session_present=1 ; échouent proprement sur clean |
| QoS matrix | perte de connexion à CHAQUE phase QoS2 (avant PUBLISH, après PUBLISH, après PUBREC, après PUBREL) × session_present 0/1 |
| Flow | fenêtre pleine → file ; refill après batch d'ACK ; receive_maximum CONNACK < local |
| Validation | topics/filtres §7, y compris `$share` |
| Concurrence | publish depuis on_message ; annulation de `connect()`/`wait()` ; deux clients dans la même loop |
| Fuzz (phase 2) | frames malformées ne crashent jamais le process : toujours `MalformedPacketError`/fermeture propre |

## 13. Definition of Done par phase

- **Phase 1** : sections §2–§9 et §11 implémentées ; matrice §12 (hors fuzz)
  verte ; test d'intégration réel contre Mosquitto (docker) pub/sub QoS 0/1/2
  v3.1.1 **et** v5 ; microbench ingress/egress établi comme baseline.
- **Phase 2** : §10 + WebSocket + fuzz ; comparaison A/B avec la baseline
  phase 1 (méthodologie de l'audit Paho : médianes, ABBA, seuils 5%/3%).
- **Phase 3** : façade Paho passant le sous-ensemble `tests/lib` ciblé ;
  décision de licence prise ; extraction vers le nouveau dépôt.
