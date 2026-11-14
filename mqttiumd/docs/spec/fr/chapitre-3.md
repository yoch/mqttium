**Version proposée 0.1 — 14 novembre 2026**

Ce chapitre définit la validation de l’architecture de référence de `mqttiumd` : admission et reprise avec P1, routage et retained avec P2, puis transport réel et saturation avec P3. Il complète le [chapitre 1 sur les garanties observables](https://chatgpt.com/space/page_a2d3db83cc088191b97e714ae72dd04a) et le [chapitre 2 sur le noyau concurrent](https://chatgpt.com/space/page_0c5f2f8425288191bffd4b424906cfac).

**Décision proposée : accepter l’architecture seulement sur des preuves reliées au code exécuté, des oracles indépendants, des fautes effectivement détectées et des mesures compatibles avec le contrat Durable.** Les résultats de correction, de progression, de performance et de qualification temporelle restent séparés.

Les tables, formats et seuils ci-dessous sont le contrat des bancs à construire. Aucun prototype, résultat de benchmark ni certificat n’est fourni par cette version. Les exemples de fichiers décrivent des interfaces à implémenter.

# 1 Décisions et niveaux de qualification

**V3-001 — Verdict par propriété et par profil.** Un résultat ne vaut que pour un contenu exact, un périmètre fonctionnel et un modèle de panne.

| Verdict | Condition |
| - | - |
| Réussi | Observations complètes, injections atteintes et toutes les attentes du scénario satisfaites |
| Échoué | Contre-exemple à une propriété ou service requis non fourni dans les conditions de l’essai |
| Inconclusif | Point d’injection non atteint, observation manquante, limite d’exploration ou environnement non maîtrisé |
| Hors périmètre | Exclusion préalable, motivée et compatible avec le profil annoncé |

Un résultat inconclusif ne devient pas réussi parce que le processus est encore vivant. Une option obligatoire du profil ne devient pas hors périmètre pour faire passer la campagne.

Quatre décisions sont distinctes : conserver R1 pour poursuivre les prototypes ; accepter ses invariants dans le périmètre vérifié ; qualifier un profil de production ; établir un dossier pour une certification identifiée. La qualification d’un compilateur ou une preuve locale n’équivaut pas à la certification du broker.

P1, P2 et P3 peuvent progresser pendant l’analyse D1, mais la combinaison Durable, MQTT 5 et expiration ne reçoit pas de qualification complète tant que D1 reste ouvert.

# 2 Dossier reproductible de chaque campagne

**V3-002 — Une preuve appartient à un contenu.** Chaque campagne conserve un manifeste et les commandes réellement exécutées.

| Domaine | Informations obligatoires |
| - | - |
| Produit | Dépôt, ref informative, SHA exact, arbre ou patch éventuel, empreinte du binaire |
| Construction | Compilateur, cible, features, profil, dépendances verrouillées, options natives et instrumentation |
| Banc | SHA de l’oracle, des injecteurs, du générateur et des modèles ; empreinte du corpus |
| Stockage | Version SQLite embarquée, options de compilation, paramètres effectifs relus, format du magasin |
| Environnement | CPU, mémoire, architecture, noyau, filesystem, support et firmware, limites système |
| Expérience | Configuration effective, workload, graines, ordonnanceur, injections, critères fixés avant exécution |
| Résultats | Verdicts, compteurs, traces, contre-exemples minimisés, rapports de preuve et limites atteintes |

Les révisions distantes sont relues avant l’investigation et avant la publication des résultats. Un nom de branche n’identifie pas le contenu testé. Après intégration, les preuves dépendantes portent sur le nouvel ensemble intégré.

Le manifeste expurge les secrets. Les versions complètes des configurations, le format des traces et le lien exigence–test–résultat restent inspectables. Un modèle exécuté avec une hypothèse différente constitue une autre campagne.

Chaque scénario associe au minimum une exigence des chapitres précédents, une transition, un oracle, un point d’injection et un résultat attendu. Une case vide dans cette chaîne bloque la déclaration correspondante.

# 3 Invariants communs

**V3-003 — Invariants vérifiés à chaque transition observable.** Le banc conserve ses propres identités de publications et de causes de livraison, distinctes des identifiants MQTT.

| Propriété | Assertion du projet |
| - | - |
| Engagement | Aucun ACK positif dépendant d’un commit n’est éligible avant sa confirmation |
| Conservation | Une obligation acceptée reste présente, achevée ou terminée par une cause autorisée |
| Identité | Une OperationId désigne une seule recette ; son rejeu ne recrée pas ses effets |
| Incertitude | Une issue inconnue ne vaut ni rejet certain ni permission de changer la recette |
| QoS 2 entrant | Un échange produit au plus une admission interne |
| Provenance | Supprimer la session productrice ne supprime pas les obligations d’autres sessions |
| Générations | Un ancien effet ne modifie ni la nouvelle connexion ni un slot réutilisé |
| Ressources | Comptes et réservations correspondent aux responsabilités ; aucune double libération |
| Ordre | Les dépendances requises survivent aux retours IO dans un ordre différent |
| Abonnements | Activation et retained partagent une coupure locale cohérente |
| Replay | Progression du cursor et création de ses obligations sont atomiques |
| Collecte | Supprimer une donnée ou un reçu ne permet pas sa recréation par un ancien message |
| Transport | Aucun entrelacement de trames ni répétition d’un préfixe sur la même connexion |
| Progression | Sous les hypothèses déclarées, le travail admissible avance sans croissance cachée |

L’oracle ne confond pas absence de seconde admission interne, retransmission protocolaire, copies dues à plusieurs abonnements et effet métier. Une comparaison portant seulement sur le nombre de payloads reçus est insuffisante.

Pour un lot fermé d’obligations, la conservation se vérifie par une partition exacte : encore actives, terminées par le protocole, ou terminées par une cause autorisée. Ces ensembles sont disjoints. Les publications refusées avant engagement ne sont pas ajoutées au lot.

# 4 Oracles indépendants et observations

**V3-004 — Trois sources d’observation.** La simulation exécute le véritable cœur Rust ; elle ne remplace pas ses transitions par le modèle utilisé pour calculer les attentes.

| Oracle | Construction proposée | Erreurs recherchées |
| - | - | - |
| Sémantique | Petit interprète indépendant, éventuellement Python, avec listes et historique explicite | Mauvais destinataire, ordre, reset, seconde admission, mauvaise cause de terminaison |
| Reprise | Lecture indépendante du magasin récupéré et recomptage des responsabilités | Transactions incomplètes au niveau métier, comptes erronés, perte de reçus ou de pins |
| Externe | Pairs de test et décodeur indépendants ; journal hors de la machine soumise à panne | ACK réellement observé, octets émis, retransmissions, fermeture et absence de livraison |

Le modèle de référence ne réutilise pas le trie de matching, les gardes d’identité ou les calculs de quotas du produit. Les schémas et types de transport peuvent être communs si leur rôle est purement descriptif.

Pour les comportements non déterministes licites, le vérificateur cherche une histoire autorisée compatible avec les observations et leur ordre causal. Il ne réordonne pas des paquets d’une même connexion et ne déplace pas un événement avant une réponse déjà observée pour sauver la trace. Les cuts wildcard restent locaux, conformément à R1.

Le banc peut connaître une vérité cachée, par exemple un commit effectué dont la réponse a été perdue. Cette information est réservée à l’oracle ; elle n’est pas injectée dans le cœur avant le mécanisme normal de réconciliation.

Un contrôle structurel du fichier SQLite ne prouve pas que les bonnes obligations ont été enregistrées. Inversement, une comparaison sémantique ne remplace pas les contrôles du moteur.

Le journal de test comprend événements externes, décisions d’injection, transitions projetées et reçus. La projection d’état du produit est contrôlée contre les effets externes ; elle ne constitue pas seule l’oracle.

# 5 Registre total des états et événements

**V3-005 — Complétude mécanique des sous-machines.** Le dépôt de validation doit contenir un registre lisible par machine. Chaque couple état et classe d’événement possède une règle, une garde et un résultat. Un validateur refuse les trous et les règles qui se recouvrent sans priorité explicite.

| Sous-machine | États minimaux à distinguer |
| - | - |
| Commande | Prepared, Submitted, Indeterminate, Committed, RejectedCertain, Consumed, ReceiptClosed |
| QoS 2 entrant | Absent, AdmissionPending, AwaitPubrel, ReleasePending |
| QoS 1 sortant | Queued, ClaimPending, AwaitPuback, FinishPending, Terminal |
| QoS 2 sortant | Queued, ClaimPending, AwaitPubrec, PubrelPersistPending, AwaitPubcomp, FinishPending, Terminal |
| Trame | NotEligible, Eligible, QueuedForWriter, NeverStarted, MayHaveStarted, LocalWriteCompleted |
| Session | Active, DisconnectedResumable, ResetPending, Ended |
| Abonnement | Prepared, IntentCommitted, PartiallyActive, FullyActive, Ending, Ended |
| Replay | Pinned, ReadingPage, PageCommitPending, Advancing, Completed |
| Will | Armed, CancelPending, TriggerPending, PendingAdmission, Admitted, Terminal |

Les connaissances sur l’émission, la génération de connexion et l’issue du commit sont des dimensions distinctes. Elles ne doivent pas être fusionnées dans un unique booléen « envoyé ».

Les classes d’événements couvrent paquet valide, paquet invalide, numéro connu ou inconnu, bonne ou mauvaise phase, résultat courant ou ancien, commit confirmé ou refusé ou inconnu, manque de capacité, reset, reprise, timer, annulation, crash et redémarrage. Les données numériques sont partitionnées autour des limites : 0, 1, maximum moins 1, maximum et maximum plus 1 lorsque représentable.

Chaque règle fournit précondition, nouvel état, effets autorisés et interdits, coût, libération éventuelle, dépendances d’ordre et lien au test. Une règle générique « ignorer le reste » est interdite sans classe précise et justification.

Les espaces de Packet Identifiers entrants et sortants sont séparés ; le même numéro peut exister simultanément dans les deux sens. Dans un même sens, les usages concurrents incompatibles restent interdits. Ce point est vérifié par un scénario positif et un scénario de collision ([MQTT 5, section 2.2.1](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)).

Les tables ci-dessous fixent les transitions discriminantes des prototypes. Le registre total du sous-ensemble exécuté est un livrable obligatoire avant son passage ; elles ne prétendent pas remplacer toutes les clauses des deux versions MQTT.

# 6 Contrat des commandes et refus définitifs

**V3-006 — Résultat stable pour une identité.**

| Situation | Événement | Décision exigée |
| - | - | - |
| Prepared | Soumission | Geler recette, préconditions et OperationId |
| Submitted | Commit confirmé | Installer résultat et versions ; autoriser les effets dépendants |
| Submitted | Rejet certain | Aucun effet de cette tentative ; fermer son identité d’exécution |
| Submitted | Timeout ou réponse perdue | Passer à Indeterminate sans libérer les responsabilités possibles |
| Indeterminate | Réconciliation positive | Retrouver le même résultat et la même recette |
| Indeterminate | Absence d’effet établie | Fermer la tentative ; nouvelle préparation sous une nouvelle identité si utile |
| Committed | Répétition de commande | Même résultat ou classification fermée sûre, jamais seconde exécution |
| RejectedCertain | Ancienne commande après changement d’état | Rester refusée ou classée fermée ; ne pas devenir une admission |
| ReceiptClosed | Ancienne commande | Interdire sa réexécution sans inventer un résultat précis déjà collecté |

Le refus certain devient terminal pour cette OperationId. La protection peut utiliser reçu, tombstone ou clôture durable d’un espace d’identités. Elle n’impose pas de réussir une écriture supplémentaire sur un support déjà incapable d’écrire.

Si le refus n’est mémorisable qu’en RAM, le processus clôt durablement l’ancienne époque avant de rouvrir les admissions après reprise. Sans cette clôture, il reste fermé. **Une clôture interdit de réexécuter ; elle ne prouve pas qu’une commande anciennement indéterminée avait été rejetée.** Les engagements possibles doivent toujours être réconciliés.

Un retour d’erreur SQLite n’est pas traduit mécaniquement en « aucune écriture ». L’adaptateur qualifie l’état de la transaction et les effets possibles ; certaines erreurs laissent une transaction active. Le cas COMMIT avec SQLITE_BUSY figure dans la [documentation des transactions](https://www.sqlite.org/lang_transaction.html). La classification s’appuie sur l’état réellement établi, sinon elle reste indéterminée.

Ce complément précise ARC-005 et ARC-019. Les tests couvrent refus sans reçu durable, clôture d’époque, ancienne réponse positive et ancienne tentative refusée rejouée après libération de quota.

# 7 P1 et transitions critiques

**V3-007 — Même cœur sur deux magasins.** P1 exécute successivement le cœur avec magasin déterministe, le même cœur avec SQLite réel, puis un adaptateur réseau contrôlable. Une preuve pilote porte sur une fonction réellement utilisée.

| Machine et événement | Effet attendu | Effet interdit |
| - | - | - |
| Entrant Absent, nouveau QoS 2 | Créer l’échange et préparer l’admission | PUBREC positif immédiat |
| AdmissionPending, commit | Installer AwaitPubrel ; rendre distribution et PUBREC éligibles | Attendre PUBREL pour créer les obligations |
| AdmissionPending, issue inconnue | Réconcilier la commande immutable | Refaire le matching sous la même identité |
| AwaitPubrel, PUBLISH du même échange | Réponse du protocole, sans nouvelle admission | Seconde création de publication |
| AwaitPubrel, PUBREL | Engager la fin de réception | Recalculer les destinataires |
| ReleasePending, commit | Autoriser PUBCOMP et libération correcte | Terminer un nouvel ExchangeId |
| Sortant Queued, claim | Réserver slot et phase récupérable | PUBLISH avant conservation requise |
| AwaitPubrec, PUBREC positif | Engager la phase PUBREL | Libération anticipée du numéro ou crédit |
| PubrelPersistPending, commit | Autoriser PUBREL | Retour à une première émission PUBLISH |
| AwaitPubcomp, PUBCOMP | Engager la terminaison | Double restitution de ressources |
| Toute phase, callback ancien | Réconcilier l’opération si nécessaire ; appliquer ses gardes | Agir sur une nouvelle socket ou un nouvel échange |

Les paquets reçus dans une mauvaise phase et les répétitions pendant un commit ont des lignes propres dans le registre total. La comparaison du payload n’est pas une identité générale de message. Les références normatives MQTT 3.1.1 et MQTT 5 restent séparées ; les branches d’erreur ne sont pas extrapolées de l’une à l’autre.

# 8 Corpus obligatoire de P1

**V3-008 — Pannes atteintes et attentes discriminantes.** Chaque cas s’exécute sans panne, avec panne à chaque frontière pertinente, puis avec la mutation indiquée dans le registre de mutations.

| Cas | Trace ciblée | Attente principale |
| - | - | - |
| P1-01 | Couper avant admission, pendant transaction et après commit avant ACK | Aucun succès sans responsabilités ; aucune responsabilité fantôme sur refus certain |
| P1-02 | Commit effectué, retour perdu, reconnexion et réconciliation | Même admission et même résultat |
| P1-03 | PUBREC reçu, crash, reprise du producteur avec seulement PUBREL | Fin sans redemander le payload ni refaire le routage |
| P1-04 | Détruire la session productrice après engagement | Conserver les obligations d’autres sessions |
| P1-05 | Changer les cibles pendant une issue inconnue, puis après un refus certain | Ancienne recette dans le premier cas ; nouvelle OperationId dans le second |
| P1-06 | Réutiliser PacketId puis livrer un ancien callback | Le nouvel ExchangeId reste inchangé |
| P1-07 | Utiliser le même PacketId simultanément dans les deux sens | Deux échanges indépendants progressent |
| P1-08 | Terminer les commits de deux entrées dans l’ordre inverse | Les contraintes d’ACK restent satisfaites |
| P1-09 | Reprendre après engagement de la phase PUBREL | Aucune régression de phase |
| P1-10 | Dupliquer résultat et libération ; rejouer après collecte | Une seule application, comptes exacts |
| P1-11 | Refus certain, libération de quota, ancien OperationId rejoué | L’ancienne tentative ne devient pas acceptée |
| P1-12 | Saturer la fenêtre de reçus, puis rendre les confirmations | Limitation de l’origine, collecte sûre et reprise |
| P1-13 | Reset et admission dans les deux ordres ; reprise sous un autre nombre de workers | SessionId correcte ; identité indépendante des threads |
| P1-14 | Deux histoires D1 avec disque identique et émission différente | Incertitude conservée et interprétation qualifiée séparément |
| P1-15 | QoS 1, perte d’ACK puis réutilisation licite du numéro | Aucune déduplication applicative inventée |
| P1-16 | Petit paquet maximal accepté, maximum plus 1, destinataire plus contraint | Décision explicite sans allocation non bornée ni faux succès |
| P1-17 | Refus terminal du destinataire et résultat de terminaison retardé | Motif correct, pas de rejeu interdit, crédits libérés une fois |
| P1-18 | ACK retardé sur connexion vivante, puis reprise de session | Contrôle de flux et règles de rejeu de la version négociée |
| P1-19 | TurnGrant ou capacité d’effets insuffisants à chaque transition | Entrée reprenable, aucune transition partielle ni ressource perdue |

Les préconditions fixent notamment la persistance des sessions, les expirations désactivées ou contrôlées, les droits et les capacités. Une obligation terminée par un reset volontaire ne doit pas être comptée comme une perte inexpliquée ; un reset injecté ne doit pas non plus masquer une perte antérieure.

# 9 D1 et les histoires indiscernables

**V3-009 — Qualification de la frontière d’émission.** MQTT 5 lie l’expiration au début de livraison et interdit l’expiration d’un échange QoS 2 après envoi de PUBLISH. Le texte ne désigne pas directement un appel fsync, TLS ou write comme frontière d’implémentation ([sections 3.3.2.3.3 et 4.3.3](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)).

Construire une obligation sortante QoS 2, une session persistante et une échéance future. Engager l’intention récupérable d’émission, puis réaliser deux exécutions :

| Histoire | Événement externe avant panne | État durable à la reprise |
| - | - | - |
| A | Aucune écriture réseau | Intention engagée, pas de confirmation ultérieure |
| B | PUBLISH complet observé par le pair ; PUBREC retenu ou perdu | La même intention, sans confirmation ultérieure |

Reprendre après l’échéance. Le banc conserve les observations différentes, mais le broker ne reçoit que l’information qu’il peut réellement reconstruire. Vérifier l’équivalence des données durables significatives ; une copie du même snapshot de test peut assurer cette condition.

Avec une définition strictement physique de l’envoi, une décision unique fondée sur cet état ne peut satisfaire les deux attentes opposées. C’est une limite du modèle d’observation choisi, pas une conclusion générale d’impossibilité de MQTT.

La politique candidate préserve l’échange MayHaveStarted. Elle protège sa continuité, sans prouver à elle seule une conformité temporelle physique exacte. NeverStarted encore établi, MayHaveStarted, écriture locale terminée et acquittement protocolaire restent distincts.

D1 est fermé seulement avec une définition de DeliveryStart et PacketSent, leur relation au transport TCP et TLS, l’analyse des deux histoires, le traitement des propriétés d’expiration au rejeu, les hypothèses de reprise et une revue normative indépendante. Renommer Prepared en Sent n’est pas une justification.

Si l’analyse impose une interprétation ou une restriction du profil, celle-ci est inscrite explicitement dans le contrat. Le support de l’expiration ne peut pas être retiré silencieusement pour qualifier la compatibilité annoncée.

# 10 P2 et causes de livraison

**V3-010 — Comparer les responsabilités et leurs causes.** L’oracle P2 conserve un journal simple de décisions locales, des incarnations de sessions et un historique retained complet pour les petits domaines de test.

| Cause | Identité sémantique minimale |
| - | - |
| Live ordinaire | Publication, destination, révisions de filtres qui justifient la copie |
| Live partagé | Publication, filtre partagé complet, destination sélectionnée |
| Replay retained | Révision d’abonnement, partition, cut, topic et version retained |
| Will admis | Publication interne stable, puis causes de distribution ordinaires ou partagées |

Deux payloads identiques ne prouvent pas deux admissions identiques. Une publication peut légitimement participer à plusieurs causes ; une même cause engagée ne doit pas être recréée après reprise.

Les propriétés P2 sont : admission indivisible ; destination attachée à SessionId ; activation derrière un front résolu ; replay d’une version au cut promis ; conservation des pins ; progression atomique du cursor ; notification dépourvue d’autorité de création ; collecte sans résurrection ; comptes exacts.

Les clocks sont fiables et les expirations neutralisées dans les traces non temporelles. Les scénarios dédiés réintroduisent ensuite l’expiration afin de vérifier qu’elle ne ressuscite jamais une ancienne version.

# 11 Activation et retained à vérifier

**V3-011 — États d’activation et de replay.**

| État ou événement | Résultat exigé |
| - | - |
| PREPARE échoue | Aucune activation ; réservations libérées exactement une fois |
| Intention engagée | Opération à compléter tant que la session reste valide |
| Admission antérieure indéterminée | Activation locale empêchée de franchir sa frontière |
| Activation locale engagée | Révision, cut, cursor, pin et complétion cohérents |
| Projection mémoire non encore installée | Aucune admission dépendante ne saute cette installation |
| Quelques partitions activées puis crash | Reprise des activations restantes |
| Toutes activées | SUBACK devient éligible sans attendre la consommation du replay |
| Remplacement de filtre | Ancienne définition jusqu’à la nouvelle activation locale |
| Page de replay engagée | Obligations et avancement du cursor visibles ensemble |
| Dernière page terminée | Libération du pin après engagement de la progression |
| Session terminée | Nettoyage justifié et paginé, sans abandon caché d’autres sessions |

L’oracle impose les cuts locaux du wildcard et la complétion après toutes les partitions. Il ne réclame pas une linéarisation globale absente du contrat.

Pour la collecte retained, le modèle conserve une version fermée si un cut actif appartient à son intervalle de validité. Il garde séparément la valeur courante. Les frontières exactes de l’intervalle, les suppressions, la dernière version et les pins multiples possèdent des tests.

Un GC ayant lu une ancienne liste de pins doit revalider sa décision avant suppression. Un cursor ne peut pas demander une coupure historique déjà collectée. Une page SQL de lecture reste courte ; la lenteur du client ne conserve pas une transaction de lecture ouverte.

# 12 Corpus obligatoire de P2

**V3-012 — Traces de concurrence minimales.**

| Cas | Ordonnancement imposé | Attente discriminante |
| - | - | - |
| P2-01 | Préparer pour S0, reset, créer S1 avec même ClientID, tenter commit | Refus de la cible périmée ; aucune substitution par S1 |
| P2-02 | Commit vers S0, puis reset et création S1 | Obligation de S0 puis terminaison justifiée |
| P2-03 | Fanout engagé, source supprimée, B terminé et C encore en attente, crash | B non recréé ; C récupéré |
| P2-04 | Publication retained préparée sur ancien index, commit retardé, activation demandée | Attendre sa résolution ; aucune valeur manquée à la fois en replay et live |
| P2-05 | Intention wildcard engagée, A activée, crash avant B | Complétion de B ; pas de refus rétroactif sans effet |
| P2-06 | Activer A, publier sur A et B, activer B | Évaluer chaque publication selon le cut de sa partition |
| P2-07 | Remplacer puis supprimer un filtre pendant le trafic | Pas de trou de remplacement ; obligations antérieures traitées selon le contrat |
| P2-08 | Capturer V1, remplacer par V2 avant parcours | V1 au replay, V2 au live si admissible |
| P2-09 | Capturer V1, effacer retained avant parcours | V1 accessible à l’ancien cut ; absence pour un cut postérieur |
| P2-10 | Crash autour de l’engagement d’une page de replay | Cursor et obligations ensemble, jamais l’un seul |
| P2-11 | GC lit les pins, nouveau pin engagé, version fermée, GC reprend | Revalidation protégeant la version requise |
| P2-12 | Pin sur V1, puis V2, V3 et V4 sans autre cut | V1 et courant protégés ; intermédiaires sans usage collectables |
| P2-13 | Perdre tous les réveils après commit ; arrêter les nouvelles entrées | Découverte par scan équitable |
| P2-14 | Réveil après fin et collecte | Aucun effet nouveau |
| P2-15 | Perdre le retour de commit, terminer et collecter ses livraisons, rejouer commande | Résultat ou clôture sûre ; aucune seconde admission |
| P2-16 | Refus pour quota, quota libéré, ancienne commande rejouée | Refus terminal ou espace fermé |
| P2-17 | Dupliquer fin de livraison et retour de crédit | Une seule restitution |
| P2-18 | Redémarrer avec 1 puis plusieurs workers | Même état logique |
| P2-19 | Cible pleine et flux indépendant sur le même propriétaire | Progression du second flux et des contrôles |
| P2-20 | Recouvrements, options différentes, groupes partagés et replay | Causes et attributs corrects, sans déduplication excessive |
| P2-21 | Membres partagés hors ligne, puis tous à capacité, puis aucun membre valide | Distinguer file admissible, saturation et absence de destinataire |
| P2-22 | QoS 2 partagé commencé, session cible terminée ; variante avec refus terminal | Aucune réaffectation interdite ni seconde cause créée |
| P2-23 | Fanout vers B admissible et C sans capacité, puis place libérée | Aucun engagement partiel de la tentative refusée ; nouvelle tentative cohérente |

P2-04 comporte deux branches : ancien commit confirmé, puis ancien commit certainement abandonné suivi d’une nouvelle préparation. P2-08 et P2-09 incluent les crashes entre activation, remplacement, lecture et matérialisation de page.

Chaque trace est rejouée avec les retours IO retardés, dupliqués ou perdus aux frontières pertinentes. Le générateur préserve les préconditions ; il ne produit pas un cartésien indiscriminé d’injections impossibles.

P2-20 comprend une recréation de session avec le même ClientID pour éprouver NoLocal, ainsi que des Subscription Identifiers identiques associés à des filtres différents.

# 13 Politiques complémentaires de routage

**V3-013 — Fixer les choix avant de comparer les traces.** Les trois précisions de cette section complètent la référence ; leur validation ne repose pas sur une similitude fortuite avec un broker tiers.

**Recouvrement.** Construire les causes par filtre et révision ; appliquer les critères d’éligibilité avant regroupement. La politique candidate regroupe le live ordinaire seulement lorsque les attributs de sortie sont compatibles, notamment le bit RETAIN calculé. Le QoS et les identifiants d’abonnement proviennent des causes réellement retenues, avec leur multiplicité. Un replay et une livraison partagée gardent leurs propres causes.

L’oracle représente d’abord les causes séparément puis contrôle les regroupements. Les combinaisons NoLocal, Retain As Published, Retain Handling, QoS et identifiants font l’objet de tables dédiées. Une optimisation « une copie par session » n’est pas qualifiée tant que ces tables ne sont pas fermées. Cette politique de copies doit également être comparée au profil Mosquitto à remplacer. Les règles de recouvrement sont ancrées dans [MQTT 5, sections 3.3.4, 3.8.3 et 3.8.4](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).

**Groupes partagés.** La clé comprend le filtre partagé complet. Pour le profil Durable QoS 1/2, la référence proposée préfère un membre connecté admissible, puis une session persistante hors ligne admissible. Le choix local est enregistré dans l’admission ; une limite d’émission n’est pas confondue avec une limite de file durable.

| Situation du groupe à la décision | Politique proposée |
| - | - |
| Aucun membre valide | Aucune obligation pour ce groupe |
| Membres exclus par politique | Motif distinct, selon la frontière de droits qualifiée |
| Membres valides mais capacités insuffisantes | Attente bornée ou refus de l’admission entière avant acceptation |
| Résultat indéterminé | Même choix et même recette jusqu’à réconciliation |
| Livraison engagée | Réaffectation seulement selon règle explicite de QoS et de phase |

Un groupe saturé ne disparaît pas silencieusement du fanout pour permettre un ACK positif. Les variantes QoS 0 suivent leur politique distincte. L’équité est testée localement ; aucune équité globale parfaite n’est promise.

**Refus d’opération.** La stabilité du refus est fixée par V3-006. Elle s’applique aux admissions, activations et opérations de collecte conditionnelles.

# 14 P3 et progression réelle

**V3-014 — Service fourni dans les limites annoncées.** P3 exécute les adaptateurs instrumentés, puis TCP et TLS réels. Il vérifie à la fois conservation, progression et bornes.

Deux classes d’essais sont obligatoires. En charge admissible, le service doit progresser sans refus ou déconnexion artificiels. En dépassement explicite, le refus ou la fermeture prévus sont acceptables si les responsabilités antérieures restent correctes. Ces classes sont marquées avant exécution.

Une simulation mesure la progression en pas sous ordonnanceur contrôlé. Le runtime réel utilise les délais du profil expérimental. Un timeout produit une trace des attentes, capacités et événements possibles ; il ne prouve pas seul un deadlock.

Les capacités réservées couvrent retour de commit, fin de livraison, fermeture et libération de crédits. Une file logique de contrôle ne résout pas à elle seule le blocage des octets sur TCP.

Les garanties d’équité [du runtime Tokio](https://docs.rs/tokio/latest/tokio/runtime/index.html) ont des hypothèses sur les tâches et leur durée de poll. Le banc recherche les tours non bornés, tâches non comptées et attentes qui empêchent le traitement de leurs propres résultats.

# 15 Corpus obligatoire de P3

**V3-015 — Transport et saturation adversariaux.**

| Cas | Préparation et perturbation | Attente principale |
| - | - | - |
| P3-01 | Données pleines, gros PUBLISH entrant suivi de l’ACK d’une livraison sortante | Atteindre l’ACK avec staging borné ; fermeture seulement dans la classe de dépassement prévue |
| P3-02 | Écriture de quelques octets, blocage, contrôle prioritaire, annulation | Paquets complets sans préfixe répété ; priorité entre trames |
| P3-03 | Fragmenter en-têtes, longueurs, propriétés et payload ; EOF et reset | Décision cohérente et mémoire bornée |
| P3-04 | Saturer données et retarder retours de commit et de crédit | Les résultats réservés permettent les terminaisons |
| P3-05 | Destinataire qui ne lit plus ; autre flux indépendant | Contre-pression ciblée ; progression indépendante et reprise correcte |
| P3-06 | Publication vers sa propre session, quotas presque pleins | Aucune attente de soi-même bloquant son propriétaire |
| P3-07 | Annuler le demandeur avant, pendant et après commit ; reconnecter | Réconciliation et gardes maintenues |
| P3-08 | Pool d’authentification plein, demandeurs déconnectés | Travaux bornés ; sessions établies disponibles ; aucune autorisation périmée |
| P3-09 | Handshakes TLS incomplets, clients lents et plafond de connexions | Nettoyage borné, refus prévu, validation des certificats conservée |
| P3-10 | Wills déclenchés sous saturation, crash, reconnexion tardive et capacité libérée | Intentions conservées et admission équitable sans seconde création |
| P3-11 | Churn de clients, topics, filtres, timers, erreurs et métriques | Comptes exacts et caches bornés après retour au même état logique |
| P3-12 | Quotas atteints puis ENOSPC réel, checkpoint et replay lents | Aucune perte acceptée ni bascule implicite en Volatile |
| P3-13 | Panic d’un propriétaire avec effets en cours, reprise avec d’autres workers | Récupération cohérente de l’ensemble |
| P3-14 | Reload invalide puis révocation valide pendant le trafic | Ancienne politique conservée ou nouvelle frontière appliquée sans élargissement implicite |
| P3-15 | Horloge recule ou devient inconnue au redémarrage | État temporel dégradé explicite ; aucune expiration massive inventée |

P3-01 comporte une variante dans les budgets normaux où fermer la connexion est un échec. Sa variante hors capacité teste la fermeture prévue. Le banc crée effectivement la dépendance entre ACK inaccessible et ressource attendue.

P3-02 conserve des curseurs d’octets. Avec TLS, plaintext accepté, ciphertext écrit et paquet observé sont des événements différents. Les contrôles ne traversent pas le milieu d’un paquet déjà commencé.

Les frontières d’annulation de [select](https://docs.rs/tokio/latest/tokio/macro.select.html) et le travail déjà lancé par [spawn_blocking](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html) doivent être couverts sans assimiler abandon d’une future et rollback externe.

P3-10 vérifie séparément annulation admissible avant déclenchement, intention déclenchée autonome et admission différée. La compatibilité temporelle du Will et la reconstruction d’une déconnexion survenue pendant l’arrêt constituent une décision de qualification propre, T1. Une simple reprise sans doublon ne ferme pas T1.

# 16 Modèle de panne du stockage

**V3-016 — Qualifier chaque niveau de panne séparément.**

| Niveau | Injection | Portée du résultat |
| - | - | - |
| Commande | Réponse retardée, perdue, dupliquée ou indéterminée | Contrat d’opérations et réconciliation |
| Processus | Arrêt brutal à des frontières identifiées | Perte de la RAM du broker |
| Stockage simulé | VFS distinguant écritures volatiles et stables | Modèle explicitement choisi de perte et réordonnancement |
| Système | Crash du système ou arrêt de VM avec pile documentée | Environnement réellement perturbé |
| Matériel | Coupures contrôlées du support ou de la machine de qualification | Couple matériel, firmware, filesystem et configuration essayé |

Un SIGKILL ne supprime pas le cache du système. Un arrêt de VM peut laisser intact celui de l’hôte. Le banc conserve les ACK observés et ses attentes sur un témoin indépendant de la machine arrêtée.

Les frontières ciblées sont : avant commande, après écritures avant synchronisation, pendant synchronisation, après engagement avant retour, après retour avant effet réseau, après émission avant confirmation persistante, puis pendant la reprise elle-même.

La VFS de test doit respecter les hypothèses qu’elle prétend explorer : les données stables ne sont pas arbitrairement corrompues dans une campagne de simple perte d’écritures non synchronisées. Corruption volontaire de données stables, support détruit et synchronisation mensongère sont des campagnes distinctes.

SQLite emploie notamment des simulations d’erreurs IO et de crash dans ses [propres tests](https://www.sqlite.org/testing.html). Le protocole `mqttiumd` doit tester en plus ses commandes métier et l’adaptateur. Les [hypothèses de synchronisation](https://www.sqlite.org/atomiccommit.html) et le mécanisme [WAL](https://www.sqlite.org/wal.html) restent explicites.

Après reprise, vérifier intégrité du moteur, responsabilités, comptes, reçus, pins, époques et disponibilité. Une corruption détectée ne déclenche jamais un démarrage silencieux avec magasin vide. Une erreur d’écriture après engagement ne justifie ni effacement ni passage en Volatile.

La campagne matérielle est préparée sur des données de test et sur les supports du profil. Ses fréquences et nombres de coupures sont des données d’essai, pas une estimation automatique d’un taux de panne résiduel.

# 17 Ordonnancement et format des traces

**V3-017 — Une trace doit être rejouable sans dépendre seulement de sa graine.** L’ordonnanceur contrôle livraison des entrées, décisions de stockage, retours, timers, progression d’octets et crashes.

Le journal enregistre le choix effectif, les identités, les gardes et le point d’injection atteint. Une graine seule ne suffit plus si le générateur ou l’ordre des choix change.

Exemple de contrat déclaratif à implémenter :

```yaml
id: P1-02
profile: durable_mqtt5_without_expiry
preconditions:
  producer_session: persistent
  subscriber_session: persistent
  delivery_capacity: available
schedule:
  - receive_publish_qos2
  - commit_admission
  - drop_commit_response
  - crash_process
  - recover_store
  - reconcile_same_operation
assertions:
  internal_admissions: 1
  required_obligations: preserved
  changed_recipe_under_same_id: forbidden
  ack_before_confirmed_commit: forbidden
evidence:
  injection_reached: required
  store_snapshot: required
  observed_packets: required
```

La minimisation supprime événements, données et entités inutiles tout en préservant les préconditions et la faute. Une trace réduite doit reproduire le même invariant violé. Le corpus conserve aussi la trace originale.

Chaque transition critique possède un témoin positif, un témoin négatif pour chacune de ses gardes essentielles et une mutation de cette garde. Une combinaison déclarée inaccessible porte un invariant et sa justification, pas seulement un filtre du générateur.

Les ordonnancements aléatoires complètent les cas imposés. Ils ne remplacent pas les fenêtres étroites qui doivent être atteintes de façon déterministe.

# 18 Modèles de concurrence et vivacité

**V3-018 — Plusieurs petits modèles reliés.** Les domaines ci-dessous sont des configurations initiales de recherche, à ajuster explicitement selon les contre-exemples et le coût d’exploration.

| Modèle | Domaine initial proposé | Propriétés |
| - | - | - |
| M1 Admission et reset | 2 publications, 2 destinations, 2 incarnations, 3 issues de stockage | Atomicité, conservation et cible périmée |
| M2 Échanges et identités | 2 numéros par sens, 2 générations, 2 échanges successifs par slot | Rejeu, phases, callbacks anciens et sens indépendants |
| M3 Activation et retained | 2 partitions, 3 topics, 2 révisions, 2 cuts, 4 versions d’une valeur | Front résolu, replay et GC |
| M4 Reçus et réveils | 2 origines, séquences 1 à 3, fenêtre de 2 reçus, 2 obligations | Trous de confirmation, collecte et découverte |
| M5 Ressources | 3 propriétaires, capacités données 0 à 2, contrôle réservé, 2 continuations | Conservation des crédits et absence d’attente circulaire |
| M6 Temps et émission | Temps avant, à et après échéance ; 2 histoires réseau ; crash | D1, timers périmés et connaissances disponibles |

Les bornes de crash et de renouvellement d’identité sont explicites. Arrêter la génération après un lot fini permet d’étudier son drainage sans masquer la vivacité par une charge infinie. Les compteurs réduits ne doivent pas introduire une réutilisation d’identité absente du produit ; les vrais débordements sont testés séparément.

[TLC](https://lamport.azurewebsites.net/tla/tools.html) vérifie les invariants et les propriétés temporelles des modèles exécutables. Chaque rapport indique domaine, contraintes, actions désactivées, états explorés, équité, mémoire, temps, statut de fin et paramètres de réduction. Une exploration interrompue est inconclusive.

La vivacité suppose une période stable : stockage utilisable, nombre fini de resets, ordonnanceur équitable, IO qui terminent et pairs permettant la progression concernée. Une propriété porte par exemple sur la complétion d’une intention valide, la découverte malgré perte de réveil, la collecte d’une ressource devenue inutile ou la réconciliation d’une issue inconnue.

Les contraintes d’équité doivent correspondre au runtime proposé. L’équité forte n’est pas ajoutée pour éliminer artificiellement un contre-exemple. Les campagnes de vivacité n’utilisent pas de réduction par symétrie non justifiée ; la [documentation TLC](https://tla.msr-inria.inria.fr/tlatoolbox/doc/model/model-values.html) déconseille ces ensembles pour la vivacité.

Une vérification de petits domaines ne constitue pas une preuve de tous les nombres de clients. Le dossier donne le lien de raffinement entre actions abstraites et transitions réelles, ainsi que les propriétés laissées aux essais système.

# 19 Preuves sur le code et base de confiance

**V3-019 — Une preuve pilote utile et maintenable.** P1 choisit une propriété centrale du code réellement exécuté : conservation des crédits et gardes de génération, ou clôture sûre des reçus. Le choix doit protéger une transition utilisée en production, pas une copie illustrative.

La preuve pilote exige préconditions visibles, postconditions reliées à V3-003, correspondance avec la compilation livrée, inventaire des hypothèses et test mutant. Une modification normale de cette fonction est réalisée pour mesurer le coût de maintenance de la preuve. Le résultat inclut temps humain de revue, temps outil et adaptations nécessaires.

| Moyen | Usage retenu | Limite à publier |
| - | - | - |
| Verus | Propriétés d’un sous-ensemble synchrone réel | Fonctions exclues, hypothèses et prise en charge du langage |
| Kani | Exploration bornée de code séquentiel et de valeurs limites | Bornes, assertions de déroulage et chemins non pris en charge |
| Loom | Petits adaptateurs concurrents instrumentés | Types instrumentés, préemptions et limites de modèle mémoire |
| Simulation du cœur | Entrelacements, pannes et temps contrôlés | Modèle d’IO et abstraction du matériel |
| Tests système | Runtime, stockage et réseau réels | Matériel, couverture de fautes et reproductibilité |

Les limites publiées de [Verus](https://verus-lang.github.io/verus/guide/features.html), [Kani](https://model-checking.github.io/kani/rust-feature-support.html) et [Loom](https://docs.rs/loom/latest/loom/) interdisent de déduire une preuve du broker entier de leur seule présence dans la CI.

Le cœur propre au projet interdit unsafe. Toute exception, dépendance native, appel non vérifié, axiomatisation ou corps exclu reste dans la base de confiance. Les hypothèses ajoutées pour obtenir une preuve sont revues comme des modifications du contrat.

Un outil inadapté peut être remplacé après un essai documenté. L’obligation de fournir une preuve pilote sur du code exécuté reste un critère distinct ; une suite de tests n’est pas renommée preuve formelle.

# 20 Fuzzing du protocole et du logiciel

**V3-020 — Fuzzing syntaxique et sémantique.**

| Cible | Entrées | Oracle principal |
| - | - | - |
| Codec | Octets tronqués, longueurs, UTF-8, propriétés, tailles limites | Absence de panic ou allocation démesurée ; acceptation ou rejet correct |
| Machines QoS | Séquences de paquets et reconnexions | Registre de transitions et obligations |
| Routage | Noms, filtres, recouvrements et changements | Matcher de référence simple et causes |
| Stockage | Commandes, erreurs, reprise, collecte | Registre indépendant et intégrité récupérée |
| Runtime | Fragments IO, annulations, retours tardifs | Curseurs et responsabilités |
| Configuration et droits | Includes, directives, ACL, reload, valeurs limites | Classification sans option ignorée ni élargissement de droits |
| Temps et Will | Ordres d’échéances, resets, horloges dégradées | Identités et décisions temporelles annoncées |

Le [fuzzing structuré](https://rust-fuzz.github.io/book/cargo-fuzz/structure-aware-fuzzing.html) sert à atteindre des phases profondes ; le fuzzing d’octets conserve les entrées malformées. Chaque cible reçoit des graines issues des traces minimales et des limites connues.

Les assertions d’invariants s’exécutent pendant le fuzzing. Un objectif « pas de crash » manque les pertes, doubles admissions et refus abusifs.

Les builds instrumentés et les builds de production restent distingués. Les features, sanitizers et différences dues à cfg(fuzzing) sont enregistrés ; ce mécanisme peut modifier les chemins compilés, comme l’explique le [guide cargo-fuzz](https://rust-fuzz.github.io/book/cargo-fuzz/guide.html). Les chemins retirés n’héritent pas de la qualification.

Mesurer couverture de transitions, gardes, branches pertinentes et atteinte des injections. Une couverture élevée ne remplace pas la sensibilité de l’oracle. Toute nouvelle faute confirmée produit une régression minimisée et un examen des cas voisins justifié par son mécanisme.

# 21 Mutations critiques du banc

**V3-021 — Le banc doit détecter les défauts qu’il prétend prévenir.**

| Mutation | Faute injectée | Scénarios associés |
| - | - | - |
| MU-01 | ACK éligible avant commit | P1-01 à P1-03 |
| MU-02 | Issue indéterminée traitée comme rejet | P1-02, P1-05 |
| MU-03 | Recette changée sous OperationId | P1-05 |
| MU-04 | Garde ExchangeId ou génération omise | P1-06, P3-07 |
| MU-05 | Crédit ou numéro QoS 2 libéré trop tôt | P1-07, P1-09, P1-10 |
| MU-06 | Nouvelle publication QoS 1 dédupliquée par numéro | P1-15 |
| MU-07 | Suppression en cascade depuis la source | P1-04, P2-03 |
| MU-08 | Refus ancien réexécuté après libération de quota | P1-11, P2-16 |
| MU-09 | Reçu collecté avant clôture sûre | P1-12, P2-15 |
| MU-10 | Cut fondé sur réception plutôt que front résolu | P2-04 |
| MU-11 | Activation partielle déclarée annulée sans effet | P2-05 |
| MU-12 | Lecture du seul retained courant | P2-08, P2-09 |
| MU-13 | Cursor avancé séparément des obligations | P2-10 |
| MU-14 | GC sans revalidation des pins | P2-11 |
| MU-15 | Toutes les versions après minCut conservées | P2-12 et stress de budget |
| MU-16 | Rattrapage dépendant d’un nouveau paquet | P2-13 |
| MU-17 | Double libération effective | P1-10, P2-17 |
| MU-18 | Contrôle consommant la dernière capacité données | P3-01, P3-04 |
| MU-19 | Reprise d’écriture depuis le premier octet | P3-02 |
| MU-20 | Will déclenché effacé par reconnexion tardive | P3-10 |
| MU-21 | Quota mémoire contourné par continuations ou métriques | P3-11 |
| MU-22 | Échec de synchronisation masqué | P1-01 et V3-016 |
| MU-23 | Garde de politique d’autorisation ignorée | P3-14 |
| MU-24 | Fanout accepté partiellement avant refus | P2-23 |
| MU-25 | Mutation d’état malgré capacité d’effets insuffisante | P1-19 |

Toutes les mutations critiques non équivalentes et applicables doivent être détectées par la propriété attendue. Une erreur de compilation n’est pas une détection du défaut ; une injection jamais atteinte révèle un trou. Un timeout n’est une détection de blocage que si la trace établit les conditions de progression.

Les mutations équivalentes sont justifiées individuellement. Les exclusions, les attentes modifiées et les propriétés affaiblies nécessitent une revue séparée. Le score global ne permet pas de compenser une mutation critique survivante.

# 22 Qualification des ressources

**V3-022 — Comptabilité logique et enveloppe réelle.** Les comptes exacts du cœur sont confrontés à la mémoire et au disque effectivement utilisés.

Le budget RAM comprend payloads et références, tâches, piles, trames, TLS, authentifications, timers, continuations, caches, reçus en mémoire, logs et cardinalité des métriques. Les payloads partagés restent comptés jusqu’à la dernière référence.

Sous Linux, conserver RSS, mémoire du cgroup, ventilation et événements de pression ou OOM. Le cgroup couvre notamment cache de pages et buffers de sockets selon la [documentation du noyau](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html). Les générateurs et témoins sont isolés de la comptabilité du broker.

Le budget disque distingue données logiques, espace réutilisable et blocs physiquement occupés. Mesurer base, WAL, pages libres, historique retained, pins, reçus, réserves de terminaison et retard de maintenance. Une suppression logique ne garantit pas une diminution immédiate du fichier.

À état logique identique après drainage, les comptes doivent revenir aux valeurs attendues. Les caches et l’allocateur peuvent conserver un plateau borné. Les campagnes de churn vérifient que ce plateau reste dans l’enveloppe annoncée ; elles ne réclament pas un retour arbitraire au RSS initial.

Les critères sont zéro dépassement logique, zéro OOM dans le profil accepté, aucune croissance non bornée, contrôle disponible et aucun engagement sacrifié pour libérer de la place. L’ENOSPC physique demeure une panne distincte malgré les quotas et réserves.

# 23 Matériel et workloads de référence

**V3-023 — Comparer des profils reproductibles.** Deux classes de matériel sont obligatoires : un Raspberry Pi ARM64 identifié précisément, puis un serveur x86-64 multicœur. La qualification électrique reste attachée au support réellement essayé.

| Profil de campagne proposé | Paramètres explorés |
| - | - |
| Pi | 1 puis plusieurs cœurs disponibles ; enveloppes cgroup initiales de 256 MiB et 1 GiB |
| Serveur | 1, 2, 4 puis davantage de cœurs physiques ; première enveloppe de 4 GiB, puis profil capacitaire |
| Stockage | Durable sur support identifié ; Volatile séparé ; mêmes responsabilités hors panne |
| Réseau | TCP puis TLS ; connexions établies séparées du coût des handshakes |

Ces enveloppes sont des points d’expérience proposés, pas des consommations obtenues ni des configurations promises. Le nombre de connexions compatible avec chacune est mesuré. Modèle de Pi, support, CPU serveur, mémoire, refroidissement, filesystem et limites sont figés dans chaque manifeste.

Les workloads couvrent topics répartis et topic dominant ; filtres exacts et wildcards ; fanout 1, 10, 100 puis 1 000 si le profil l’admet ; payloads 32 B, 256 B, 4 KiB et 64 KiB ; QoS 0, 1 et 2 ; retained et replay lent ; churn ; clients hors ligne ; checkpoints, GC et reprise.

Le chemin QoS 0 sans responsabilité durable est mesuré séparément du retained et du QoS durable. La charge qui force un commit ne doit pas être comparée à un chemin qui n’en promet pas.

Le plan ne croise pas aveuglément tous les facteurs. Il impose les combinaisons à risque, puis explore les facteurs indépendants. SMT, NUMA, fréquence, température, throttling, caches et saturation du générateur sont enregistrés.

# 24 Méthode de mesure

**V3-024 — Mesurer la charge réellement servie.** Le générateur utilise un calendrier d’émission indépendant des réponses, tout en respectant le contrôle de flux du protocole. Quand il ne peut émettre, il enregistre le retard ou le rendez-vous manqué dans une file elle-même bornée.

Les unités publiées sont publications prévues, effectivement émises, reçues, refusées, engagées ; obligations créées et terminées par motif ; paquets, retransmissions et octets. PUBREC et PUBCOMP ont des latences distinctes. Un ACK producteur n’est pas une fin de fanout.

Mesurer latence depuis l’émission prévue, retard du générateur, latence depuis l’émission effective, attente et service de chaque étape, commit, réception destinataire et terminaison. Ne pas soustraire des horloges intermachines sans borne d’erreur ; utiliser une horloge commune du banc lorsque possible.

Les quantiles des seuls succès sont accompagnés des absents, refusés, encore en attente et de leur âge. Si la cohorte n’a pas assez terminé pour observer son p99, le résultat le dit ; elle n’est pas amputée jusqu’à devenir rapide.

La capacité soutenable d’un workload est la plus grande charge testée qui respecte ses critères pendant la fenêtre, sans croissance des files incompatible avec leurs bornes et avec drainage correct du lot final. Les opérations non terminées ne disparaissent pas à l’arrêt du chronomètre.

Les points de décision utilisent cinq répétitions indépendantes, ordre des candidats alterné, échauffement jusqu’à stabilité et au moins trente minutes mesurées incluant maintenance. Présenter dispersion et pire résultat observé ; cinq répétitions ne constituent pas une preuve d’événement rare.

Pour identifier le plafond, mesurer séparément matching, préparation, writer, synchronisation, réseau, encodeur et consommation. Un générateur ou une carte réseau saturés ne démontrent pas la limite du broker.

# 25 Seuils et comparaison des brokers

**V3-025 — Critères fixés avant les résultats.** Chaque profil de production exige une fiche complète : charge de pointe Q, connexions, abonnements, distributions de payload et fanout, p99 d’ACK et de livraison, délai de contrôle, délai de reprise, budgets mémoire et disque, et refus permis.

La règle initiale proposée accepte une capacité soutenable d’au moins **1,30 fois Q**, avec les autres critères toujours satisfaits. Cette marge de 30 % est une décision d’ingénierie à valider pour le profil, pas une règle MQTT. Elle est fixée avant la campagne de décision.

Sans valeur renseignée pour une exigence de service, l’exploration peut publier une enveloppe de capacité ; elle ne prononce pas la qualification de production. Les seuils ne sont pas choisis après coup à partir du résultat obtenu.

Mosquitto, FlashMQ et RMQTT sont mesurés avec version ou SHA exact, configuration et garanties effectives. Les groupes de comparaison fixent QoS, TLS, durabilité, limites, saturation, fanout et mode d’acceptation. Une option appelée persistence ne démontre pas la conservation avant ACK.

Un candidat dépourvu de mode comparable reste présenté dans un groupe de garanties distinct. Aucun ratio de débit ne sera présenté comme une comparaison équivalente si le point de durabilité diffère. La distinction entre WAL NORMAL et FULL illustre ce risque dans la [documentation SQLite](https://www.sqlite.org/pragma.html#pragma_synchronous).

R1 est rejetée pour un profil si son writer, son topic chaud ou son coût de fanout ne satisfait pas les exigences avec la marge convenue. Avant de répartir le stockage, isoler le coût dominant et conserver les mêmes traces de correction. Une optimisation ne gagne pas le droit de modifier silencieusement le contrat.

# 26 Rythme des campagnes

**V3-026 — Des campagnes proportionnées aux changements.**

| Moment | Contenu obligatoire |
| - | - |
| Chaque modification | Registre complet du sous-ensemble ; régressions ; traces et mutations critiques touchées ; simulation déterministe |
| Campagne périodique | Graines nouvelles, fuzzing structuré, IO injectées, churn et analyse des budgets |
| Candidat d’architecture | Modèles obligatoires terminés, corpus P1 à P3 applicable, mutations critiques, mesures Pi et serveur |
| Candidat de production | Profil fonctionnel complet, supports qualifiés, endurance, reprise, migration et dossier de construction |

Budget initial proposé : 256 traces de 512 événements par campagne courte ; 4 096 traces de 4 096 événements par campagne périodique, en plus des traces imposées. Le fuzzing périodique reçoit au moins une heure CPU par cible active ; une campagne candidate reçoit vingt-quatre heures CPU par cible critique. Les campagnes déclarent les budgets réellement consommés.

Ces nombres permettent de planifier le calcul. Ils ne remplacent ni les témoins de transition ni les mutations et ne donnent pas une probabilité de défaut résiduel. Si la suite dépasse son budget, le résultat reste incomplet ; réduire son domaine demande une justification visible.

Une endurance de vingt-quatre heures par profil candidat alterne remplissage, drainage, replays, churn, maintenance et reprises. Les erreurs sont classées et minimisées ; une répétition sans diagnostic n’efface pas un échec.

Les nouvelles vérifications sont élargies lorsqu’un changement touche une hypothèse, une frontière durable, une identité ou une règle d’ordre. Les modifications locales sans impact démontré ne déclenchent pas automatiquement toute la matrice matérielle.

# 27 Conformité et remplacement Mosquitto

**V3-027 — Les prototypes ne constituent pas toute la qualification produit.** Un registre normatif distinct couvre les clauses serveur de [MQTT 3.1.1 avec Errata 01](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html) et de [MQTT 5](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html), avec préconditions, test, oracle et portée.

Les fonctionnalités facultatives annoncées et les options refusées sont testées dans les deux sens. Les conflits entre texte principal, exemples et annexes sont consignés avec une décision normative ; une extraction automatique de numéros d’exigences n’est pas seule un oracle.

La campagne d’interopérabilité utilise MQTTium, des clients Mosquitto inchangés et au moins une implémentation indépendante. Elle conserve des pairs adversariaux capables de comportements que les bibliothèques normales empêchent. Un consensus de plusieurs brokers n’est pas la preuve d’une règle normative.

La qualification de remplacement ajoute configuration, fichiers de mots de passe et ACL, certificats, listeners, reload, limites, codes de sortie et outils d’exploitation du profil. Chaque directive est classée. Bridges, plugins, Dynamic Security et continuité d’état restent des périmètres explicites.

Une migration doit tester source préservée, import ou drainage annoncé, comportements des clients, interruption, données acceptées après bascule et retour arrière. Un test de reconnexion ne suffit pas à valider le transfert des sessions et échanges.

Les chapitres suivants préciseront le périmètre opérationnel ; aucun passage de P1 à P3 ne rend implicitement ces exigences satisfaites.

# 28 Critères de passage

**V3-028 — Décision fondée sur des livrables et des verdicts.**

| Porte | Conditions cumulatives |
| - | - |
| G0 Banc crédible | Registre sans trous ; oracles distincts ; traces rejouables ; injections effectivement atteintes |
| G1 P1 | Admission et reprise correctes ; issues classées ; identités et reçus sûrs ; mutations détectées ; preuve pilote |
| G2 P2 | Cuts, pins, replay et collecte corrects ; causes explicites ; budgets d’historique respectés |
| G3 P3 | Progression dans le profil ; refus seulement prévus ; transport, annulation et ressources maîtrisés |
| G4 Architecture adaptée | Pi et serveur mesurés ; plafonds identifiés ; exigences renseignées et satisfaites |
| G5 Production | Conformité et remplacement qualifiés ; D1 et T1 fermés pour le périmètre annoncé ; panne matérielle et exploitation couvertes |

Une porte peut indiquer une réussite partielle de propriétés précisément nommées ; elle ne devient pas globalement réussie si une condition obligatoire manque. Les variantes overlap ou shared non fermées restent hors qualification, sans pouvoir disparaître d’un profil qui les exige.

Les critères communs sont zéro contre-exemple non résolu, zéro perte inexpliquée, zéro mutation critique survivante applicable, zéro cellule obligatoire non classée et dossier lié au build exact. « Zéro observé » décrit le résultat de la campagne et sa portée ; ce n’est pas une preuve d’absence universelle de bugs.

La reprise après panne ne doit pas attendre que tous les clients lents vident leurs files pour déclarer les services indépendants disponibles. Le délai de reconstruction et celui de drainage ont des mesures distinctes.

# 29 Organisation du travail des agents

**V3-029 — Implémentation et validation avec revues croisées.** L’agent oracle écrit attentes et contre-exemples ; l’agent cœur réalise les transitions ; l’agent stockage réalise transactions et récupération ; l’agent adversarial construit injecteurs, mutations et pairs réseau. Un responsable d’intégration arbitre les interfaces et les changements de contrat.

Les agents peuvent proposer une correction de spécification, mais ne font pas passer simultanément une modification de comportement et l’affaiblissement de son test sans revue indépendante. L’indépendance des rôles réduit la corrélation des erreurs ; elle ne rend pas un agent infaillible.

L’ordre de travail est : G0 et preuve pilote ; P1 ; modèle retained et P2 ; runtime et P3 ; mesures comparatives ; décision sur R1 ; extension au profil complet. Les tests système ciblés commencent dès que leur frontière existe.

Chaque lot livre contenu exact, changements de contrat éventuels, tests réellement exécutés, résultats négatifs, traces et exclusions. Un rapport d’agent est un guide de revue ; le code, les sorties et les traces restent les preuves.

Les points de commit, formats persistants, clôtures d’identités et changements d’ordonnancement ont un responsable identifié. Après intégration, les propriétés affectées sont revalidées sur le contenu intégré.

# 30 Raccordement et prochaine étape

**V3-030 — Compléments à reporter explicitement dans le cahier des charges.**

| Sujet | Précision de ce chapitre | Référence antérieure |
| - | - | - |
| Refus certain | Identité définitivement non réexécutable, sans faux rejet des issues inconnues | ARC-005 et ARC-019 |
| Recouvrement | Causes avant agrégation ; qualification des combinaisons d’options | CONC-007 et ARC-017 |
| Groupe saturé | Capacité indisponible distincte de l’absence de membres | ARC-017 |
| Expiration incertaine | Test à deux histoires et condition de fermeture de D1 | ARC-013 |
| Temps du Will | T1 distingue conservation, admission différée et conformité temporelle | WILL-004 et ARC-024 |
| Progression | Charge admissible séparée du dépassement autorisant une fermeture | RES-005 et ARC-022 |
| Performance | Critères antérieurs aux résultats et publications séparées des obligations | VAL-006 et ARC-025 |

Ces précisions sont proposées avec ce chapitre ; les deux chapitres précédents conservent leur version actuelle. Une consolidation ultérieure doit répercuter les décisions retenues pour ne laisser aucun contrat divergent.

La prochaine étape recommandée est un chapitre 4 consacré au **lot de démarrage exécutable** : structure du dépôt, schémas versionnés du registre de transitions et des traces, interfaces des adaptateurs, premier modèle, première preuve pilote et tâches P1 découpées avec leurs critères de réception. Les valeurs de service du profil réel y seront renseignées avant toute décision de production.

La décision immédiate reste de construire une démonstration falsifiable des frontières critiques de R1 avant d’étendre son périmètre. L’ensemble du dossier permettra ensuite de conserver cette architecture, d’en remplacer une partie ou de revenir à l’adoption d’un broker existant selon les garanties et capacités effectivement établies.
