**Version proposée 0.1 — 14 novembre 2026**

Ce chapitre définit les comportements observables attendus de `mqttiumd`, nom provisoire d’un broker Rust autonome associé à MQTTium. Il transforme les objectifs de compatibilité, de concurrence et de fiabilité en exigences et en scénarios de réception. Il prépare le choix de l’architecture et la qualification des alternatives existantes.

La décision centrale est la suivante : **en mode Durable, une réponse positive d’acceptation ne peut devenir éligible à l’envoi qu’après la conservation durable de toutes les responsabilités correspondantes**. Pour QoS 2, cette frontière précède PUBREC. Les obligations de distribution deviennent exécutables sans attendre PUBREL.

Les mots **DOIT** et **NE DOIT PAS** expriment des exigences proposées pour le produit. Ils ne signifient pas que l’implémentation existe, que ces exigences sont déjà satisfaites, ou qu’une certification est acquise.

# 1 Périmètre du produit

**PRD-001 — Produit autonome.** Le broker DOIT être exploitable comme un service autonome, avec un binaire principal, une configuration locale, des diagnostics et des outils de validation. Une dépendance externe obligatoire à une base de données, à un service d’authentification ou à un cluster n’est pas prévue pour le profil initial.

**PRD-002 — Plateformes.** La première cible de qualification DOIT couvrir Linux x86-64 et Linux ARM64. Le nombre de workers, les budgets et les tailles de lots pourront différer entre un Raspberry Pi et un serveur. Les règles de protocole et les garanties du mode de durabilité choisi DOIVENT rester identiques.

**PRD-003 — Périmètre de disponibilité.** La v1 vise une instance capable de reprendre son état local après les pannes couvertes. La haute disponibilité par réplication, la tolérance à la perte complète d’une machine et le clustering nécessiteront un contrat séparé.

**PRD-004 — Mise en œuvre.** Rust est le choix recommandé. Le cœur de transitions doit rester déterministe, sans entrées et sorties, avec un propriétaire unique de chaque état mutable. Le découpage des partitions et le moteur de stockage seront décidés après les prototypes de faisabilité du chapitre suivant.

# 2 Sources et nature des garanties

Le socle protocolaire est [MQTT 3.1.1 avec Errata 01](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html) et [MQTT 5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).

Trois niveaux devront être distingués dans la matrice de traçabilité :

| Niveau | Objet | Oracle |
| - | - | - |
| Norme MQTT | Comportements obligatoires et capacités optionnelles | Texte applicable et interprétation explicitée |
| Contrat mqttiumd | Durabilité, limites, reprise et isolation promises par le produit | Exigence numérotée et scénario de réception |
| Profil Mosquitto | Syntaxes, fichiers et pratiques d’exploitation compatibles | Documentation archivée, version exacte et essais différentiels |

**SPEC-001 — Traçabilité.** Toute exigence de livraison DOIT identifier son origine, son périmètre, ses préconditions, le comportement interdit, son test et ses limites de preuve. Les divergences entre le corps des normes, les annexes et les commentaires DOIVENT être conservées dans les décisions d’interprétation.

**SPEC-002 — Référence indépendante.** Le comportement de Mosquitto NE DOIT PAS remplacer l’oracle normatif. Une différence entre deux brokers peut être licite. Inversement, reproduire un comportement existant ne prouve pas sa conformité.

**SPEC-003 — Versions.** La qualification DOIT porter sur un commit, un binaire, ses options de compilation et un profil de configuration précis. Un nom de branche ou de famille de versions ne constitue pas une preuve.

# 3 Profil initial de remplacement Mosquitto

Le terme retenu est **remplacement compatible avec le profil standard Mosquitto**, après une bascule préparée. Il ne promet pas une reprise transparente de tous les états d’une autre implémentation.

| Domaine | Exigence pour le profil initial | Extension distincte |
| - | - | - |
| MQTT | 3.1.1 avec Errata 01 et 5.0 ; QoS 0, 1 et 2 | MQTT 3.1 ancien |
| Fonctions usuelles | Sessions, retained, Will, wildcards, mécanismes MQTT 5 du profil annoncé | Méthodes d’authentification étendue particulières |
| Clients | Clients conformes inchangés, notamment MQTTium et les outils Mosquitto qualifiés | Compatibilité avec des violations connues du protocole |
| Réseau | TCP IPv4 et IPv6, TLS 1.2 et 1.3, mTLS, WebSocket et WSS | Options de proxy ou de TLS spécifiques non qualifiées |
| Linux local | Socket Unix prévu dans le profil Linux | Autres OS |
| Configuration | Sous-ensemble explicite de mosquitto.conf, inclusions et portées des listeners | Directives spécialisées hors matrice |
| Authentification | Fichiers de mots de passe et ACL des versions qualifiées | Backends externes et ABI C arbitraire |
| Exploitation | Foreground, service systemd, arrêt contrôlé, SIGHUP, journaux et métriques | Identité exacte de tous les logs et compteurs |
| Persistance | Format propre et contrat Durable mqttiumd | Import des bases Mosquitto |
| Bridges | Détection bloquante si utilisés hors profil | Profil bridge qualifié |
| Dynamic Security | Détection bloquante si utilisé hors profil | Modèle de droits, import et API compatibles |

**CMP-001 — Clients.** Une application conforme appartenant au profil DOIT conserver son code et sa bibliothèque cliente. Les ajustements d’adresse, de certificats ou de configuration liés au déploiement restent possibles.

**CMP-002 — Capacités.** Chaque capacité MQTT 5 annoncée DOIT être implémentée et testée. Le profil cible inclut les abonnements partagés, mais ils disposent de tests et de garanties distincts. Une version intermédiaire ne les implémentant pas devra annoncer leur indisponibilité et ne pourra être présentée comme ayant terminé ce profil.

**CMP-003 — Absence d’émulation implicite.** Une ancienne tolérance à un paquet invalide ne devient pas automatiquement une exigence de compatibilité. Toute exception envisagée devra être isolée, documentée et confrontée au contrat de conformité.

# 4 Configuration et fichiers compatibles

La compatibilité doit être déterminée sur la configuration effective, après résolution des valeurs par défaut, des inclusions et des portées. La [documentation de mosquitto.conf](https://mosquitto.org/man/mosquitto-conf-5.html) distingue notamment les listeners, les ACL, la persistance et les options rechargeables.

**CFG-001 — Analyse complète.** Toute directive rencontrée DOIT recevoir exactement un statut : équivalente, transformée explicitement, nécessitant un redémarrage, ou bloquante. Une directive inconnue NE DOIT PAS être ignorée.

**CFG-002 — Options sensibles.** Une transformation non résolue portant sur l’exposition réseau, les identités, les droits, le routage ou la durabilité DOIT empêcher la déclaration de compatibilité.

**CFG-003 — Version source.** L’import DOIT connaître le profil Mosquitto source et produire une configuration effective explicite. Il NE DOIT PAS appliquer les valeurs par défaut d’une version supposée à une configuration provenant d’une autre version.

**CFG-004 — Syntaxes d’authentification.** Le profil DOIT couvrir les directives historiques retenues ainsi que les déclarations reconnues des modules officiels de fichiers. Mosquitto 2.1 a déplacé ces fonctions vers les plugins [password-file](https://mosquitto.org/documentation/plugins/password-file/) et [acl-file](https://mosquitto.org/documentation/plugins/acl-file/). La conversion vers des fonctions natives mqttiumd sera explicite ; un basename de bibliothèque inconnu ne suffira pas à autoriser une substitution.

**CFG-005 — Formats de mots de passe.** La qualification DOIT utiliser des fichiers réellement produits par les builds de mosquitto_passwd retenus. La [manpage actuelle](https://mosquitto.org/man/mosquitto_passwd-1.html) décrit Argon2id, SHA512-PBKDF2 et SHA512. Leur reconnaissance, leurs paramètres et leurs coûts de vérification doivent être testés. Le calcul des hashes NE DOIT PAS bloquer les workers de routage.

**CFG-006 — Persistance importée.** La transformation de persistence=true vers le contrat Durable mqttiumd DOIT signaler les différences de coût et de garanties. Un intervalle d’autosave ne sera pas interprété comme une borne de perte prouvée. Le [plugin SQLite Mosquitto](https://mosquitto.org/documentation/persistence/sqlite/) possède ses propres paramètres de synchronisation et de regroupement.

# 5 Rechargement et autorisation

**SEC-001 — Rechargement atomique.** Un reload DOIT préparer une génération complète de configuration, valider ses fichiers et ne l’activer qu’après succès. En cas d’erreur, la génération précédente reste active. Chaque option précise si elle est rechargeable ou impose un redémarrage. Cette politique est un choix produit délibéré ; la [release Mosquitto 2.1](https://mosquitto.org/blog/2026/01/version-2-1-0-released/) décrit un comportement différent pour certains fichiers invalides au reload.

**SEC-002 — Frontière de révocation.** L’activation d’une nouvelle politique DOIT définir le moment à partir duquel les nouvelles décisions d’accès l’utilisent. Les données déjà remises au transport peuvent être irrévocables. Aucune nouvelle décision de distribution ne doit autoriser un destinataire désormais interdit ; la fermeture éventuelle d’une connexion et l’achèvement des seuls contrôles protocolaires nécessaires seront spécifiés.

**SEC-003 — État existant.** Les retained et files hors ligne NE DOIVENT PAS servir à contourner les droits courants. Une obligation arrêtée pour révocation doit posséder une cause terminale identifiable. Une publication déjà envoyée ne peut pas être effacée chez son destinataire.

**SEC-004 — Identités.** Le droit de reprendre une session DOIT être vérifié selon la politique d’identité du profil. Le ClientID seul ne constitue pas une preuve d’identité. Les substitutions ACL, les caractères spéciaux et les portées de listener feront partie du corpus de tests.

**SEC-005 — TLS.** Une option de sécurité non traduisible DOIT être bloquante. Le système NE DOIT PAS désactiver une validation ou accepter des paramètres plus permissifs pour rendre une migration silencieusement compatible.

Dynamic Security dispose d’un modèle de clients, groupes et rôles et d’une API MQTT ([documentation officielle](https://mosquitto.org/documentation/dynamic-security/)). Le réduire automatiquement à une ACL plate ne constitue pas une migration admissible.

# 6 Vocabulaire des responsabilités

| Terme | Définition pour mqttiumd |
| - | - |
| Publication reçue | PUBLISH complet décodé ; son admission n’est pas encore assurée |
| Publication admise | Décision logique de prise en charge avec ressources et effets définis |
| Publication engagée | Décision et responsabilités conservées selon le mode choisi |
| Réponse observée | Accusé effectivement reçu par le client ; sa perte ne prouve pas l’absence de commit |
| Obligation de distribution | Responsabilité identifiée de traiter une publication pour une destination |
| Livraison protocolaire terminée | Échange MQTT sortant terminé selon son QoS |
| Traitement applicatif | Effet métier chez le consommateur, extérieur à la transaction du broker |

**ID-001 — Identité de session.** Un SessionId interne DOIT rester stable lors d’une reprise effective. Une session recréée reçoit une autre identité, même si son ClientID est identique.

**ID-002 — Identité de connexion.** Une ConnectionGeneration DOIT changer à chaque connexion. Un résultat réseau, d’authentification ou de timer ancien NE DOIT PAS agir sur la nouvelle connexion.

**ID-003 — Identité de publication.** Les obligations DOIVENT être identifiables indépendamment du Packet Identifier MQTT, qui peut être réutilisé. L’identité d’une tentative interne doit permettre de réconcilier un commit incertain.

**ID-004 — Effets tardifs.** Un résultat tardif de stockage peut confirmer une responsabilité toujours valide tout en étant interdit d’écriture sur une socket remplacée. Le rejet des événements obsolètes NE DOIT PAS supprimer aveuglément un engagement durable.

# 7 Deux modes de durabilité

| Propriété | Volatile | Durable |
| - | - | - |
| Transitions MQTT et ordre | Même contrat | Même contrat |
| Bornes de ressources et isolation | Obligatoires | Obligatoires |
| Sessions après panne du processus | Perte possible | Reprise des sessions persistantes engagées |
| Obligations après panne couverte | Perte possible | Conservation ou terminaison autorisée |
| Émission des ACK positifs | Après prise en charge en mémoire | Après le commit durable nécessaire |
| Usage conseillé | Développement ou flux tolérant la perte | Exploitation visant la conservation après panne |

**DUR-001 — Choix visible.** Le mode DOIT être explicite dans la configuration effective et les diagnostics. Les profils de production proposés pour Pi et serveur utilisent Durable ; Volatile est un choix explicite.

**DUR-002 — Absence de dégradation implicite.** Une erreur de stockage NE DOIT PAS provoquer un passage automatique de Durable à Volatile.

**DUR-003 — Garantie centrale.** Une obligation durable acceptée NE DOIT PAS disparaître à cause d’une panne couverte. Après reprise, elle reste exécutable, est déjà achevée, ou possède une terminaison autorisée par ce contrat.

Cette garantie ne signifie pas que toute publication acquittée sera traitée par une application. Le QoS effectif du destinataire, les expirations, la durée de vie de sa session et les terminaisons autorisées restent applicables.

Un mode Buffered pourra être étudié ultérieurement. Une simple période de flush ne suffit pas à garantir une fenêtre maximale de perte si le stockage peut prendre du retard.

# 8 Pannes couvertes et hypothèses

| Événement | Résultat exigé en Durable |
| - | - |
| Arrêt brutal du processus | Reprise des engagements durables |
| Crash du système | Même garantie sous les hypothèses de stockage qualifiées |
| Coupure électrique | Même garantie sur le couple matériel et stockage qualifié |
| Espace épuisé ou erreur d’écriture | Arrêt des admissions concernées ; conservation des engagements connus |
| Commit au résultat inconnu | Réconciliation avant nouvelle admission ambiguë |
| Corruption détectée | Pas de démarrage silencieux avec état vide |
| Disque détruit ou machine perdue | Restauration ou réplication selon un contrat distinct |
| Synchronisation annoncée mais non honorée par le stockage | Hypothèse de durabilité violée, hors promesse logicielle locale |

**FAIL-001 — Hypothèses publiées.** Le profil qualifié DOIT identifier le système, le système de fichiers, le support, les paramètres de synchronisation et les défaillances effectivement injectées. Un test SIGKILL ne qualifie pas à lui seul les coupures électriques.

**FAIL-002 — Protection de l’état.** Le répertoire de données DOIT avoir un seul écrivain. Une version de stockage inconnue, une migration incomplète ou une corruption détectée NE DOIT PAS être traitée comme une base vide.

**FAIL-003 — Arrêt sûr.** La v1 peut arrêter l’instance après une erreur de stockage empêchant de garantir la cohérence. Continuer partiellement ne sera permis que si ce mode dégradé possède un périmètre et des invariants prouvés.

La [documentation SQLite sur synchronous](https://www.sqlite.org/pragma.html#pragma_synchronous) distingue WAL/FULL de WAL/NORMAL pour la résistance aux pannes système et électriques. Les [hypothèses de synchronisation](https://www.sqlite.org/atomiccommit.html) restent déterminantes. Le choix d’un moteur transactionnel ne prouve pas la correction des responsabilités que l’application lui confie.

# 9 Frontières des accusés de réception

Le socle MQTT place la prise de responsabilité à PUBACK pour QoS 1 et à PUBREC pour QoS 2. MQTT 3.1.1 présente plusieurs moments possibles pour la distribution ; le contrat produit retient une distribution disponible dès l’engagement initial ([MQTT 3.1.1, sections 4.3 et 4.5](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html)).

| Réponse émise | État requis en Durable |
| - | - |
| PUBACK positif | Publication, décision de routage, obligations protégées et retained éventuel engagés |
| PUBREC positif | Éléments précédents et état entrant QoS 2 requis pour reconnaître le rejeu |
| PUBCOMP | Transition de fin QoS 2 engagée, sans possibilité de recréer la distribution initiale |
| CONNACK réussi | Création, reprise ou reset de session cohérent ; intention de Will conservée si applicable |
| SUBACK réussi | Modification activée et, pour une session persistante, conservée durablement |
| UNSUBACK réussi | Suppression effective et, pour une session persistante, conservée durablement |

**ACK-001 — Barrière d’émission.** Une réponse positive NE DOIT PAS entrer dans le transport avant l’achèvement du commit qui la justifie. Remettre un travail au worker de stockage n’est pas un commit.

**ACK-002 — Commit groupé.** Plusieurs réponses peuvent partager un commit. Chacune attend le succès du groupe qui porte ses responsabilités.

**ACK-003 — Absence d’attente des consommateurs.** L’ACK producteur n’attend pas que tous les abonnés aient reçu ou traité le message. Les obligations engagées permettent la poursuite autonome.

**ACK-004 — Sens sortant.** Le même soin s’applique au broker lorsqu’il est émetteur vers un abonné : l’état permettant la reprise d’un PUBLISH ou d’un PUBREL DOIT être conservé avant l’envoi correspondant.

**ACK-005 — QoS 0.** Aucun accusé MQTT ne permet au producteur QoS 0 de constater un commit. Les retained reçus en QoS 0 peuvent être persistés, mais leur réception réseau ne constitue pas une confirmation durable pour ce producteur.

# 10 Admission et comportement QoS 2

**ADM-001 — Décision cohérente.** Chaque publication possède un point logique unique de prise en charge par rapport aux changements d’abonnements. Les destinataires et options sont déterminés à ce point. Un commit rend cette décision récupérable avant l’ACK positif. Le point logique et l’instant physique du flush ne doivent pas être confondus.

**ADM-002 — Admission sans effet partiel.** Si l’admission est refusée avec certitude, aucune distribution ni modification retained ne doit avoir été engagée pour cette tentative. Préparer puis abandonner des réservations internes reste permis.

**ADM-003 — Réservation.** Avant d’accepter une responsabilité protégée, le broker DOIT réserver son coût borné : données, références de distribution, état QoS, métadonnées de reprise et coût minimal de leur libération.

**ADM-004 — QoS 2 autonome.** La décision de routage et les obligations DOIVENT être engagées avant PUBREC positif. Elles deviennent exécutables dès ce commit, indépendamment d’un futur PUBREL et de la survie de la session productrice. La suppression de cette dernière ne supprime pas les obligations déjà admises vers d’autres sessions.

**ADM-005 — Phase de terminaison.** PUBREL et PUBCOMP terminent l’échange entrant et la déduplication. Ils NE DOIVENT PAS créer une nouvelle décision de distribution ni servir de seconde admission avec des quotas calculés tardivement.

**ADM-006 — Rejeu.** Pendant l’échange QoS 2, un PUBLISH rejoué NE DOIT PAS créer une deuxième publication interne. En QoS 1, le produit NE DOIT PAS promettre une déduplication applicative générale fondée sur le Packet Identifier ou le payload.

MQTT 5 permet un refus d’admission dans PUBREC ; PUBCOMP n’offre pas un refus tardif pour quota. Le contrôle de flux QoS 2 reste engagé jusqu’à sa terminaison, et le rejeu sur une connexion vivante n’est pas un mécanisme de retry libre ([MQTT 5, sections 3.7, 4.4 et 4.9](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)).

Pour MQTT 3.1.1, un rejet d’admission faute de capacité ne doit pas être maquillé en succès : le profil utilise une fermeture avant ACK lorsqu’aucune réponse d’échec appropriée n’existe.

# 11 Distribution entre propriétaires

**ROUTE-001 — Reprise fidèle.** La récupération DOIT utiliser les obligations ou la décision historique conservée. Elle NE DOIT PAS simplement recalculer les destinataires depuis les abonnements actuels.

**ROUTE-002 — Transfert de responsabilité.** Une source conserve sa preuve durable tant que la destination n’a pas durablement pris en charge l’obligation. La seule insertion dans une file en mémoire ne suffit pas.

**ROUTE-003 — Idempotence interne.** Le transfert d’une même obligation peut être répété sans répéter son effet sur l’état de la session. Cette idempotence interne est distincte des retransmissions MQTT visibles et des doublons autorisés en QoS 1.

**ROUTE-004 — Collecte.** Les preuves de déduplication et les métadonnées de transfert DOIVENT être collectables sans réintroduire une ancienne obligation. Les séquences, acquittements et bornes sur les transferts retardés font partie du protocole à vérifier.

Exemple de réception : A produit pour B et C ; B prend durablement l’obligation, C ne l’a pas encore reçue, puis le processus tombe. Après reprise, B ne doit pas recevoir un second effet interne et C doit retrouver son obligation. Un abonné D créé plus tard ne doit pas être ajouté rétroactivement à cette publication.

Le moteur peut utiliser une transaction commune ou un protocole durable entre partitions. Le présent contrat fixe le résultat observable, pas encore la technique.

# 12 Sessions et changements concurrents

**CONC-001 — Un propriétaire.** Chaque état mutable de session DOIT être modifié par un seul propriétaire à un instant donné. Tout transfert de propriété éventuel devra posséder un protocole explicite.

**CONC-002 — Reprise et reset.** Reconnexion, reprise de session et création d’une nouvelle session DOIVENT être trois événements distincts dans les modèles et les tests. Une obligation de l’ancienne SessionId NE DOIT PAS être livrée à une session recréée.

**CONC-003 — Activation d’abonnement.** Une activation confirmée DOIT être visible aux admissions ultérieures qui ne lui sont pas concurrentes. Les opérations concurrentes doivent être explicables par l’ordre logique documenté.

**CONC-004 — Remplacement.** Remplacer un abonnement NE DOIT PAS être implémenté comme une suppression suivie d’une recréation laissant un trou observable.

**CONC-005 — Retained et flux live.** L’installation, le point de lecture retained et les publications concurrentes DOIVENT posséder une règle cohérente. Le replay doit être paginé et borné ; un SUBACK ne constitue pas une barrière universelle interdisant les publications avant lui.

**CONC-006 — Ordre.** Les workers DOIVENT préserver les contraintes d’ordre du profil MQTT. Le contrat ne promet pas un ordre total entre tous les producteurs et tous les topics.

**CONC-007 — Chevauchement.** La politique de copie pour des abonnements qui se recouvrent DOIT être explicite, avec QoS effectif et identifiants d’abonnement cohérents. L’oracle NE DOIT PAS considérer tout doublon apparent comme une faute.

**CONC-008 — Partagés.** Les abonnements partagés disposent d’une machine de distribution distincte. Une réaffectation NE DOIT PAS casser un échange QoS 2 déjà commencé. Aucun engagement de livraison exactement une fois à un traitement métier réparti entre membres n’est ajouté implicitement.

# 13 Ressources et progression

**RES-001 — Comptabilité complète.** Les budgets DOIVENT compter les octets et le nombre d’objets : buffers, TLS, publications, files hors ligne, retained, abonnements, obligations de fanout, timers, authentifications en cours, journal et métadonnées de reprise. Les copies et références partagées doivent être comptabilisées selon une règle explicite.

**RES-002 — Charge excessive.** Lorsque les ressources nécessaires manquent, les nouvelles admissions sont ralenties ou refusées avant l’engagement. Un message accepté ne peut pas être supprimé pour rendre un benchmark plus rapide.

**RES-003 — Contrôle réservé.** Les opérations qui libèrent des ressources DOIVENT pouvoir progresser lorsque les files de données sont pleines. Cela concerne notamment les ACK, les commits de terminaison, les retours de crédits et les déconnexions.

**RES-004 — Travail borné.** Un événement du cœur NE DOIT PAS construire une liste non bornée de destinataires ou monopoliser un worker sur un fanout massif. Les continuations et lots doivent avoir des limites testées.

**RES-005 — Isolation.** Un abonné lent peut imposer une contre-pression aux publications qui doivent lui être conservées. Il NE DOIT PAS bloquer les flux indépendants ni le traitement des contrôles permettant la progression.

**RES-006 — Garanties de progression.** Les propriétés de vivacité DOIVENT expliciter leurs hypothèses : stockage de nouveau disponible, ordonnanceur équitable, client qui lit ou reconnexion utile. Aucun délai absolu de livraison n’est promis à un client qui ne consomme jamais.

**RES-007 — Profils.** Les profils Pi et serveur modifient les budgets et le parallélisme, sans désactiver silencieusement les contrôles de sûreté. Le budget applicatif, la marge de l’allocateur et la mémoire des sockets gérée par le noyau seront distingués.

La combinaison stockage fini, producteurs sans limite, consommateurs arrêtés et absence de perte ne peut pas être satisfaite indéfiniment. Le contrat choisit de préserver les engagements antérieurs et de limiter les admissions suivantes.

# 14 Temps et Will

**TIME-001 — Temps explicite.** Le cœur reçoit le temps comme une entrée. Le fonctionnement courant utilise une horloge monotone ; la reprise des échéances persistantes doit expliciter son lien avec l’heure civile et le temps écoulé pendant l’arrêt.

**TIME-002 — Incertitude.** Une horloge remontant dans le passé ou non fiable au redémarrage NE DOIT PAS provoquer une expiration massive implicite. Le profil qualifié doit décrire la source de temps, la borne d’erreur admise et le comportement de reprise lorsque cette hypothèse manque.

**TIME-003 — État dégradé.** Le choix recommandé est de conserver les états ambigus et de bloquer les décisions dépendantes d’une échéance indéterminable jusqu’à résolution, avec un diagnostic explicite. Ce mode de sauvegarde ne doit pas être présenté comme assurant toutes les échéances MQTT en temps réel.

**WILL-001 — Intention conservée.** En Durable, le broker DOIT conserver le payload du Will, son identité et une place pour son suivi avant un CONNACK réussi. Le quota compte les Wills actifs et les Wills déclenchés non encore admis. Lorsque cette capacité est épuisée, de nouvelles connexions demandant un Will sont refusées. La réserve de contrôle doit permettre de décider le déclenchement même lorsque les files de données sont pleines.

**WILL-002 — Déclenchement unique.** La décision entre annulation admissible et déclenchement DOIT être cohérente avec la session et la connexion concernées. Un ancien timer NE DOIT PAS agir sur un Will plus récent.

**WILL-003 — Publication autonome.** Le déclenchement crée durablement une publication interne en attente, avec une identité stable. La suppression de l’ancienne session et une reconnexion tardive ne l’annulent pas implicitement. La reprise peut répéter son transfert sans créer une seconde admission. Cela ne promet pas l’absence de retransmissions QoS 1 sur le réseau.

**WILL-004 — Saturation.** Le choix proposé est de réserver les destinataires et le fanout au point d’admission effective de cette publication interne. Si la capacité manque, le Will reste en attente et son ancienneté est observable. Les abonnements considérés sont ceux de cette admission, pas un instantané historique implicite de l’échéance. Les Wills déjà dus doivent progresser face aux nouvelles admissions ordinaires, sans affamer les contrôles qui libèrent les ressources. Une borne absolue sous saturation permanente demanderait un autre contrat, avec réservation du coût maximal à l’avance. La compatibilité de cette politique temporelle fait partie de la qualification du prototype.

L’instant exact d’une déconnexion pendant une panne complète peut être inconnu. La règle de reconstruction de cet instant devra être spécifiée et testée avant de qualifier les garanties temporelles du Will.

# 15 Terminaison des obligations

Le contrat Durable protège une responsabilité, mais n’impose pas de la conserver éternellement indépendamment des événements autorisés.

| Cause | Règle proposée |
| - | - |
| Échange sortant terminé | Libération normale de la responsabilité |
| Expiration applicable | Fin explicitement liée à la règle d’expiration et à la phase de livraison |
| Session destinataire expirée ou recréée | Fin des obligations appartenant à cette session |
| QoS effectif 0 | Pas de promesse de reprise comparable à QoS 1 ou 2 |
| Refus terminal du destinataire | Application de la règle MQTT, avec motif distinct |
| Limite protocolaire du destinataire | Terminaison ou traitement exigé par le protocole, avec diagnostic |
| Retained remplacé ou effacé | Fin de cette valeur retained selon une nouvelle opération autorisée, sans supprimer les distributions déjà engagées |
| Révocation d’autorisation | Priorité à la politique de sécurité selon sa frontière d’activation |
| Suppression administrative explicite | Action identifiée, bornée au périmètre demandé et observable |

**END-001 — Pas d’exception opportuniste.** Une saturation ordinaire NE DOIT PAS être transformée en suppression administrative automatique.

**END-002 — Traçabilité bornée.** Les terminaisons DOIVENT être observables par des compteurs et des événements de diagnostic. Le niveau de détail et la rétention des traces doivent eux-mêmes respecter les budgets. Un historique individuel infini n’est pas promis.

**END-003 — État de protocole.** La fin de l’obligation de distribution ne permet pas d’effacer un état QoS encore nécessaire à la terminaison correcte d’un échange.

**END-004 — Application.** Un accusé du destinataire n’établit pas qu’une transaction métier a été exécutée. Une telle garantie demanderait un protocole applicatif ou une transaction couvrant aussi le consommateur.

MQTT 5 possède notamment des contraintes de taille maximale et des causes d’expiration ou de refus du destinataire ; les traiter comme des pannes internes masquerait le contrat réel ([MQTT 5, sections 3.1.2, 3.3.2 et 4.8](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)).

# 16 Résultat de stockage indéterminé

**IO-001 — Trois résultats.** L’interface de stockage DOIT distinguer commis, non commis avec certitude, et indéterminé. Un timeout ou une réponse perdue ne suffit pas à conclure à l’échec de la transaction.

**IO-002 — Réconciliation.** En cas de résultat indéterminé, l’admission concernée est suspendue. L’identité de la tentative est conservée. La reprise interroge l’état durable et détermine les effets déjà engagés.

**IO-003 — Réponse honnête.** Le broker NE DOIT PAS envoyer un succès injustifié ni un rejet affirmant l’absence d’effet alors qu’un commit peut avoir eu lieu. Il peut fermer la connexion et imposer une récupération plutôt que fabriquer une certitude.

**IO-004 — Retry interne.** Une relance interne DOIT réutiliser l’identité appropriée et ne pas créer une nouvelle publication accidentelle. Un nouvel envoi applicatif indépendant du client est un autre événement et ne sera pas dédupliqué arbitrairement.

La perte d’un ACK après un commit fait partie des scénarios normaux à modéliser. Le producteur peut ignorer le résultat alors que le broker a déjà engagé ses responsabilités.

# 17 Qualification de la migration

**MIG-001 — Inventaire.** La migration DOIT relever les versions et empreintes, fichiers inclus, listeners, mécanismes d’authentification, certificats, ACL, plugins, bridges et états persistants. Le manifeste publié est expurgé des secrets.

**MIG-002 — Validation hors ligne.** Un outil proposé tel que `mqttiumd migrate check` DOIT pouvoir analyser la configuration sans ouvrir de listener ni modifier l’état source. Sa sortie structurée indique les directives reconnues, les transformations et les blocages. Cette commande est une exigence à développer, pas un outil existant.

**MIG-003 — Qualification fonctionnelle.** Le profil est testé avec des clients inchangés dans deux environnements isolés. Les comparaisons portent sur les propriétés attendues, pas sur une identité de tous les paquets lorsque plusieurs comportements sont licites.

**MIG-004 — Continuité d’état séparée.** Un importeur de sessions, d’inflight ou de retained DOIT disposer de ses propres tests. Un bridge ou un simple redémarrage des clients ne prouve pas le transfert de tous les états.

**MIG-005 — Bascule sans importeur.** La procédure DOIT annoncer l’interruption, le drainage ou le reset nécessaire et les données non transférées. Le terme transparent ne sera pas utilisé pour cette procédure.

**MIG-006 — Retour arrière.** La procédure de retour DOIT expliquer le devenir des données acceptées après la bascule. Restaurer l’ancien binaire et une ancienne sauvegarde ne réconcilie pas automatiquement ces écritures.

**MIG-007 — Critère de compatibilité.** La déclaration du profil exige zéro directive non classée, zéro divergence de sécurité non résolue et un rapport de tests portant sur les artefacts exacts.

# 18 Scénarios de réception prioritaires

Tous les scénarios ci-dessous sont des critères à implémenter et à exécuter. Leur présence dans ce chapitre ne constitue pas un résultat de test.

| Test | Injection | Résultat discriminant |
| - | - | - |
| T01 | Panne à chaque frontière entre réception, commit et ACK | Aucun ACK positif observé sans responsabilités récupérables |
| T02 | QoS 2, PUBREC reçu, crash, reprise avec seulement PUBREL | Achèvement sans payload redemandé et sans seconde admission |
| T03 | QoS 2, disparition de la session productrice après PUBREC | Les obligations déjà engagées vers d’autres sessions persistent |
| T04 | Fanout interrompu après prise en charge durable par B mais avant C | B déduplique le transfert interne ; C récupère son obligation |
| T05 | Nouvel abonnement entre PUBREC et PUBREL | Aucune recomputation rétroactive des destinataires |
| T06 | Même ClientID repris puis recréé, résultats IO retardés | Reprise correcte ; aucune contamination de la nouvelle session |
| T07 | QoS 1, ACK perdu puis identifiant réutilisé | Pas de déduplication excessive d’une nouvelle publication licite |
| T08 | Toutes les files pleines, Receive Maximum épuisé | Les contrôles de terminaison progressent ; budgets respectés |
| T09 | ENOSPC avant admission, puis après engagement | Refus avant responsabilité ; aucun effacement opportuniste après |
| T10 | Commit réussi dont le retour est perdu | Réconciliation sans deuxième engagement |
| T11 | Retained, remplacement d’abonnement et trafic simultané | Trace compatible avec le point d’activation et le replay définis |
| T12 | Will, reprise avant échéance, takeover et ancien timer | Une seule décision valide ; aucune action sur le nouveau Will |
| T13 | Horloge invalide après reboot, expiration en cours d’échange | Politique temporelle explicite et conservation de l’état protocolaire requis |
| T14 | Destinataire avec taille réduite ou refus terminal | Motif correct et observable, sans fausse promesse de livraison |
| T15 | QoS 2 partagé commencé puis session cible détruite | Aucune réaffectation interdite |
| T16 | Collecte de déduplication suivie d’un ancien transfert retardé | Aucun second effet interne ; métadonnées bornées |
| T17 | Reload invalide ou révocation pendant le trafic | Ancienne génération conservée ou nouvelle activée entièrement ; pas d’élargissement implicite |
| T18 | Carte ou disque qualifié soumis à des coupures réelles | Confrontation de la reprise aux hypothèses matérielles publiées |

Les scénarios DOIVENT distinguer publications entrantes, obligations internes, paquets retransmis et traitements applicatifs observés. Une valeur unique de compteur de messages ne suffit pas.

# 19 Preuves et critères de sortie

**VAL-001 — Cœur réel.** La simulation déterministe DOIT utiliser le véritable cœur de transitions destiné au broker. Les modèles abstraits restent reliés à des transitions et à des scénarios précis.

**VAL-002 — Indépendance.** L’agent qui définit l’oracle et celui qui réalise l’implémentation ne doivent pas pouvoir modifier conjointement les attentes sans revue explicite. Une réduction des états explorés ou un affaiblissement d’une propriété doit être visible.

**VAL-003 — Mutation.** Les tests critiques DOIVENT détecter des fautes injectées représentatives : ACK avant commit, génération ignorée, outbox libérée trop tôt, quotas non réservés, déduplication collectée trop tôt.

**VAL-004 — Portée des preuves.** Les rapports DOIVENT distinguer preuve abstraite, preuve sur du code réel, exploration bornée, test systémique et mesure de performance. Les hypothèses, limites, exclusions et fonctionnalités désactivées font partie du résultat.

**VAL-005 — Artefact qualifié.** Le dossier de livraison DOIT relier le commit exact au binaire livré, aux outils, aux configurations et aux traces. Une modification ultérieure invalide les conclusions dépendantes de son contenu tant qu’elles ne sont pas revalidées.

**VAL-006 — Comparaison équitable.** Toute comparaison avec Mosquitto, FlashMQ ou RMQTT DOIT fixer le même contrat utile : QoS, TLS, persistance, limites, traitement de la saturation et charge réellement admise. Les messages refusés ou perdus restent visibles dans les résultats.

La certification éventuelle sera choisie selon son objet et son organisme. Ce chapitre prépare des preuves inspectables ; il ne promet pas une certification universelle du logiciel.

# 20 Décisions recommandées et suite du travail

| Décision | Recommandation de cette version |
| - | - |
| Positionnement initial | Remplacement du profil standard Mosquitto, avec périmètre annoncé |
| Plateformes | Linux x86-64 et ARM64 |
| Mode d’exploitation proposé | Durable ; Volatile explicite |
| QoS 2 entrant | Responsabilités engagées avant PUBREC ; distribution indépendante de PUBREL |
| Saturation | Préserver les engagements, limiter les nouvelles admissions |
| Erreur de stockage ambiguë | Suspendre et réconcilier, sans mode volatile automatique |
| Reload invalide | Conserver la génération précédente |
| Import Mosquitto et extensions | Qualifications séparées |
| Horloge de reprise non fiable | Conservation prudente et état dégradé explicite |
| Architecture concrète | À sélectionner par prototypes, pas figée par ce chapitre |

Trois points doivent être résolus avant de lancer une implémentation large :

1. **Admission et protocole QoS 2** : modèle des identités, du commit, du rejeu, de la disparition de la session source et des abonnements concurrents.

2. **Transfert durable et collecte** : publication vers plusieurs propriétaires, reprise à toutes les frontières et collecte sûre des preuves.

3. **Budgets et événements différés** : absence de blocage circulaire, consommateur lent, Will sous saturation et reprise temporelle.

Le **chapitre 2** définira les types d’état, les événements, les effets, leurs identités, les points logiques d’activation et les frontières de commit. Il comparera ensuite les organisations du routage et du stockage au regard de ces trois prototypes.

L’évaluation d’une adoption ou d’un durcissement de FlashMQ ou RMQTT reste ouverte. Ces exigences constituent également une grille pour décider si une base existante permet d’atteindre les garanties souhaitées avec moins de travail qu’un nouveau cœur.
