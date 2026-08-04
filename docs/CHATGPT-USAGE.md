# Persistent Workbench — instructions courtes pour agent

> **Agent-only.** Utiliser ce mécanisme pour développer sur `yoch/mqttium` via ChatGPT **uniquement**. Ce n’est pas le workflow destiné aux contributeurs ordinaires.

## Principe

* **Plugin GitHub** : lire la PR et les fichiers, déposer le job, poster `/cgw`, lire les résultats et la CI.
* **Workbench** : appliquer le patch, exécuter Docker et les tests, committer et pousser.

Ne pas effectuer un développement multi-fichiers avec une succession de `create_file` / `update_file`. Utiliser ces actions uniquement pour le fichier de transport du job ou une opération GitHub ponctuelle.

## Boucle normale

1. Lire la PR cible.
2. Vérifier qu’elle est ouverte, issue de `yoch/mqttium`, et récupérer :

   * sa branche head ;
   * son SHA head exact.
3. Lire uniquement les fichiers nécessaires.
4. Construire un patch Git atomique.
5. Créer un `job_id` inédit.
6. Déposer le job sur `chatgpt-workbench-jobs`.
7. Poster exactement :

   ```text
   /cgw job=<job_id>
   ```
8. Lire le commentaire du bot.
9. Après un push, relire immédiatement le nouveau SHA.
10. Vérifier la CI du commit poussé avant de continuer.

Un seul job avec `push: true` peut être actif par PR. Ne jamais préparer deux pushes depuis le même SHA.

## Contrat du job

Toujours renseigner explicitement `push`. Son défaut dans le code est `true`.

```json
{
  "version": 1,
  "job_id": "fix-example-20260804-01",
  "expected_head": "<SHA-40>",
  "patch": "diff --git ...",
  "setup": [],
  "checks": [
    {
      "name": "targeted tests",
      "image": "python:3.12-bookworm",
      "command": "python -m pip install -e '.[dev]' && python -m pytest -q tests/unit/test_target.py",
      "timeout": 1800,
      "network": true
    }
  ],
  "commit_message": "Fix example behavior",
  "push": true
}
```

Règles :

* `job_id` : `[A-Za-z0-9._-]`, maximum 80 caractères, jamais réutilisé.
* `expected_head` : SHA exact de la PR.
* Il doit encore correspondre au SHA inclus dans le payload au moment où `/cgw` est posté.
* `setup` et `checks` : maximum 20 entrées chacun.
* `image` : obligatoire pour chaque entrée.
* `command` : obligatoire, maximum 8000 caractères.
* `timeout` : entre 1 et 7200 secondes.
* `commit_message` : obligatoire lorsque `push` vaut `true`.

Images autorisées :

```text
python:3.12-bookworm
node:24-bookworm
ubuntu:24.04
```

## Dépôt du job

Encoder le JSON ainsi :

```python
encoded = base64.urlsafe_b64encode(
    gzip.compress(json.dumps(job).encode("utf-8"))
).decode("ascii").rstrip("=")
```

Créer ensuite :

```text
.cgw/jobs/<job_id>.b64
```

sur :

```text
chatgpt-workbench-jobs
```

Ne pas ouvrir de PR pour cette branche de transport.

## Docker

Chaque commande est exécutée dans un conteneur neuf. Seuls le workspace et le cache persistent.

Donc éviter :

```text
setup: pip install ...
checks: pytest
```

L’installation du premier conteneur ne sera pas présente dans le second.

Préférer une commande autonome :

```bash
python -m pip install -e '.[dev]' &&
python -m pytest -q tests/unit
```

Pour un environnement persistant :

```bash
python -m venv /cache/venv &&
/cache/venv/bin/pip install -e '.[dev]' &&
/cache/venv/bin/python -m pytest -q tests/unit
```

Les conteneurs sont non-root. Ne pas utiliser `sudo` ou `apt-get`.

Activer `"network": true` uniquement lorsqu’un téléchargement est requis.

Configuration infrastructure actuelle :

```text
CGW_DOCKER_SNAP_COMPAT=true
CGW_DOCKER_UID/GID facultatifs
```

En leur absence, l’UID/GID du runner est détecté automatiquement.

## Autorisation du slash command

L’auteur de `/cgw` doit disposer au minimum du niveau configuré dans :

```text
CGW_REQUIRED_PERMISSION
```

La valeur par défaut actuelle est :

```text
admin
```

Une absence de réaction emoji ne prouve pas un échec. Vérifier le run `repository_dispatch` et le commentaire final.

## Résultat attendu

Le commentaire bot expose notamment :

```text
Execution: success|failure
Push: success|failure|disabled|skipped
Commit: <sha éventuel>
Run: <URL>
```

Interprétation :

* `success / disabled` : validation sans modification de branche.
* `success / success` : commit poussé.
* `failure / skipped` : échec avant push.
* `success / failure` : patch et tests réussis, problème de push.

Artefact :

```text
cgw-<job_id>-<run_id>
```

Résultats persistants :

```text
/home/yoch/chatgpt-workbench/results/<owner>/<repo>/<job_id>
```

Contenu utile :

```text
job.json
job.patch
final.patch
state.json
logs/
```

## Politique anti-boucle

Ne jamais relancer aveuglément le même job.

Après un échec, classifier :

* **aucun run** : dispatcher, syntaxe `/cgw`, permission ou job absent ;
* **validation** : schéma, image, timeout, commande, SHA ou patch ;
* **git apply** : branche déplacée ou patch obsolète ;
* **Docker** : image, droits, réseau ou commande root ;
* **checks** : problème applicatif ou dépendances non persistantes ;
* **push** : token, permissions ou branche déplacée ;
* **CI** : inspecter uniquement les jobs et logs en échec.

Toujours créer un nouveau `job_id` après correction.

Ne jamais :

* republier la même commande ;
* réutiliser un ancien SHA ;
* utiliser un force-push ;
* contourner un problème de secret par un changement de code ;
* modifier massivement la branche avec le plugin GitHub ;
* lancer un second push avant la fin du premier.

Après deux échecs de même nature, réexaminer l’hypothèse au lieu de répéter la procédure.

## Modifications du Workbench

Les changements touchant :

```text
.github/chatgpt-workbench/processor.py
.github/workflows/cgw-command.yml
```

doivent suivre ce cycle :

1. branche dédiée ;
2. PR vers `main` ;
3. validation par `controller-smoke` et CI ;
4. fusion ;
5. test `/cgw` réel.

Un `repository_dispatch` utilise le contrôleur de la branche par défaut. Un `/cgw` lancé avant la fusion ne teste donc pas la nouvelle version du contrôleur.

## Définition de terminé

Pour `push: true` :

* `Execution: success`;
* `Push: success`;
* SHA de la PR modifié ;
* CI du nouveau commit verte ;
* diff limité aux changements souhaités ;
* aucun fichier temporaire ou smoke restant.

Pour `push: false` :

* `Execution: success`;
* `Push: disabled`;
* checks verts ;
* SHA de la PR inchangé.
