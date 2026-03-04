# cesi-big-data — Pipeline de modération de contenu en temps réel

Pipeline de traitement de données Big Data construit sur **Apache Kafka** et **Apache Spark**. Le système ingère des messages de chat Twitch, détecte le contenu offensant par correspondance de mots-clés, et diffuse les résultats à travers une chaîne Kafka → Spark → Kafka → Elasticsearch → Kibana.

---

## Sommaire

1. [Architecture générale](#architecture-générale)
2. [Structure du projet](#structure-du-projet)
3. [Description des classes](#description-des-classes)
4. [Build](#build)
5. [Lancer l'infrastructure](#lancer-linfrastructure)
6. [Soumettre un job Spark](#soumettre-un-job-spark)
7. [Lancer un producteur Kafka en local](#lancer-un-producteur-kafka-en-local)
8. [Architecture Docker Compose](#architecture-docker-compose)
9. [Topics Kafka](#topics-kafka)
10. [Interfaces web](#interfaces-web)
11. [Configurer Kafka Connect vers Elasticsearch](#configurer-kafka-connect-vers-elasticsearch)

---

## Architecture générale

```
Twitch API ──► TwitchTracker ──► [twitch-messages] ──► TwitchAnalytics ──► [aggregated-messages]
                                                                                      │
posts.json ──► RandomPostProducer ──► [offensive-posts] ──► OffensiveDetectorStream ──► [offensive-posts-output]
                                                                                      │
                                                              Kafka Connect ──► Elasticsearch ──► Kibana
```

**Flux de données :**

1. `TwitchTracker` se connecte à l'API Twitch Helix, récupère les 15 plus gros streams en direct, rejoint leurs chats IRC et publie chaque message dans le topic Kafka `twitch-messages`.
2. `TwitchAnalytics` (Spark Streaming) consomme `twitch-messages`, agrège le nombre de messages par utilisateur et par chaîne sur des fenêtres glissantes d'1 minute, et publie le résultat dans `aggregated-messages`.
3. `OffensiveDetectorStream` (Spark Streaming) consomme `offensive-posts`, croise chaque message avec la liste de mots offensants, et publie les messages filtrés dans `offensive-posts-output`.
4. **Kafka Connect** (connecteur Elasticsearch Sink) indexe les topics de sortie dans Elasticsearch, visualisé via Kibana.

---

## Structure du projet

```
.
├── cesi-big-data/              ← Parent Maven multi-module (Java 17)
│   ├── pom.xml
│   ├── kafka/                  ← Module producteurs Kafka
│   │   └── src/main/java/com/example/kafka/
│   │       ├── KafkaUtils.java
│   │       ├── JsonSerializer.java
│   │       ├── RandomPostProducer.java
│   │       └── TwitchTracker.java
│   └── spark/                  ← Module jobs Spark (fat JAR via maven-shade)
│       └── src/main/java/com/example/spark/
│           ├── OffensiveDetector.java
│           ├── OffensiveDetectorStream.java
│           └── TwitchAnalytics.java
├── data/
│   ├── spark-apps/             ← JARs compilés + scripts shell (montés dans les conteneurs)
│   ├── spark-data/             ← Fichiers de données (offensive_words.txt, posts.json)
│   └── spark-events/           ← Logs d'événements Spark (pour l'History Server)
├── docker-compose.yml
└── create_sink.json            ← Configuration du connecteur Kafka Connect → Elasticsearch
```

---

## Description des classes

### Module `kafka`

#### `KafkaUtils`
Utilitaire partagé qui initialise le producteur Kafka et gère l'envoi asynchrone des messages.

- Crée un `KafkaProducer<String, Map<String, String>>` connecté à `localhost:9092`.
- Maintient une `BlockingQueue` de 10 000 messages pour découpler la production de l'envoi réseau.
- Un thread daemon consomme la queue en continu et envoie chaque message au topic Kafka cible.
- **Point d'entrée pour l'envoi :** `KafkaUtils.offer(key, message)` — non bloquant, retourne immédiatement.

#### `JsonSerializer`
Sérialiseur Kafka personnalisé (`Serializer<T>`) qui convertit n'importe quel objet Java en JSON (via Jackson) avant de l'envoyer sur le réseau. Remplace le `StringSerializer` par défaut pour les valeurs de type `Map<String, String>`.

#### `TwitchTracker`
Collecteur de données Twitch en temps réel.

- Se connecte à l'API Twitch Helix (via Twitch4J) avec les credentials `CLIENT_ID` / `CLIENT_SECRET` définis en tête de classe.
- Récupère les 15 streams les plus regardés (`TOP_N_STREAMS = 15`).
- Rejoint le chat IRC de chacun de ces streams.
- Pour chaque message reçu, construit un objet JSON `{channel, user, message, lang, date}` et l'envoie dans `twitch-messages` via `KafkaUtils.offer()`.

> Les credentials Twitch sont fournis par le cours. Les remplacer si l'API renvoie des erreurs de rate-limit.

---

### Module `spark`

#### `OffensiveDetectorStream` *(job streaming)*
Job Spark Structured Streaming — détection de contenu offensant en temps réel.

- Consomme le topic `offensive-posts` depuis Kafka (`kafka-cesi:29092`, offset `earliest`).
- Désérialise chaque message JSON avec le schéma `{platform, id, message}`.
- Effectue un **cross-join** entre les messages et la liste de mots offensants lue depuis `/opt/spark-data/offensive_words.txt`.
- Filtre les lignes où le message contient un mot offensant, puis regroupe par post pour collecter la liste des mots détectés.
- Publie les résultats enrichis dans le topic `offensive-posts-output` en mode `update`.
- Checkpoint dans `/tmp/checkpoints/offensive_detector` (effacé automatiquement par `start2.sh` avant soumission).

#### `TwitchAnalytics` *(job streaming)*
Job Spark Structured Streaming — agrégation des statistiques de chat Twitch.

- Consomme le topic `twitch-messages` depuis Kafka (`kafka-cesi:29092`, offset `earliest`).
- Désérialise le JSON `{channel, user, message, lang, date}` et extrait `channel`, `user` et `event_time`.
- Applique un **watermark d'1 minute** sur `event_time` pour gérer les messages en retard.
- Compte le nombre de messages par `(user, channel)` sur des **fenêtres glissantes d'1 minute**.
- Publie les agrégats dans le topic `aggregated-messages` en mode `update`.

---

## Build

> Prérequis : Java 17, Maven 3.x

```bash
# Compiler tous les modules depuis la racine du projet Maven
mvn package -f cesi-big-data/pom.xml

# Compiler uniquement le fat JAR Spark
# (sortie : data/spark-apps/spark-offensive-detector.jar)
mvn package -f cesi-big-data/spark/pom.xml

# Compiler uniquement le module Kafka
mvn package -f cesi-big-data/kafka/pom.xml
```

---

## Lancer l'infrastructure

```bash
docker compose up -d
```

Cela démarre l'ensemble des services : Zookeeper, Kafka, Kafka UI, Spark (master + workers + submit + history), Elasticsearch, Kibana et Kafka Connect.

---

## Soumettre un job Spark

```bash
# Obtenir un shell interactif dans le conteneur spark-submit
docker exec -it spark-submit bash

# Lancer n'importe quelle classe par son nom
bash /opt/spark-apps/start2.sh OffensiveDetectorStream
bash /opt/spark-apps/start2.sh TwitchAnalytics
```

> **Problème de fins de ligne Windows** : si `start.sh` répond "file not found", convertir en format Unix :
> ```bash
> sed -i 's/\r$//' /opt/spark-apps/start.sh
> ```

> **Checkpoints corrompus** : si un job streaming refuse de redémarrer, supprimer les checkpoints :
> ```bash
> rm -rf /tmp/checkpoints
> ```
> `start2.sh` effectue cette opération automatiquement.

---

## Lancer un producteur Kafka en local

```bash
# Depuis cesi-big-data/kafka/ après mvn package
java -cp target/kafka-1.0.0.jar com.example.kafka.RandomPostProducer
java -cp target/kafka-1.0.0.jar com.example.kafka.TwitchTracker
```

Les producteurs locaux utilisent `localhost:9092` (listener externe de Kafka).

---

## Architecture Docker Compose

Le fichier `docker-compose.yml` définit 11 services répartis en 3 groupes.

### Cluster Apache Spark

| Service | Rôle |
|---|---|
| `spark-master` | Nœud maître Spark — coordonne les workers, expose l'UI sur `:8080` et le port de soumission `:7077` |
| `spark-worker-1` | Worker Spark n°1 — exécute les tâches déléguées par le master, UI sur `:8081` |
| `spark-worker-2` | Worker Spark n°2 — second executeur pour la parallélisation |
| `spark-submit` | Conteneur client Spark sans rôle actif (reste vivant via `tail -f /dev/null`) — sert de point d'entrée pour soumettre des jobs via `docker exec -it spark-submit bash` |
| `spark-history` | Serveur d'historique Spark — lit les logs d'événements dans `/opt/spark-events` et les expose sur `:18080` |

Tous les conteneurs Spark partagent trois volumes bind-montés depuis l'hôte :
- `./data/spark-apps` → `/opt/spark-apps` : JARs et scripts de lancement
- `./data/spark-data` → `/opt/spark-data` : fichiers de données (`posts.json`, `offensive_words.txt`)
- `./data/spark-events` → `/opt/spark-events` : logs d'événements pour l'History Server

### Cluster Kafka

| Service | Rôle |
|---|---|
| `zookeeper-cesi` | Coordination du cluster Kafka (gestion des métadonnées, élection du leader) — port `:2181` |
| `kafka-cesi` | Broker Kafka unique — deux listeners : `localhost:9092` (accès depuis l'hôte) et `kafka-cesi:29092` (accès inter-conteneurs) |
| `kafka-ui-cesi` | Interface web Kafka UI — visualisation des topics, messages et consommateurs sur `:8085` |

**Réseau Kafka :**
- Depuis l'hôte : `localhost:9092`
- Entre conteneurs Docker : `kafka-cesi:29092`

### Stack Elasticsearch / Kibana / Kafka Connect

| Service | Rôle |
|---|---|
| `elasticsearch` | Moteur d'indexation et de recherche (mode single-node, sécurité désactivée) — port `:9200` |
| `kibana` | Tableau de bord de visualisation connecté à Elasticsearch — port `:5601` |
| `kafka-connect` | Passerelle Kafka → Elasticsearch — installe automatiquement le connecteur `confluentinc/kafka-connect-elasticsearch` au démarrage, API REST sur `:8083` |

---

## Interfaces web

| Service | URL |
|---|---|
| Spark Master UI | http://localhost:8080 |
| Spark Worker 1 | http://localhost:8081 |
| Spark History Server | http://localhost:18080 |
| Kafka UI | http://localhost:8085 |
| Elasticsearch | http://localhost:9200 |
| Kibana | http://localhost:5601 |
| Kafka Connect REST | http://localhost:8083 |

---

## Configurer Kafka Connect vers Elasticsearch

Une fois l'infrastructure démarrée, enregistrer le connecteur Sink via l'API REST de Kafka Connect :

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @create_sink.json
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @create_sink_viewers.json
```

ou sur windows :
```PowerShell
Invoke-RestMethod -Method Post -Uri "http://localhost:8083/connectors" -ContentType "application/json" -InFile "create_sink.json"
Invoke-RestMethod -Method Post -Uri "http://localhost:8083/connectors" -ContentType "application/json" -InFile "create_sink_viewers.json"
```

Le fichier `create_sink.json` configure le connecteur `ElasticsearchSinkConnector` pour indexer le topic `aggregated-messages` dans Elasticsearch à `http://elasticsearch:9200`.

Vérifier que le connecteur est actif :

```bash
curl http://localhost:8083/connectors/es-sink-connector/status
```
