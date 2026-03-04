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
Twitch API ──► TwitchMessageTracker ──► [twitch-messages]  ──► TwitchMessagesAnalytics ──► [aggregated-messages]  ──┐
           └─► TwitchViewerTracker  ──► [twitch-viewers]   ──► TwitchViewersAnalytics  ──► [aggregated-viewers]   ──┤
                                                                                                                     │
                                            
                                                                                                                     │
                                                                               Kafka Connect ──► Elasticsearch ──► Kibana
```

**Flux de données :**

1. `TwitchMessageTracker` se connecte à l'API Twitch Helix, récupère les 15 plus gros streams en direct, rejoint leurs chats IRC et publie chaque message dans `twitch-messages`.
2. `TwitchViewerTracker` interroge l'API Twitch toutes les minutes pour récupérer le nombre de spectateurs de chaque stream et publie dans `twitch-viewers`.
3. `TwitchMessagesAnalytics` (Spark Streaming) consomme `twitch-messages`, agrège le nombre de messages par utilisateur et par chaîne sur des fenêtres d'1 minute, et publie dans `aggregated-messages`.
4. `TwitchViewersAnalytics` (Spark Streaming) consomme `twitch-viewers`, calcule le maximum de spectateurs par chaîne sur des fenêtres d'1 minute, et publie dans `aggregated-viewers`.
5. `OffensiveDetectorStream` (Spark Streaming) consomme `offensive-posts`, croise chaque message avec la liste de mots offensants, et publie les messages filtrés dans `offensive-posts-output`.
6. **Kafka Connect** (connecteur Elasticsearch Sink) indexe les topics de sortie dans Elasticsearch, visualisé via Kibana.

---

## Structure du projet

```
.
├── cesi-big-data/                        ← Parent Maven multi-module (Java 17)
│   ├── pom.xml
│   ├── kafka/                            ← Module producteurs Kafka
│   │   └── src/main/
│   │       ├── java/com/example/kafka/
│   │       │   ├── KafkaUtils.java           # Producteur asynchrone partagé
│   │       │   ├── JsonSerializer.java        # Sérialiseur Jackson personnalisé
│   │       │   ├── PropertiesUtils.java       # Chargeur de application.properties
│   │       │   ├── RandomPostProducer.java    # Rejoue posts.json vers offensive-posts
│   │       │   ├── TwitchMessageTracker.java  # Version avec credentials depuis properties
│   │       │   └── TwitchViewerTracker.java   # Sondage du nombre de spectateurs
│   │       └── resources/
│   │           ├── application.properties     # Credentials Twitch API
│   └── spark/                            ← Module jobs Spark (fat JAR via maven-shade)
│       └── src/main/
│           ├── java/com/example/spark/
│           │   ├── OffensiveDetector.java          # Job batch : détection offline
│           │   ├── OffensiveDetectorStream.java    # Job streaming : détection temps réel
│           │   ├── TwitchMessagesAnalytics.java    # Variante de TwitchAnalytics
│           │   └── TwitchViewersAnalytics.java     # Job streaming : max spectateurs
│           └── resources/
│               ├── offensive_words.txt             # Liste de 200+ mots offensants
│               └── start2.sh                       # Script spark-submit (streaming, arg)
├── data/
│   ├── spark-apps/                       ← JARs compilés + scripts (montés dans les conteneurs)
│   ├── spark-data/                       ← Fichiers de données (offensive_words.txt, posts.json)
│   └── spark-events/                     ← Logs d'événements Spark (pour l'History Server)
├── docker-compose.yml
├── create_sink.json                      ← Connecteur Kafka Connect → ES (aggregated-messages)
└── create_sink_viewers.json              ← Connecteur Kafka Connect → ES (aggregated-viewers)
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

#### `PropertiesUtils`
Charge `application.properties` depuis le classpath au démarrage de la classe. Expose un getter statique `PropertiesUtils.get(key)` utilisé par `TwitchMessageTracker` et `TwitchViewerTracker` pour récupérer les credentials Twitch.

#### `RandomPostProducer`
Simule la production de posts offensants à partir du fichier `posts.json` embarqué dans le JAR.

- Produit dans le topic `offensive-posts`.
- Rejoue les posts dans un ordre aléatoire avec une pause de 100–300 ms entre chaque envoi.
- Chaque message est envoyé avec l'ID du post comme clé.

#### `TwitchTracker`
Collecteur de données Twitch — version avec **credentials hardcodés** en tête de classe (`CLIENT_ID`, `CLIENT_SECRET`).

- Récupère les 15 streams les plus regardés via l'API Twitch Helix.
- Rejoint le chat IRC de chacun de ces streams.
- Pour chaque message reçu, envoie un JSON `{channel, user, message, lang, date}` dans `twitch-messages`.

> Utiliser `TwitchMessageTracker` en priorité ; `TwitchTracker` est conservé pour compatibilité.

#### `TwitchMessageTracker`
Variante de `TwitchTracker` qui charge les credentials depuis `application.properties` via `PropertiesUtils`. Comportement identique : suivi des 15 tops streams, envoi des messages chat dans `twitch-messages`.

> Les credentials Twitch sont fournis par le cours. Les remplacer dans `application.properties` si l'API renvoie des erreurs de rate-limit.

#### `TwitchViewerTracker`
Sonde le nombre de spectateurs des 15 tops streams **toutes les minutes** (via un `ScheduledExecutorService`).

- Produit dans le topic `twitch-viewers`.
- Message format : `{userId, channel, viewerCount, language, date}`.
- Credentials chargés depuis `application.properties`.

---

### Module `spark`

#### `OffensiveDetector` *(job batch)*
Job Spark batch — détection offline de contenu offensant.

- Lit les posts depuis `/opt/spark-data/posts.json`.
- Lit la liste de mots offensants depuis `/opt/spark-data/offensive_words.txt`.
- Effectue un cross-join, filtre les correspondances et affiche les résultats en console.
- Utilisé pour valider la logique de détection hors streaming.

#### `OffensiveDetectorStream` *(job streaming)*
Job Spark Structured Streaming — détection de contenu offensant en temps réel.

- Consomme le topic `offensive-posts` depuis Kafka (`kafka-cesi:29092`, offset `earliest`).
- Désérialise chaque message JSON avec le schéma `{platform, id, message}`.
- Effectue un **cross-join** entre les messages et la liste de mots offensants lue depuis `/opt/spark-data/offensive_words.txt`.
- Filtre les lignes où le message contient un mot offensant, puis regroupe par post pour collecter la liste des mots détectés.
- Publie les résultats enrichis dans le topic `offensive-posts-output` en mode `update`.
- Checkpoint : `/tmp/checkpoints/offensive_detector` (effacé automatiquement par `start2.sh`).

#### `TwitchAnalytics` / `TwitchMessagesAnalytics` *(job streaming)*
Jobs Spark Structured Streaming équivalents — agrégation des statistiques de chat Twitch.

- Consomme `twitch-messages` depuis Kafka (`kafka-cesi:29092`).
- Désérialise `{channel, user, message, lang, date}` et extrait `channel`, `user` et `event_time`.
- Applique un **watermark d'1 minute** sur `event_time` pour gérer les messages en retard.
- Compte le nombre de messages par `(user, channel)` sur des **fenêtres de 1 minute**.
- Publie les agrégats dans `aggregated-messages` en mode `update`.
- Checkpoint : `/tmp/checkpoints/offensive_detector`.

#### `TwitchViewersAnalytics` *(job streaming)*
Job Spark Structured Streaming — agrégation du nombre de spectateurs par chaîne.

- Consomme `twitch-viewers` depuis Kafka (`kafka-cesi:29092`).
- Désérialise `{userId, channel, viewerCount, language, date}` et caste `viewerCount` en entier.
- Applique un **watermark d'1 minute** sur `event_time`.
- Calcule le **maximum de spectateurs** par chaîne sur des **fenêtres de 1 minute**.
- Publie les agrégats dans `aggregated-viewers` en mode `update`.
- Checkpoint : `/tmp/checkpoints/twitch_viewers_analytics`.

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

Cela démarre l'ensemble des 11 services : Zookeeper, Kafka, Kafka UI, Spark (master + 2 workers + submit + history), Elasticsearch, Kibana et Kafka Connect.

---

## Soumettre un job Spark

```bash
# Obtenir un shell interactif dans le conteneur spark-submit
docker exec -it spark-submit bash

# Lancer n'importe quelle classe par son nom (efface les checkpoints avant soumission)
bash /opt/spark-apps/start2.sh OffensiveDetectorStream
bash /opt/spark-apps/start2.sh TwitchMessagesAnalytics
bash /opt/spark-apps/start2.sh TwitchViewersAnalytics

> **Problème de fins de ligne Windows** : si `start.sh` répond "file not found", convertir en format Unix :
> ```bash
> sed -i 's/\r$//' /opt/spark-apps/start.sh /opt/spark-apps/start2.sh
> ```

> **Checkpoints corrompus** : si un job streaming refuse de redémarrer, supprimer manuellement :
> ```bash
> rm -rf /tmp/checkpoints
> ```
> `start2.sh` effectue cette opération automatiquement avant chaque soumission.

---

## Lancer un producteur Kafka en local

```bash
# Depuis cesi-big-data/kafka/ après mvn package

# Simuler des posts offensants (rejoue posts.json)
java -cp target/kafka-1.0.0.jar com.example.kafka.RandomPostProducer

# Collecter les messages chat Twitch (credentials en dur)
java -cp target/kafka-1.0.0.jar com.example.kafka.TwitchTracker

# Collecter les messages chat Twitch (credentials depuis application.properties)
java -cp target/kafka-1.0.0.jar com.example.kafka.TwitchMessageTracker

# Collecter les compteurs de spectateurs Twitch
java -cp target/kafka-1.0.0.jar com.example.kafka.TwitchViewerTracker
```

Les producteurs locaux utilisent `localhost:9092` (listener externe de Kafka).

---

## Architecture Docker Compose

Le fichier `docker-compose.yml` définit 11 services répartis en 3 groupes.

### Cluster Apache Spark

| Service | Rôle |
|---|---|
| `spark-master` | Nœud maître Spark — coordonne les workers, UI sur `:8080`, soumission sur `:7077` |
| `spark-worker-1` | Worker Spark n°1 — exécute les tâches déléguées par le master, UI sur `:8081` |
| `spark-worker-2` | Worker Spark n°2 — second exécuteur pour la parallélisation |
| `spark-submit` | Conteneur client (reste vivant via `tail -f /dev/null`) — point d'entrée pour `docker exec` |
| `spark-history` | Serveur d'historique — lit les logs dans `/opt/spark-events`, expose sur `:18080` |

Tous les conteneurs Spark partagent trois volumes bind-montés depuis l'hôte :
- `./data/spark-apps` → `/opt/spark-apps` : JARs et scripts de lancement
- `./data/spark-data` → `/opt/spark-data` : fichiers de données (`posts.json`, `offensive_words.txt`)
- `./data/spark-events` → `/opt/spark-events` : logs d'événements pour l'History Server

### Cluster Kafka

| Service | Rôle |
|---|---|
| `zookeeper-cesi` | Coordination du cluster Kafka — port `:2181` |
| `kafka-cesi` | Broker Kafka unique — `localhost:9092` (hôte) et `kafka-cesi:29092` (inter-conteneurs) |
| `kafka-ui-cesi` | Interface web Kafka UI — topics, messages et consommateurs sur `:8085` |

**Réseau Kafka :**
- Depuis l'hôte : `localhost:9092`
- Entre conteneurs Docker : `kafka-cesi:29092`

### Stack Elasticsearch / Kibana / Kafka Connect

| Service | Rôle |
|---|---|
| `elasticsearch` | Moteur d'indexation et de recherche (single-node, sécurité désactivée) — port `:9200` |
| `kibana` | Tableau de bord de visualisation connecté à Elasticsearch — port `:5601` |
| `kafka-connect` | Passerelle Kafka → Elasticsearch — installe automatiquement `confluentinc/kafka-connect-elasticsearch`, API REST sur `:8083` |

---

## Topics Kafka

| Topic | Producteur | Consommateur |
|---|---|---|
| `twitch-messages` | `TwitchMessageTracker` / `TwitchTracker` | `TwitchMessagesAnalytics` / `TwitchAnalytics` |
| `twitch-viewers` | `TwitchViewerTracker` | `TwitchViewersAnalytics` |
| `offensive-posts` | `RandomPostProducer` | `OffensiveDetectorStream` |
| `offensive-posts-output` | `OffensiveDetectorStream` | Kafka Connect → Elasticsearch |
| `aggregated-messages` | `TwitchMessagesAnalytics` | Kafka Connect → Elasticsearch |
| `aggregated-viewers` | `TwitchViewersAnalytics` | Kafka Connect → Elasticsearch |

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

Une fois l'infrastructure démarrée, enregistrer les connecteurs Sink via l'API REST de Kafka Connect :

```bash
# Messages de chat agrégés
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @create_sink.json

# Compteurs de spectateurs agrégés
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @create_sink_viewers.json
```

Ou sur Windows (PowerShell) :
```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8083/connectors" -ContentType "application/json" -InFile "create_sink.json"
Invoke-RestMethod -Method Post -Uri "http://localhost:8083/connectors" -ContentType "application/json" -InFile "create_sink_viewers.json"
```

- `create_sink.json` : indexe `aggregated-messages` dans Elasticsearch (`es-sink-connector`)
- `create_sink_viewers.json` : indexe `aggregated-viewers` dans Elasticsearch (`es-sink-connector-viewers`)

Vérifier que les connecteurs sont actifs :

```bash
curl http://localhost:8083/connectors/es-sink-connector/status
curl http://localhost:8083/connectors/es-sink-connector-viewers/status
```
