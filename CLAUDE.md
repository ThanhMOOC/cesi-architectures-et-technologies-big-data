# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CESI Big Data course project — a real-time content moderation pipeline built on Apache Kafka and Apache Spark. The system ingests Twitch chat messages, detects offensive content using keyword matching, and streams results through a Kafka → Spark → Kafka → Elasticsearch pipeline.

## Build & Run

### Build (from `cesi-big-data/`)

```bash
# Build all modules
mvn package -f cesi-big-data/pom.xml

# Build only the Spark fat JAR (outputs to data/spark-apps/spark-offensive-detector.jar)
mvn package -f cesi-big-data/spark/pom.xml

# Build only the Kafka module
mvn package -f cesi-big-data/kafka/pom.xml
```

### Start the infrastructure

```bash
docker compose up -d
```

### Submit a Spark job

```bash
# Get an interactive shell on the spark-submit container
docker exec -it spark-submit bash

# Inside the container — run OffensiveDetector (batch)
bash /opt/spark-apps/start.sh

# Run any class by name (OffensiveDetectorStream, TwitchAnalytics, etc.)
bash /opt/spark-apps/start2.sh OffensiveDetectorStream
```

> **Windows line-ending issue**: If `start.sh` reports "file not found", convert to Unix line endings:
> `sed -i 's/\r$//' /opt/spark-apps/start.sh`

### Run a Kafka producer locally

```bash
# From cesi-big-data/kafka/ after mvn package
java -cp target/kafka-1.0.0.jar com.example.kafka.RandomPostProducer
java -cp target/kafka-1.0.0.jar com.example.kafka.TwitchTracker
```

## Architecture

### Module layout

```
cesi-big-data/          ← Maven multi-module parent (Java 17)
  kafka/                ← Kafka producer module
  spark/                ← Spark processing module (fat JAR via maven-shade)
data/
  spark-apps/           ← Compiled JARs + shell scripts (bind-mounted into containers)
  spark-data/           ← Input data files (offensive_words.txt, posts.json)
  spark-events/         ← Spark event logs (for History Server)
docker-compose.yml      ← Full cluster definition
```

### Data flow

```
Twitch API ──► TwitchTracker ──► Kafka: twitch-messages ──► TwitchAnalytics ──► Kafka: aggregated-messages
                                                                                         │
posts.json ──► RandomPostProducer ──► Kafka: offensive-posts ──► OffensiveDetectorStream ──► Kafka: offensive-posts-output
                                                                                         │
                                                                       Kafka Connect ──► Elasticsearch ──► Kibana
```

### Key classes

| Class | Module | Role |
|---|---|---|
| `KafkaUtils` | kafka | Shared async producer with a 10k-message `BlockingQueue`; use `KafkaUtils.offer()` to enqueue |
| `JsonSerializer` | kafka | Custom Kafka `Serializer<Map<String,String>>` using Jackson |
| `RandomPostProducer` | kafka | Simulates posts by replaying `posts.json` at random; entry point for offline testing |
| `TwitchTracker` | kafka | Connects to Twitch Helix API + Chat, tracks top 15 streams, sends to `twitch-messages` |
| `OffensiveDetector` | spark | **Batch** job — joins `/opt/spark-data/posts.json` against `offensive_words.txt` |
| `OffensiveDetectorStream` | spark | **Streaming** job — reads `offensive-posts`, cross-joins offensive words list, writes to `offensive-posts-output` |
| `TwitchAnalytics` | spark | **Streaming** job — reads `twitch-messages`, windowed 1-minute count per user+channel, writes to `aggregated-messages` |

### Network topology

- **From the host**: Kafka is reachable at `localhost:9092`
- **Between containers**: Kafka is reachable at `kafka-cesi:29092`
- Spark jobs run inside Docker and always use `kafka-cesi:29092`
- Kafka producers running locally use `localhost:9092`

### Docker services & UIs

| Service | URL |
|---|---|
| Spark Master UI | http://localhost:8080 |
| Spark Worker 1 | http://localhost:8081 |
| Spark History Server | http://localhost:18080 |
| Kafka UI | http://localhost:8085 |
| Elasticsearch | http://localhost:9200 |
| Kibana | http://localhost:5601 |
| Kafka Connect REST | http://localhost:8083 |

### Streaming checkpoint issue

If a streaming job (e.g. `OffensiveDetectorStream`) fails to restart, clear stale checkpoints inside the container:

```bash
rm -rf /tmp/checkpoints
```

`start2.sh` does this automatically before submitting.

## Kafka Topics

| Topic | Producer | Consumer |
|---|---|---|
| `twitch-messages` | `TwitchTracker` | `TwitchAnalytics` |
| `offensive-posts` | `RandomPostProducer` | `OffensiveDetectorStream` |
| `offensive-posts-output` | `OffensiveDetectorStream` | Kafka Connect → Elasticsearch |
| `aggregated-messages` | `TwitchAnalytics` | — |

## Twitch credentials

`TwitchTracker` has hardcoded `CLIENT_ID` and `CLIENT_SECRET` at the top of the class. These are course-provided credentials — replace them with your own if the API rate-limits.
