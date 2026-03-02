#!/bin/bash
set -e

# 🧩 Créer un utilisateur système (corrige le bug Hadoop/UnixPrincipal)
useradd -m sparkuser || true
export USER=sparkusers
export HADOOP_USER_NAME=sparkuser
export SPARK_HOME=/opt/bitnami/spark
export HOME=/opt/bitnami/spark

mkdir -p "$HOME/.ivy2"
echo "✅ Running as $USER"

# 🚀 Lancer le job Spark
$SPARK_HOME/bin/spark-submit \
  --conf spark.jars.ivy=/opt/bitnami/spark/.ivy2 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=/opt/spark-events \
  --master spark://spark-master:7077 \
  --class com.example.spark.OffensiveDetector \
  /opt/spark-apps/spark-offensive-detector.jar
