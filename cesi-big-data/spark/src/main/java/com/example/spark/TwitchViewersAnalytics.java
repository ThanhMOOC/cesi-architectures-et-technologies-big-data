package com.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class TwitchViewersAnalytics {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        // 🧩 Fix Hadoop FileSystem login issue inside Docker
        System.setProperty("HADOOP_USER_NAME", "sparkuser");
        System.setProperty("user.name", "sparkuser");

        // 1️⃣ Init SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("TwitchViewersAnalytics")
                .master("spark://spark-master:7077")
                .getOrCreate();

        Dataset<Row> kafkaDS = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-cesi:29092")
                .option("subscribe", "twitch-viewers")
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load();

        // Tous les champs sont des strings car le producteur utilise Map<String, String>
        StructType schema = new StructType()
                .add("userId", "string")
                .add("channel", "string")
                .add("viewerCount", "string")
                .add("language", "string")
                .add("date", "timestamp");

        // key et value sont en binaire -> cast en STRING
        Dataset<Row> lines = kafkaDS.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        // Parsing du json
        .select(
                functions.col("key"),
                functions.from_json(functions.col("value"), schema).alias("json")
        )
        // Flatten the json + cast viewerCount string -> integer
        .select(
                functions.col("key"),
                functions.col("json.channel").alias("channel"),
                functions.col("json.viewerCount").cast("integer").alias("viewerCount"),
                functions.col("json.date").alias("event_time")
        )
        .withWatermark("event_time", "1 minute")
        // Agrégation par fenêtre d'1 minute et par chaîne
        .groupBy(functions.window(functions.col("event_time"), "1 minute"), functions.col("channel"))
        .agg(functions.max("viewerCount").as("max_viewers"));

        lines
                .selectExpr("CAST(channel AS STRING) AS key", "to_json(struct(*)) AS value")
                .writeStream()
                .outputMode("update")
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-cesi:29092")
                .option("topic", "aggregated-viewers")
                .option("checkpointLocation", "/tmp/checkpoints/twitch_viewers_analytics")
                .start()
                .awaitTermination();
    }
}
