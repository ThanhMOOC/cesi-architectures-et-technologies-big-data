package com.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class TwitchMessagesAnalytics {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        // 🧩 Fix Hadoop FileSystem login issue inside Docker
        System.setProperty("HADOOP_USER_NAME", "sparkuser");
        System.setProperty("user.name", "sparkuser");

        // 1️⃣ Init SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("TwitchMessagesAnalytics")
                .master("spark://spark-master:7077")
                .getOrCreate();

        Dataset<Row> kafkaDS = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-cesi:29092")
                /* we'll do this manual assignment to avoid windows/docker permissions issues */
//                .option("assign", "{\"offensive-posts\":[0]}")
                .option("subscribe", "twitch-messages")
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load();

        StructType schema = new StructType()
                .add("channel", "string")
                .add("user", "string")
                .add("message", "string")
                .add("lang", "string")
                .add("date", "timestamp");

        // key et value sont en binaire -> cast en STRING
        Dataset<Row> lines = kafkaDS.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        // Parsing du json
        .select(
                functions.col("key"),
                functions.from_json(functions.col("value"), schema).alias("json")
        )
        // Flatten the json
        .select(
                functions.col("key"),
                functions.col("json.channel").alias("channel"),
                functions.col("json.user").alias("user"),
                functions.col("json.date").alias("event_time")
        )
        .withWatermark("event_time", "1 minute")
        // Grouping par minute
        .groupBy(functions.window(functions.col("event_time"), "1 minute"), functions.col("user"), functions.col("channel"))
        .count();

        lines
                .selectExpr("CAST(channel AS STRING) AS key", "to_json(struct(*)) AS value")
                .writeStream()
                .outputMode("update")
//                .outputMode("append")
                .format("kafka")
//                .format("console")
                .option("kafka.bootstrap.servers", "kafka-cesi:29092")
                .option("topic", "aggregated-messages")
                .option("checkpointLocation", "/tmp/checkpoints/offensive_detector")
                .start()
                .awaitTermination();

    }

}
