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

        /* Here you can write the aggregation pipeline logic */

//        lines
//                .selectExpr("CAST(channel AS STRING) AS key", "to_json(struct(*)) AS value")
//                .writeStream()
//                .outputMode("update")
//                .format("kafka")
//                .option("kafka.bootstrap.servers", "kafka-cesi:29092")
//                .option("topic", "aggregated-messages")
//                .option("checkpointLocation", "/tmp/checkpoints/offensive_detector")
//                .start()
//                .awaitTermination();

    }

}
