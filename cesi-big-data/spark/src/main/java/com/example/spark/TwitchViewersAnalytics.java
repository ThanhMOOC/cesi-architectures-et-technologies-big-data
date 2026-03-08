package com.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
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

        // 2️⃣ Parse JSON + data cleaning
        StructType schema = new StructType()
                .add("userId", "string")
                .add("channel", "string")
                .add("viewerCount", "string")
                .add("language", "string")
                .add("date", "string");

        Dataset<Row> cleaned = kafkaDS
                .selectExpr("CAST(value AS STRING) AS raw_json")
                .select(functions.from_json(functions.col("raw_json"), schema).alias("json"))
                .select("json.*")
                .withColumn("channel", functions.lower(functions.trim(functions.col("channel"))))
                .withColumn("viewer_count", functions.col("viewerCount").cast("int"))
                .withColumn("event_time", functions.to_timestamp(functions.col("date")))
                .filter(functions.col("channel").isNotNull().and(functions.length(functions.col("channel")).gt(0)))
                .filter(functions.col("viewer_count").isNotNull())
                .filter(functions.col("event_time").isNotNull())
                .select("channel", "viewer_count", "event_time");

        // 3️⃣ KPI: max viewers per channel in 1-minute windows
        Dataset<Row> aggregated = cleaned
                .withWatermark("event_time", "1 minute")
                .groupBy(
                        functions.window(functions.col("event_time"), "1 minute"),
                        functions.col("channel")
                )
                .agg(functions.max(functions.col("viewer_count")).alias("max_viewers"));

        // 4️⃣ Publish aggregates to Kafka topic: aggregated-viewers
        Dataset<Row> kafkaOutput = aggregated.selectExpr(
                "CAST(concat(channel, '|', CAST(window.start AS STRING)) AS STRING) AS key",
                "to_json(named_struct('window_start', CAST(window.start AS STRING), 'window_end', CAST(window.end AS STRING), 'channel', channel, 'max_viewers', max_viewers)) AS value"
        );

        StreamingQuery viewersWindowQuery = kafkaOutput
                .writeStream()
                .outputMode("update")
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-cesi:29092")
                .option("topic", "aggregated-viewers")
                .option("checkpointLocation", "/tmp/checkpoints/twitch_viewers_analytics")
                .start();

        // 5️⃣ KPI session: max viewers reached per channel (session scope)
        Dataset<Row> maxViewersSession = cleaned
                .withWatermark("event_time", "365 days")
                .groupBy(functions.col("channel"))
                .agg(functions.max(functions.col("viewer_count")).alias("max_viewers_session"));

        Dataset<Row> maxViewersSessionKafkaOutput = maxViewersSession.selectExpr(
                "CAST(channel AS STRING) AS key",
                "to_json(named_struct('channel', channel, 'max_viewers_session', max_viewers_session)) AS value"
        );

        StreamingQuery maxViewersSessionQuery = maxViewersSessionKafkaOutput
                .writeStream()
                .outputMode("update")
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-cesi:29092")
                .option("topic", "kpi-max-viewers-session")
                .option("checkpointLocation", "/tmp/checkpoints/kpi_max_viewers_session")
                .start();

        spark.streams().awaitAnyTermination();
    }
}
