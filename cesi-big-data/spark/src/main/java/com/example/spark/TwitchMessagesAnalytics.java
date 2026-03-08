package com.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
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
                .option("subscribe", "twitch-messages")
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load();

        // 2️⃣ Parse JSON + data cleaning + GDPR Compliance (Hashing user ID)
        StructType schema = new StructType()
                .add("channel", "string")
                .add("user", "string")
                .add("message", "string")
                .add("lang", "string")
                .add("date", "string");

        Dataset<Row> cleaned = kafkaDS
                .selectExpr("CAST(value AS STRING) AS raw_json")
                .select(functions.from_json(functions.col("raw_json"), schema).alias("json"))
                .select("json.*")
                .withColumn("channel", functions.lower(functions.trim(functions.col("channel"))))
                // Băm (Hash) tên người dùng để tuân thủ RGPD
                .withColumn("user", functions.sha2(functions.lower(functions.trim(functions.col("user"))), 256))
                .withColumn("message", functions.trim(functions.col("message")))
                .withColumn(
                        "lang",
                        functions.when(
                                functions.col("lang").isNull().or(functions.length(functions.trim(functions.col("lang"))).equalTo(0)),
                                functions.lit("unknown")
                        ).otherwise(functions.lower(functions.trim(functions.col("lang"))))
                )
                .withColumn("event_time", functions.to_timestamp(functions.col("date")))
                .withColumn("ingest_time", functions.current_timestamp())
                .filter(functions.col("channel").isNotNull().and(functions.length(functions.col("channel")).gt(0)))
                .filter(functions.col("user").isNotNull().and(functions.length(functions.col("user")).gt(0)))
                .filter(functions.col("message").isNotNull().and(functions.length(functions.col("message")).gt(0)))
                .filter(functions.col("event_time").isNotNull())
                .select("channel", "user", "message", "lang", "event_time", "ingest_time");

        // 3️⃣ KPI: number of total messages per channel in 1-minute windows
        Dataset<Row> aggregated = cleaned
                .withWatermark("event_time", "1 minute")
                .groupBy(
                        functions.window(functions.col("event_time"), "1 minute"),
                        functions.col("channel")
                )
                .agg(functions.count(functions.lit(1)).alias("message_count"));

        // 4️⃣ Publish aggregates to Kafka topic: aggregated-messages
        Dataset<Row> kafkaOutput = aggregated.selectExpr(
                "CAST(concat(channel, '|', CAST(window.start AS STRING)) AS STRING) AS key",
                "to_json(named_struct('window_start', CAST(window.start AS STRING), 'window_end', CAST(window.end AS STRING), 'channel', channel, 'message_count', message_count)) AS value"
        );

        StreamingQuery messagesWindowQuery = kafkaOutput.writeStream()
                .outputMode("update")
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-cesi:29092")
                .option("topic", "aggregated-messages")
                .option("checkpointLocation", "/tmp/checkpoints/twitch_messages_analytics")
                .start();

        // 5️⃣ KPI session: number of unique users per channel (session scope)
        Dataset<Row> uniqueUsersSession = cleaned
                .withWatermark("event_time", "365 days")
                .dropDuplicates("channel", "user")
                .groupBy(functions.col("channel"))
                .agg(functions.count(functions.lit(1)).alias("unique_users_session"));

        Dataset<Row> uniqueUsersKafkaOutput = uniqueUsersSession.selectExpr(
                "CAST(channel AS STRING) AS key",
                "to_json(named_struct('channel', channel, 'unique_users_session', unique_users_session)) AS value"
        );

        StreamingQuery uniqueUsersSessionQuery = uniqueUsersKafkaOutput
                .writeStream()
                .outputMode("update")
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-cesi:29092")
                .option("topic", "kpi-unique-users-session")
                .option("checkpointLocation", "/tmp/checkpoints/kpi_unique_users_session")
                .start();

        spark.streams().awaitAnyTermination();
    }
}