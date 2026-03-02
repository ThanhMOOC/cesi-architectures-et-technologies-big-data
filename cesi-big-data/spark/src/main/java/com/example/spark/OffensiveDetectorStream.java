package com.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class OffensiveDetectorStream {

    public static void main(String[] args) throws Exception {

        // 🧩 Fix Hadoop FileSystem login issue inside Docker
        System.setProperty("HADOOP_USER_NAME", "sparkuser");
        System.setProperty("user.name", "sparkuser");

        // 1️⃣ Init SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("OffensiveDetectorStream")
                .master("spark://spark-master:7077")
                .getOrCreate();

        Dataset<Row> kafkaDS = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-cesi:29092")
                /* we'll do this manual assignment to avoid windows/docker permissions issues */
//                .option("assign", "{\"offensive-posts\":[0]}")
                .option("subscribe", "offensive-posts")
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load();

        StructType schema = new StructType()
                .add("platform", "string")
                .add("id", "string")
                .add("message", "string");

        // key et value sont en binaire -> cast en STRING
        Dataset<Row> lines = kafkaDS.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                // Parsing du json
                .select(
                        functions.col("key"),
                        functions.from_json(functions.col("value"), schema).alias("json")
                );

        Dataset<Row> offensiveWords = spark
                .read()
                .text("/opt/spark-data/offensive_words.txt")
                .select(concat(lit(" "), col("value"), lit(" ")).as("word"));

        Dataset<Row> dataset = lines
                .crossJoin(offensiveWords)
                .select(
                        col("json.id"),
                        col("json.platform"),
                        col("json.message"),
                        col("json.message").contains(col("word")).as("contains_offensive"),
                        col("word").as("offensive_word")
                )
                .filter(col("contains_offensive").isNotNull().and(col("contains_offensive").equalTo(true)))
                .groupBy(
                        col("id"),
                        col("platform"),
                        col("message")
                )
                .agg(collect_list(col("offensive_word")).as("offensive_words"));

        dataset
                .selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
                .writeStream()
                .outputMode("update")
//                .format("console")
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-cesi:29092")
                .option("topic", "offensive-posts-output")
                .option("failOnDataLoss", false)
                .option("checkpointLocation", "/tmp/checkpoints/offensive_detector")
                .start()
                .awaitTermination();


    }

}
