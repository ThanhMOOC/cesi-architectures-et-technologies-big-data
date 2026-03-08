package com.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;

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

        // 2️⃣ ĐỌC DỮ LIỆU TỪ TOPIC TWITCH THẬT
        Dataset<Row> kafkaDS = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-cesi:29092")
                .option("subscribe", "twitch-messages") // Đổi sang nghe twitch-messages
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load();

        // 3️⃣ Schema chuẩn của Twitch Chat
        StructType schema = new StructType()
                .add("channel", "string")
                .add("user", "string")
                .add("message", "string")
                .add("lang", "string")
                .add("date", "string");

        Dataset<Row> lines = kafkaDS
                .selectExpr("CAST(value AS STRING) AS raw_json")
                .select(functions.from_json(functions.col("raw_json"), schema).alias("json"))
                .select("json.*")
                .withColumn("channel", functions.lower(functions.trim(functions.col("channel"))))
                .withColumn("message", functions.lower(functions.trim(functions.col("message"))))
                .withColumn("event_time", functions.to_timestamp(functions.col("date")))
                // Lọc bỏ các dòng lỗi null
                .filter(functions.col("channel").isNotNull().and(functions.length(functions.col("channel")).gt(0)))
                .filter(functions.col("message").isNotNull().and(functions.length(functions.col("message")).gt(0)))
                .filter(functions.col("event_time").isNotNull());

        // 4️⃣ Đọc danh sách từ cấm
        Dataset<Row> offensiveWords = spark
                .read()
                .text("/opt/spark-data/offensive_words.txt")
                .select(functions.lower(functions.trim(functions.col("value"))).alias("word"));

        // 5️⃣ Tìm các tin nhắn Twitch chứa từ cấm
        Dataset<Row> offensiveMessages = lines
                .crossJoin(offensiveWords)
                // Ép message về chữ thường để so sánh không phân biệt hoa/thường
                .filter(functions.col("message").contains(functions.col("word")))
                .select(
                        functions.col("channel"),
                        functions.col("user"),
                        functions.col("message"),
                        functions.col("event_time")
                )
                // Đảm bảo 1 tin nhắn chỉ bị đếm 1 lần dù chứa nhiều từ cấm
                .dropDuplicates("channel", "user", "message", "event_time");

        // 6️⃣ KPI: Đếm số lượng tin nhắn vi phạm theo khung thời gian 1 phút cho TỪNG KÊNH
        Dataset<Row> dataset = offensiveMessages
                .withWatermark("event_time", "1 minute")
                .groupBy(
                        functions.window(functions.col("event_time"), "1 minute"),
                        functions.col("channel")
                )
                .agg(functions.count(functions.lit(1)).alias("offensive_count"));

        // 7️⃣ Đẩy kết quả KPI ra Kafka topic mới
        dataset
                .selectExpr(
                        "CAST(concat(channel, '|', CAST(window.start AS STRING)) AS STRING) AS key",
                        "to_json(named_struct('window_start', CAST(window.start AS STRING), 'window_end', CAST(window.end AS STRING), 'channel', channel, 'offensive_count', offensive_count)) AS value"
                )
                .writeStream()
                .outputMode("update")
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-cesi:29092")
                .option("topic", "aggregated-offensive-messages") // Đẩy ra topic mới
                .option("failOnDataLoss", "false")
                // Checkpoint tách biệt để không đụng độ các job khác
                .option("checkpointLocation", "/tmp/checkpoints/twitch_offensive_detector") 
                .start()
                .awaitTermination();
    }
}