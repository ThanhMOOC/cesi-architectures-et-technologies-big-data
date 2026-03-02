package com.example.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Pattern;

import static org.apache.spark.sql.functions.*;

/**
 * => /opt/spark-apps dans le conteneur
 * Si le module distribué ne démarre pas => rm -rf /tmp/checkpoints
 * Si il ne lis rien c'est aprce qu'on est pas en mode earliest (pb avec Docker bind mount).
 * Si le ./start.sh indique fichier non trouvé c'est parce que c'est au format windows => sed -i 's/\r$//' start.sh
 */
public class OffensiveDetector {

    public static void main(String[] args) throws Exception {

        // 🧩 Fix Hadoop FileSystem login issue inside Docker
        System.setProperty("HADOOP_USER_NAME", "sparkuser");
        System.setProperty("user.name", "sparkuser");

        // 1️⃣ Init SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("OffensiveDetector")
                .master("spark://spark-master:7077")
                .getOrCreate();

        // Liste des mots a rechercher "/opt/spark-data/offensive_words.txt"


        Dataset<Row> offensive = spark
                .read()
                .text("/opt/spark-data/offensive_words.txt");

        Dataset<Row> dataset = spark
                .read()
                .option("multiline", "true")
                .json("/opt/spark-data/posts.json")
                .select(col("id"), col("platform"), col("message"), explode(split(col("message"), "\\s+")).alias("word"))
                .join(offensive, col("word").equalTo(col("value")))
                .groupBy("id", "platform", "message")
                .agg(collect_list(col("word")).as("offensive_words"), count(col("id")).as("count_of"))
                .sort(desc("count_of"));

        /* here you should complete the code to use your own moderation system */


        dataset.printSchema();
        dataset.show(10, true);


    }

}
