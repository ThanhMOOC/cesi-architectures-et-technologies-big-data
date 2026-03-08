package com.example.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class RandomPostProducer {

    private static final Logger log = LoggerFactory.getLogger(RandomPostProducer.class);
    private static final String TOPIC = "offensive-posts";

    public static void main(String[] args) throws InterruptedException {

        // 1️⃣ Khởi tạo Kafka Producer dùng chung (KafkaUtils)
        KafkaUtils.initKafka(TOPIC);
        Random random = new Random();

        // 2️⃣ Dữ liệu giả lập
        List<String> platforms = Arrays.asList("twitter", "facebook", "reddit", "twitch", "youtube");
        
        // Trộn lẫn các tin nhắn bình thường và tin nhắn chứa từ khóa độc hại (từ file offensive_words.txt)
        List<String> sampleMessages = Arrays.asList(
                "Hello world, what a beautiful day!",
                "This game is complete bullshit and the devs are idiots.", // Chứa từ cấm
                "Can someone help me with this level?",
                "You are such a loser and a coward.", // Chứa từ cấm
                "I love playing this character so much.",
                "What a complete waste of time, this is garbage.", // Chứa từ cấm
                "GG WP everyone, that was a close match!",
                "Shut up you dumbass before I report you.", // Chứa từ cấm
                "Does anyone know when the next patch drops?",
                "This is the worst stream ever, absolutely pathetic." // Chứa từ cấm
        );

        log.info("🚀 Démarrage du RandomPostProducer (Générateur de posts aléatoires)...");

        // 3️⃣ Vòng lặp bắn tin nhắn liên tục
        while (true) {
            String id = UUID.randomUUID().toString();
            String platform = platforms.get(random.nextInt(platforms.size()));
            String message = sampleMessages.get(random.nextInt(sampleMessages.size()));

            // Chuẩn bị payload theo đúng schema yêu cầu
            Map<String, String> payload = Map.of(
                    "id", id,
                    "platform", platform,
                    "message", message
            );

            // Gửi vào Kafka (dùng id làm Kafka key)
            KafkaUtils.offer(id, payload);
            log.info("📨 Gửi tin nhắn | id={} platform={} | content: {}", id, platform, message);

            // Tạm dừng ngẫu nhiên từ 100ms đến 300ms theo đúng yêu cầu
            int sleepTime = 100 + random.nextInt(201);
            Thread.sleep(sleepTime);
        }
    }
}