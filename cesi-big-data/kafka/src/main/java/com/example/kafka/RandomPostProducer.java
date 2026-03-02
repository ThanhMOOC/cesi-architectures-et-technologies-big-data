package com.example.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class RandomPostProducer {

    private static final Logger log = LoggerFactory.getLogger(RandomPostProducer.class);
    private static final String topic = "offensive-posts";

    public static void main(String[] args) throws InterruptedException {

        log.info("Starting random post producer");

        /* init kafka producer */

        KafkaUtils.initKafka(topic);

        /* send random message based on a list of real posts */

        try (InputStream is = RandomPostProducer.class
                .getClassLoader()
                .getResourceAsStream("posts.json")) {

            ObjectMapper mapper = new ObjectMapper();
            List<Map<String, String>> posts = mapper.readValue(is, new TypeReference<>() {});

            do {

                /* get a random message */
                int n = ThreadLocalRandom.current().nextInt(0, posts.size());
                int wait = ThreadLocalRandom.current().nextInt(1, 3);

                System.out.println("Envoi d'un message vers kafka ...");
                KafkaUtils.offer(posts.get(n).get("id"), posts.get(n));

                Thread.sleep(wait * 100);

            } while (true);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }

}
