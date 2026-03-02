package com.example.kafka;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class KafkaUtils {

    private static final Logger log = LoggerFactory.getLogger(KafkaUtils.class);
    private static BlockingQueue<Pair<String, Map<String, String>>> queue = new LinkedBlockingQueue<>(10_000);

    /**
     * Init kafka producer
     */
    public static Producer<String, Map<String, String>> initKafka(String topic) {
        /* ****** Kafka part ****** */

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        Producer<String, Map<String, String>> producer = new KafkaProducer<>(props);

        Thread kafkaThread = new Thread(() -> {
            try {
                while (true) {
                    Pair<String, Map<String, String>> message = queue.take();
                    log.info("Message récupéré depuis la queue");

                    ProducerRecord<String, Map<String, String>> record = new ProducerRecord<>(topic, message.getLeft(), message.getRight());

                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            log.error("Erreur envoi: ",exception);
                        } else {
                            log.info(
                                    "✅ Kafka ACK | topic={} partition={} offset={}",
                                    metadata.topic(),
                                    metadata.partition(),
                                    metadata.offset()
                            );
                        }
                    });
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        kafkaThread.setDaemon(true);
        kafkaThread.start();

        return producer;
    }

    public static void offer(String key, Map<String, String> message) {
        queue.offer(Pair.of(key, message));
    }

}
