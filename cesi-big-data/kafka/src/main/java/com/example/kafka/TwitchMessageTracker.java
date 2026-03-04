package com.example.kafka;


import com.github.twitch4j.TwitchClient;
import com.github.twitch4j.TwitchClientBuilder;
import com.github.twitch4j.chat.events.channel.ChannelMessageEvent;
import com.github.twitch4j.helix.domain.Stream;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TwitchMessageTracker {

    private static final String CLIENT_ID = PropertiesUtils.get("twitch.client.id");
    private static final String CLIENT_SECRET = PropertiesUtils.get("twitch.client.secret");
    private static final int TOP_N_STREAMS = 15;
    private static final Logger log = LoggerFactory.getLogger(TwitchMessageTracker.class);
    private static Map<String, Stream> streams = new HashMap<>();
    private static final String topic = "twitch-messages";
    private static Producer<String, Map<String, String>> producer;


    public static void main(String[] args) {

        /* ****** init kafka producer ****** */

        KafkaUtils.initKafka(topic);

        /* ****** Twitch part ****** */

        // Doc of API can be found here :
        // https://twitch4j.github.io/rest-helix/


    }



    /**
     * Send e message to kafka
     * @param message
     */
    public static void sendMessage(ChannelMessageEvent message) {

        Map<String, String> formattedMessage = Map.of(
                "channel", message.getChannel().getName(),
                "user", message.getUser().getName(),
                "message", message.getMessage(),
                "lang", streams.get(message.getChannel().getName()).getLanguage(),
                "date", ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT)
        );

        KafkaUtils.offer(message.getEventId(), formattedMessage);
    }

}
