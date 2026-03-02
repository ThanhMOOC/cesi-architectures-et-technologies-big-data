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

public class TwitchTracker {

    private static final String CLIENT_ID = "n888166co10py46y6nlu79exmm83vf";
    private static final String CLIENT_SECRET = "2lw30iegsmyli7rn8mfwkj2d1i5owy";
    private static final int TOP_N_STREAMS = 15;
    private static final Logger log = LoggerFactory.getLogger(TwitchTracker.class);
    private static Map<String, Stream> streams = new HashMap<>();
    private static final String topic = "twitch-messages";
    private static Producer<String, Map<String, String>> producer;


    public static void main(String[] args) {

        /* ****** init kafka producer ****** */

        KafkaUtils.initKafka(topic);

        /* ****** Twitch part ****** */

        try (TwitchClient twitchClient = TwitchClientBuilder.builder()
                .withEnableHelix(true)
                .withEnableChat(true)
                .withClientId(CLIENT_ID)
                .withClientSecret(CLIENT_SECRET)
                .build()) {

            // 1️⃣ Récupération des N plus gros streams
            List<Stream> topStreams = twitchClient.getHelix()
                    .getStreams(null, null, null, TOP_N_STREAMS, null, null, null, null)
                    .execute()
                    .getStreams();

            System.out.println(""+topStreams.stream().map(s -> s.getUserLogin()).toList());

            System.out.println("🎥 Top " + TOP_N_STREAMS + " streams Twitch :");

            for (Stream stream : topStreams) {
                String channelName = stream.getUserLogin();

                System.out.printf(
                        "- %s (%d viewers)%n",
                        channelName,
                        stream.getViewerCount()
                );

                streams.put(channelName, stream);

                // 2️⃣ Connexion au chat
                System.out.printf("Joining %s ...", channelName);
                twitchClient.getChat().joinChannel(channelName);
            }

            // 3️⃣ Écoute des messages
            twitchClient.getEventManager().onEvent(ChannelMessageEvent.class, event -> {
                Stream stream = streams.get(event.getChannel().getName());

                stream.getLanguage();
                stream.getType();

//                System.out.printf(
//                        "[%s] %s : %s lang %s type %s%n",
//                        event.getChannel().getName(),
//                        event.getUser().getName(),
//                        event.getMessage(),
//                        stream.getLanguage(),
//                        stream.getType()
//                );
                /* send it to kafka */
                sendMessage(event);
            });

            System.out.println("✅ Connexion aux chats réussie !");

            Thread.currentThread().join();

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        };


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
