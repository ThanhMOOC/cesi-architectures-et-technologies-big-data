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
import java.util.Locale;

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

        // 1️⃣ Khởi tạo TwitchClient với credentials
        TwitchClient twitchClient = TwitchClientBuilder.builder()
                .withClientId(CLIENT_ID)
                .withClientSecret(CLIENT_SECRET)
                .withEnableHelix(true)
                .withEnableChat(true)
                .build();

        // 2️⃣ Lấy TOP_N_STREAMS stream đang live nhiều viewer nhất
        List<Stream> topStreams = twitchClient.getHelix()
                .getStreams(null, null, null, TOP_N_STREAMS, null, null, null, null)
                .execute()
                .getStreams();

        // 3️⃣ Lưu thông tin stream và join vào chat từng kênh
        for (Stream stream : topStreams) {
            streams.put(stream.getUserName().toLowerCase(Locale.ROOT), stream);
            twitchClient.getChat().joinChannel(stream.getUserName());
            log.info("Joined channel: {}", stream.getUserName());
        }

        // 4️⃣ Lắng nghe tin nhắn chat → gửi vào Kafka
        twitchClient.getEventManager().onEvent(ChannelMessageEvent.class, event -> {
            sendMessage(event);
        });

    }



    /**
     * Send e message to kafka
     * @param message
     */
    public static void sendMessage(ChannelMessageEvent message) {

        String channelName = message.getChannel().getName();
        Stream stream = streams.get(channelName.toLowerCase(Locale.ROOT));
        String language = stream != null && stream.getLanguage() != null ? stream.getLanguage() : "unknown";
        String userName = message.getUser() != null ? message.getUser().getName() : "unknown";

        Map<String, String> formattedMessage = Map.of(
            "channel", channelName,
            "user", userName,
                "message", message.getMessage(),
            "lang", language,
                "date", ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT)
        );

        KafkaUtils.offer(message.getEventId(), formattedMessage);
    }

}
