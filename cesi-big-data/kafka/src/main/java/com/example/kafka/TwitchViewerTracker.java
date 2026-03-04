package com.example.kafka;

import com.github.twitch4j.TwitchClient;
import com.github.twitch4j.TwitchClientBuilder;
import com.github.twitch4j.helix.domain.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TwitchViewerTracker {

    private static final String CLIENT_ID = PropertiesUtils.get("twitch.client.id");
    private static final String CLIENT_SECRET = PropertiesUtils.get("twitch.client.secret");
    private static final int TOP_N_STREAMS = 15;
    private static final String TOPIC = "twitch-viewers";
    private static final Logger log = LoggerFactory.getLogger(TwitchViewerTracker.class);

    public static void main(String[] args) throws InterruptedException {

        /* ****** init kafka producer ****** */

        KafkaUtils.initKafka(TOPIC);

        /* ****** Twitch part ****** */

        // Doc of API can be found here :
        // https://twitch4j.github.io/rest-helix/

    }
}
