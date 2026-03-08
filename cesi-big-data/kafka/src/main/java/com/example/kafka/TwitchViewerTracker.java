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

        TwitchClient twitchClient = TwitchClientBuilder.builder()
                .withClientId(CLIENT_ID)
                .withClientSecret(CLIENT_SECRET)
                .withEnableHelix(true)
                .build();

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        Runnable fetchAndPublish = () -> {
            try {
                List<Stream> topStreams = twitchClient.getHelix()
                        .getStreams(null, null, null, TOP_N_STREAMS, null, null, null, null)
                        .execute()
                        .getStreams();

                for (Stream stream : topStreams) {
                    Map<String, String> payload = Map.of(
                            "userId", stream.getUserId(),
                            "channel", stream.getUserName(),
                            "viewerCount", String.valueOf(stream.getViewerCount()),
                            "language", stream.getLanguage() != null ? stream.getLanguage() : "unknown",
                            "date", ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT)
                    );

                    KafkaUtils.offer(stream.getUserId(), payload);
                    log.info("Viewer metric sent | channel={} viewers={}", stream.getUserName(), stream.getViewerCount());
                }
            } catch (Exception e) {
                log.error("Error while fetching/publishing viewer metrics", e);
            }
        };

        scheduler.scheduleAtFixedRate(fetchAndPublish, 0, 60, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Stopping TwitchViewerTracker...");
            scheduler.shutdownNow();
        }));

        Thread.currentThread().join();

    }
}
