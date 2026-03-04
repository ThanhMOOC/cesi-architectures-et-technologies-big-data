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

        KafkaUtils.initKafka(TOPIC);

        try (TwitchClient twitchClient = TwitchClientBuilder.builder()
                .withEnableHelix(true)
                .withClientId(CLIENT_ID)
                .withClientSecret(CLIENT_SECRET)
                .build()) {

            // 1️⃣ Récupération initiale des TOP_N_STREAMS streams et stockage de leurs IDs
            List<Stream> topStreams = twitchClient.getHelix()
                    .getStreams(null, null, null, TOP_N_STREAMS, null, null, null, null)
                    .execute()
                    .getStreams();

            List<String> trackedUserIds = topStreams.stream()
                    .map(Stream::getUserId)
                    .toList();

            log.info("Tracking {} streams : {}", trackedUserIds.size(),
                    topStreams.stream().map(Stream::getUserLogin).toList());

            // 2️⃣ Polling toutes les minutes
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

            scheduler.scheduleAtFixedRate(() -> {
                try {
                    List<Stream> currentStreams = twitchClient.getHelix()
                            .getStreams(null, null, null, TOP_N_STREAMS, null, null, trackedUserIds, null)
                            .execute()
                            .getStreams();

                    String timestamp = ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT);

                    for (Stream stream : currentStreams) {
                        Map<String, String> payload = Map.of(
                                "userId",      stream.getUserId(),
                                "channel",     stream.getUserLogin(),
                                "viewerCount", String.valueOf(stream.getViewerCount()),
                                "language",    stream.getLanguage(),
                                "date",        timestamp
                        );
                        KafkaUtils.offer(stream.getUserId(), payload);
                        log.info("viewers channel={} count={}", stream.getUserLogin(), stream.getViewerCount());
                    }

                } catch (Exception e) {
                    log.error("Erreur lors du polling Helix", e);
                }
            }, 0, 1, TimeUnit.MINUTES);

            // Maintien du thread principal en vie
            Thread.currentThread().join();
        }
    }
}
