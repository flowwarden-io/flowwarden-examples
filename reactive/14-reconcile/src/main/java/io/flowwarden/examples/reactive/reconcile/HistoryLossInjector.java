package io.flowwarden.examples.reactive.reconcile;

import io.flowwarden.stream.core.FlowWardenStreamManager;
import io.flowwarden.stream.spi.Checkpoint;
import io.flowwarden.stream.spi.CheckpointStore;
import java.time.Instant;
import java.util.Map;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Produces a history loss on demand, deterministically, against the shared
 * MongoDB of the examples (whose oplog is far too large to roll over in a
 * demo).
 *
 * <p>In production the sequence is: the stream is down longer than the
 * oplog window, MongoDB discards the entries its saved positions point to,
 * and the next start finds both positions expired. Here the downtime is a
 * few seconds and the "expiry" is simulated by overwriting both saved
 * tokens with one that no longer resolves — the timestamps of the real
 * checkpoint are kept, so the lost window handed to {@code @Reconcile} is
 * the actual downtime. The resume cascade cannot tell the difference: this
 * is the same technique the library's own integration tests use.</p>
 */
@Component
public class HistoryLossInjector {

    private static final Logger log = LoggerFactory.getLogger(HistoryLossInjector.class);
    private static final BsonDocument EXPIRED_TOKEN = BsonDocument.parse("{\"_data\": \"DEADBEEF\"}");

    private final FlowWardenStreamManager streamManager;
    private final CheckpointStore checkpointStore;
    private final long downtimeMs;

    public HistoryLossInjector(FlowWardenStreamManager streamManager,
                               CheckpointStore checkpointStore,
                               @Value("${examples.reconcile-demo.downtime-ms:3000}") long downtimeMs) {
        this.streamManager = streamManager;
        this.checkpointStore = checkpointStore;
        this.downtimeMs = downtimeMs;
    }

    @Scheduled(initialDelayString = "${examples.reconcile-demo.initial-delay-ms:15000}",
               fixedDelayString = "${examples.reconcile-demo.interval-ms:30000}")
    void loseHistory() throws InterruptedException {
        log.warn("[reconcile-demo] stopping stream '{}' — simulating a downtime longer than the oplog window",
                ReconcileHandler.STREAM);
        streamManager.stopStream(ReconcileHandler.STREAM);

        // The generator keeps inserting: these orders are the lost window.
        Thread.sleep(downtimeMs);

        Checkpoint current = checkpointStore.findByStreamName(ReconcileHandler.STREAM)
                .orElseGet(() -> {
                    Instant past = Instant.now().minusMillis(downtimeMs);
                    return new Checkpoint(ReconcileHandler.STREAM, null, null, past, null, past, past, Map.of());
                });
        checkpointStore.save(new Checkpoint(current.streamName(), current.instanceId(),
                EXPIRED_TOKEN, current.lastSeenTimestamp(),
                EXPIRED_TOKEN, current.lastProcessedTimestamp(),
                current.lastHeartbeatTimestamp(), current.metadata()));
        log.warn("[reconcile-demo] both saved positions of '{}' replaced by an expired token "
                + "(last activity kept at {}) — the next start reaches cascade level 3",
                ReconcileHandler.STREAM, current.lastHeartbeatTimestamp());

        streamManager.startStream(ReconcileHandler.STREAM);
    }
}
