package io.flowwarden.examples.reactive.reconcile;

import io.flowwarden.stream.FlowWardenMetrics;
import io.flowwarden.stream.spi.BackpressureAction;
import io.flowwarden.stream.spi.ChangeEventMetadata;
import io.flowwarden.stream.spi.StopReason;
import io.flowwarden.stream.spi.StreamConfiguration;
import io.flowwarden.stream.spi.StreamMetricsProvider;
import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * A minimal {@link StreamMetricsProvider} logging the signals of one
 * history-loss cycle: {@code onResumeHistoryLost} (cascade level 3 reached),
 * then {@code onHistoryLostReconciled} once the hook returned — or
 * {@code onHistoryLostReconciliationFailed} if it threw, in which case the
 * stream does not start.
 */
@Component
public class ReconcileObserver implements StreamMetricsProvider {

    private static final Logger log = LoggerFactory.getLogger(ReconcileObserver.class);

    private final AtomicLong historyLosts = new AtomicLong();
    private final List<Integer> reconciledAttempts = new CopyOnWriteArrayList<>();
    private final AtomicLong reconciliationFailures = new AtomicLong();

    @PostConstruct
    void register() {
        FlowWardenMetrics.setProvider(this);
    }

    @Override
    public void onResumeHistoryLost(String streamName) {
        historyLosts.incrementAndGet();
        log.warn("[reconcile] stream '{}' reached cascade level 3 — both saved positions expired", streamName);
    }

    @Override
    public void onHistoryLostReconciled(String streamName, int attempt, Duration elapsed) {
        reconciledAttempts.add(attempt);
        log.info("[reconcile] stream '{}' reconciled in {} ms (attempt {}) — resuming", streamName,
                elapsed.toMillis(), attempt);
    }

    @Override
    public void onHistoryLostReconciliationFailed(String streamName, int attempt, Throwable cause) {
        reconciliationFailures.incrementAndGet();
        log.error("[reconcile] stream '{}' reconciliation FAILED (attempt {}) — the stream stays down: {}",
                streamName, attempt, cause.toString());
    }

    @Override
    public void onStreamStopped(String streamName, StopReason reason, Throwable cause) {
        log.info("[reconcile] stream '{}' stopped ({})", streamName, reason);
    }

    public long getHistoryLosts()               { return historyLosts.get(); }
    public List<Integer> getReconciledAttempts() { return List.copyOf(reconciledAttempts); }
    public long getReconciliationFailures()     { return reconciliationFailures.get(); }

    // --- callbacks this sample does not care about ---
    @Override public void onStreamStarted(String streamName, StreamConfiguration config) {}
    @Override public void onEventReceived(String streamName, ChangeEventMetadata metadata) {}
    @Override public void onEventProcessed(String streamName, long durationNanos, boolean success) {}
    @Override public void onEventError(String streamName, Throwable error, boolean willRetry,
                                       int attemptNumber, ChangeEventMetadata metadata) {}
    @Override public void onCheckpoint(String streamName, String resumeToken) {}
    @Override public void onBufferStatus(String streamName, int currentSize, int maxSize) {}
    @Override public void onBackpressure(String streamName, BackpressureAction action) {}
    @Override public void onEventSentToDlq(String streamName) {}
    @Override public void onOplogStats(double logLengthHours, String status) {}
}
