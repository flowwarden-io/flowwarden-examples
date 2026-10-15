package io.flowwarden.examples.reactive.restart;

import io.flowwarden.stream.FlowWardenMetrics;
import io.flowwarden.stream.OperationType;
import io.flowwarden.stream.spi.BackpressureAction;
import io.flowwarden.stream.spi.ChangeEventMetadata;
import io.flowwarden.stream.spi.StopReason;
import io.flowwarden.stream.spi.StreamConfiguration;
import io.flowwarden.stream.spi.StreamMetricsProvider;
import jakarta.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Minimal {@link StreamMetricsProvider} that logs the three signals the
 * restart loop emits, in the order they happen:
 *
 * <ol>
 *   <li>{@code onStreamInvalidated} — the drop reached the cursor;</li>
 *   <li>{@code onStreamStopped(CRASHED)} — the stream is dead, the loop owns it;</li>
 *   <li>{@code onStreamRestarted(attempt)} — resubscribed, with the attempt that succeeded.</li>
 * </ol>
 *
 * <p>A second {@code CRASHED} for the same death would mean the loop gave up
 * (history lost under {@code FAIL}, or {@code maxAttempts} exhausted).</p>
 *
 * <p>Providers register themselves with {@code FlowWardenMetrics.setProvider}
 * — there is one provider per JVM, so a real application would use the
 * FlowWarden Reporter rather than a hand-rolled class like this one.</p>
 */
@Component
public class RestartObserver implements StreamMetricsProvider {

    private static final Logger log = LoggerFactory.getLogger(RestartObserver.class);

    private final AtomicLong invalidations = new AtomicLong();
    private final AtomicLong crashes = new AtomicLong();
    private final List<Integer> restartAttempts = new CopyOnWriteArrayList<>();

    @PostConstruct
    void register() {
        FlowWardenMetrics.setProvider(this);
    }

    @Override
    public void onStreamInvalidated(String streamName, OperationType cause) {
        invalidations.incrementAndGet();
        log.warn("[restart] stream '{}' invalidated (cause: {}) — the cursor is gone", streamName, cause);
    }

    @Override
    public void onStreamStopped(String streamName, StopReason reason, Throwable cause) {
        if (reason == StopReason.CRASHED) {
            crashes.incrementAndGet();
            log.warn("[restart] stream '{}' CRASHED — handed to the restart loop", streamName);
        } else {
            log.info("[restart] stream '{}' stopped ({})", streamName, reason);
        }
    }

    @Override
    public void onStreamRestarted(String streamName, int attempt, Throwable cause) {
        restartAttempts.add(attempt);
        log.info("[restart] stream '{}' resubscribed on attempt #{}", streamName, attempt);
    }

    public long getInvalidations()        { return invalidations.get(); }
    public long getCrashes()              { return crashes.get(); }
    public List<Integer> getRestartAttempts() { return List.copyOf(restartAttempts); }

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
