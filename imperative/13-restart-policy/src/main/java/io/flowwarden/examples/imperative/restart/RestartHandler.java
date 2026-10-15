package io.flowwarden.examples.imperative.restart;

import io.flowwarden.examples.common.model.Order;
import io.flowwarden.stream.OnHistoryLost;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.annotation.RestartPolicy;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A plain insert handler whose only job is to keep counting across cursor
 * deaths.
 *
 * <p>Two annotations make the demo work:</p>
 * <ul>
 *   <li>{@code @RestartPolicy} — the cadence of the resubscription attempts
 *       after a runtime death, and (here) a ceiling of 10 attempts. The
 *       defaults (1s doubling to 60s, unlimited, no jitter) are what every
 *       stream gets without the annotation; this sample tightens the delays
 *       so the restart shows up within a second of the drop.</li>
 *   <li>{@code @Checkpoint(onHistoryLost = RESUME_FROM_NOW)} — a dropped
 *       collection is a history loss: the persisted resume token points
 *       into a namespace that no longer exists. With the default
 *       {@code FAIL} strategy the invalidation is <em>terminal</em> and the
 *       restart loop is never entered. {@code RESUME_FROM_NOW} lets
 *       FlowWarden repair the checkpoint to a fresh position and hand the
 *       stream to the loop.</li>
 * </ul>
 */
@ChangeStream(collection = "orders-restart", documentType = Order.class)
@Checkpoint(onHistoryLost = OnHistoryLost.RESUME_FROM_NOW)
@RestartPolicy(
        maxAttempts = 10,
        initialDelay = "500ms",
        maxDelay = "5s",
        multiplier = 2.0,
        jitter = true
)
public class RestartHandler {

    private static final Logger log = LoggerFactory.getLogger(RestartHandler.class);

    private final AtomicLong processed = new AtomicLong();

    @OnInsert
    void onInsert(Order order) {
        long n = processed.incrementAndGet();
        log.info("[restart] #{} ✓ order {} (total={})", n, order.getId().substring(0, 8), order.getTotal());
    }

    public long getProcessed() {
        return processed.get();
    }
}
