package io.flowwarden.examples.reactive.restart;

import io.flowwarden.examples.common.model.Order;
import io.flowwarden.stream.OnHistoryLost;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.annotation.RestartPolicy;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * A plain insert handler whose only job is to keep counting across cursor
 * deaths. See the imperative twin for why both annotations are needed:
 * {@code @RestartPolicy} sets the resubscription cadence, and
 * {@code @Checkpoint(onHistoryLost = RESUME_FROM_NOW)} makes a dropped
 * collection restartable instead of terminal.
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
    Mono<Void> onInsert(Order order) {
        return Mono.fromRunnable(() -> {
            long n = processed.incrementAndGet();
            log.info("[restart] #{} ✓ order {} (total={})", n, order.getId().substring(0, 8), order.getTotal());
        });
    }

    public long getProcessed() {
        return processed.get();
    }
}
