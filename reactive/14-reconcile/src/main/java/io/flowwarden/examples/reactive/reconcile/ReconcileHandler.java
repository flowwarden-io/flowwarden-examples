package io.flowwarden.examples.reactive.reconcile;

import io.flowwarden.examples.common.model.Order;
import io.flowwarden.stream.HistoryLostContext;
import io.flowwarden.stream.OnHistoryLost;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.annotation.Reconcile;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * Maintains {@link OrderSearchIndex} from the stream and declares how to
 * repair it when the history is lost.
 *
 * <p>{@code RESUME_FROM_NOW} alone would silently skip the lost window —
 * acceptable for a notification, not for an index that must be complete.
 * The {@code @Reconcile} method makes the skip <em>correct</em>: it brings
 * the index in line with the collection before a single new event is
 * delivered.</p>
 */
@ChangeStream(name = ReconcileHandler.STREAM, collection = OrderSearchIndex.COLLECTION, documentType = Order.class)
@Checkpoint(onHistoryLost = OnHistoryLost.RESUME_FROM_NOW)
public class ReconcileHandler {

    public static final String STREAM = "reconcile-handler";

    private static final Logger log = LoggerFactory.getLogger(ReconcileHandler.class);

    private final OrderSearchIndex index;

    public ReconcileHandler(OrderSearchIndex index) {
        this.index = index;
    }

    @OnInsert
    Mono<Void> onInsert(Order order) {
        return Mono.fromRunnable(() -> index.index(order, "stream"));
    }

    /**
     * Runs at resume cascade level 3, before the cursor opens and before any
     * checkpoint write. Everything after {@code ctx.lostWindowEnd()} is then
     * delivered by the stream; everything before it has to be re-synced
     * here. The returned {@code Mono} is awaited on the starting thread.
     */
    @Reconcile
    Mono<Void> rebuild(HistoryLostContext ctx) {
        Instant since = ctx.lostWindowStart().orElse(Instant.EPOCH);
        log.warn("[reconcile] history lost on '{}' ({}, attempt {}) — re-syncing the index for orders "
                        + "created since {} (lost window end: {})",
                ctx.streamName(), ctx.strategy(), ctx.attempt(), since, ctx.lostWindowEnd());
        return index.resyncCreatedSince(since)
                .doOnNext(n -> log.warn("[reconcile] re-synced {} order(s) from the collection — "
                        + "the stream now resumes after {}", n, ctx.lostWindowEnd()))
                .then();
    }
}
