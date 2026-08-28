/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.reactive.fullstack;

import io.flowwarden.examples.common.model.Order;
import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.FullDocumentMode;
import io.flowwarden.stream.OnHistoryLost;
import io.flowwarden.stream.StartPosition;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.DeadLetterQueue;
import io.flowwarden.stream.annotation.Filter;
import io.flowwarden.stream.annotation.MongoDlqOptions;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.annotation.OnReplace;
import io.flowwarden.stream.annotation.OnUpdate;
import io.flowwarden.stream.annotation.Pipeline;
import io.flowwarden.stream.annotation.RetryPolicy;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import reactor.core.publisher.Mono;

/**
 * Reactive twin of {@code imperative/09-full-stack}. See the imperative
 * twin's Javadoc for the dual-token model and the constraint that
 * keeps {@code @OnDelete} out of this sample.
 *
 * <p>Reactive-specific notes:
 * <ul>
 *   <li>{@code @Pipeline} returns synchronously (it's a startup
 *       declaration, not a per-event hot path).</li>
 *   <li>{@code @Filter} is synchronous {@code boolean} — the lib
 *       doesn't offer a {@code Mono<Boolean>} variant. See sample 08.</li>
 *   <li>Only the typed handlers and the failure signal change shape:
 *       handlers return {@code Mono<Void>}, failures use
 *       {@code Mono.error(...)}.</li>
 * </ul>
 * </p>
 */
@ChangeStream(
        collection = "orders-full",
        documentType = Order.class,
        fullDocument = FullDocumentMode.UPDATE_LOOKUP
)
@RetryPolicy(
        maxAttempts = 3,
        initialDelay = "200ms",
        maxDelay = "2s",
        multiplier = 2.0,
        retryOn = TransientGatewayException.class,
        jitter = true
)
@DeadLetterQueue(
        retentionDays = 14,
        includeOriginalDocument = true,
        includeStackTrace = true
)
@MongoDlqOptions(collection = "orders-full-dlq")
@Checkpoint(
        saveEveryN = 1,
        saveIntervalSeconds = 2,
        startPosition = StartPosition.RESUME,
        onHistoryLost = OnHistoryLost.RESUME_FROM_NOW
)
public class FullStackHandler {

    private static final Logger log = LoggerFactory.getLogger(FullStackHandler.class);

    static final double PIPELINE_MIN_TOTAL = 50.0;
    static final String FILTER_KEPT_STATUS = "CONFIRMED";
    static final int FAILURE_HASH_MODULO = 10;

    private final AtomicLong handledInsert = new AtomicLong();
    private final AtomicLong handledUpdate = new AtomicLong();
    private final AtomicLong handledReplace = new AtomicLong();
    private final AtomicLong clientRejectedByFilter = new AtomicLong();
    private final AtomicLong transientFailureAttempts = new AtomicLong();

    @Pipeline
    List<AggregationOperation> pipeline() {
        return List.of(
                Aggregation.match(Criteria.where("fullDocument.total").gt(PIPELINE_MIN_TOTAL))
        );
    }

    @Filter
    boolean keepConfirmed(ChangeStreamContext<Order> ctx) {
        Optional<Order> doc = ctx.getFullDocument(Order.class);
        boolean keep = doc.map(o -> FILTER_KEPT_STATUS.equals(o.getStatus())).orElse(false);
        if (!keep) {
            clientRejectedByFilter.incrementAndGet();
        }
        return keep;
    }

    @OnInsert
    Mono<Void> created(Order order) {
        if (Math.abs(order.getId().hashCode()) % FAILURE_HASH_MODULO == 0) {
            transientFailureAttempts.incrementAndGet();
            log.warn("[fullstack] INSERT {} → Mono.error TransientGatewayException",
                    order.getId().substring(0, 8));
            return Mono.error(new TransientGatewayException(
                    "Simulated gateway timeout for " + order.getId()));
        }
        return Mono.fromRunnable(() -> {
            handledInsert.incrementAndGet();
            log.info("[fullstack] INSERT  {}", order);
        });
    }

    @OnUpdate
    Mono<Void> updated(ChangeStreamContext<Order> ctx) {
        return Mono.fromRunnable(() -> {
            handledUpdate.incrementAndGet();
            log.info("[fullstack] UPDATE  {}", ctx.summary());
        });
    }

    @OnReplace
    Mono<Void> replaced(Order order) {
        return Mono.fromRunnable(() -> {
            handledReplace.incrementAndGet();
            log.info("[fullstack] REPLACE {}", order);
        });
    }

    public long getHandledInsert()           { return handledInsert.get(); }
    public long getHandledUpdate()           { return handledUpdate.get(); }
    public long getHandledReplace()          { return handledReplace.get(); }
    public long getHandledTotal() {
        return handledInsert.get() + handledUpdate.get() + handledReplace.get();
    }
    public long getClientRejectedByFilter()  { return clientRejectedByFilter.get(); }
    public long getTransientFailureAttempts() { return transientFailureAttempts.get(); }
}
