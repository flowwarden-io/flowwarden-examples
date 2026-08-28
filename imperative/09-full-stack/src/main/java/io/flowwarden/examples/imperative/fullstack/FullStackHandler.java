/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.imperative.fullstack;

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

/**
 * The showcase handler: every major FlowWarden annotation in one class.
 *
 * <p>What this sample proves, in one bullet: <b>FlowWarden's dual-token
 * model has a real-world payoff</b>. A raw MongoDB change stream gives
 * you a single resume token that only advances when you've successfully
 * processed an event. If 99% of events are dropped by your business
 * filter, your resume token stays stale and your stream cannot recover
 * if the oplog rolls over — even though nothing was lost. FlowWarden
 * decouples "what was <em>seen</em>" (heartbeat-fresh) from "what was
 * <em>processed</em>" (handler-success), so neither metric stalls the
 * other.</p>
 *
 * <p>Where each annotation lives in the pipeline:</p>
 * <pre>
 *   MongoDB oplog
 *        │
 *        ▼  @Pipeline           — server-side match. Events not
 *        │                         matching never leave MongoDB.
 *        │                         Neither token advances for them.
 *        ▼  driver → app
 *        │
 *        ▼  @Filter              — client-side decision. Rejected
 *        │                         events still cross the wire so
 *        │                         lastSeen advances via heartbeat.
 *        │                         lastProcessed does NOT.
 *        ▼  @RetryPolicy         — temporary divergence: an event in
 *        │                         the retry loop is "seen" but not
 *        │                         yet processed.
 *        ▼  @OnInsert / @OnUpdate / @OnReplace
 *        │                       — success → lastProcessed advances.
 *        ▼  @DeadLetterQueue     — retry exhaustion → DLQ write acks
 *                                   the event, lastProcessed advances.
 * </pre>
 *
 * <p>{@code @Checkpoint} persists both tokens to {@code _fw_checkpoints}
 * and drives the resume cascade on restart. {@code saveIntervalSeconds = 2}
 * keeps the processed anchor's age visibly bounded within a few
 * seconds in the metrics endpoint.</p>
 *
 * <p><b>No {@code @OnDelete}:</b> the lib statically refuses
 * {@code @Filter} + {@code @OnDelete} on the same class because delete
 * events carry no {@code fullDocument} for the filter predicate to
 * inspect. The recommended workarounds are a server-side
 * {@code @Pipeline} stage that excludes deletes, or inlining the
 * filter logic in {@code @OnDelete}. We sidestep by setting
 * {@code deletes-per-second: 0} in the generator — deletes are out of
 * scope for this sample.</p>
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

    /** Server-side cut-off — eliminates the smallest orders entirely. */
    static final double PIPELINE_MIN_TOTAL = 50.0;

    /** Client-side business rule — keep only confirmed orders for processing. */
    static final String FILTER_KEPT_STATUS = "CONFIRMED";

    /** Deterministic failure trigger — 1 in N confirmed orders simulates a downstream timeout. */
    static final int FAILURE_HASH_MODULO = 10;

    private final AtomicLong handledInsert = new AtomicLong();
    private final AtomicLong handledUpdate = new AtomicLong();
    private final AtomicLong handledReplace = new AtomicLong();
    private final AtomicLong clientRejectedByFilter = new AtomicLong();
    private final AtomicLong transientFailureAttempts = new AtomicLong();

    /**
     * Server-side filter. The change stream cursor itself runs this
     * aggregation in MongoDB — events not matching never leave the
     * database, so they never count towards <em>either</em> token.
     */
    @Pipeline
    List<AggregationOperation> pipeline() {
        return List.of(
                Aggregation.match(Criteria.where("fullDocument.total").gt(PIPELINE_MIN_TOTAL))
        );
    }

    /**
     * Client-side filter. Rejected events have already crossed the wire,
     * so {@code lastSeenToken} can advance to them via the heartbeat,
     * while {@code lastProcessedToken} stays behind. This is where the
     * dual-token divergence is born.
     *
     * <p>The generator doesn't emit deletes (see class Javadoc), so the
     * predicate always sees a {@code fullDocument}.</p>
     */
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
    void created(Order order) {
        // Deterministic failure to keep @RetryPolicy and @DeadLetterQueue exercised.
        if (Math.abs(order.getId().hashCode()) % FAILURE_HASH_MODULO == 0) {
            transientFailureAttempts.incrementAndGet();
            log.warn("[fullstack] INSERT {} → simulated transient failure",
                    order.getId().substring(0, 8));
            throw new TransientGatewayException("Simulated gateway timeout for " + order.getId());
        }
        handledInsert.incrementAndGet();
        log.info("[fullstack] INSERT  {}", order);
    }

    @OnUpdate
    void updated(ChangeStreamContext<Order> ctx) {
        handledUpdate.incrementAndGet();
        log.info("[fullstack] UPDATE  {}", ctx.summary());
    }

    @OnReplace
    void replaced(Order order) {
        handledReplace.incrementAndGet();
        log.info("[fullstack] REPLACE {}", order);
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
