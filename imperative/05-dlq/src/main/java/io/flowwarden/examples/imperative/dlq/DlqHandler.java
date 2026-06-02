/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.imperative.dlq;

import io.flowwarden.examples.common.model.Order;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.DeadLetterQueue;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.annotation.RetryPolicy;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Demonstrates {@link DeadLetterQueue} combined with {@link RetryPolicy}.
 *
 * <p>Orders with {@code total > 1500} are deterministically rejected by
 * the handler (simulating a stuck downstream service). With
 * {@code maxAttempts = 2}, every such order is tried twice and then
 * routed by FlowWarden to the DLQ collection {@code orders-dlq-failed}
 * — instead of being silently lost as it would be without
 * {@code @DeadLetterQueue}.</p>
 *
 * <p>The DLQ entry includes the original document and (by default) the
 * full stack trace, so a human can later inspect why a given order
 * could not be processed. A TTL index on {@code ttlDays} guarantees
 * that the DLQ doesn't grow forever.</p>
 *
 * <p>The companion {@code DlqController} (REST endpoint
 * {@code GET /dlq}) lets you read the collection from a browser
 * without having to {@code mongosh} into the database.</p>
 *
 * <p>{@code @DeadLetterQueue} also works <em>without</em>
 * {@code @RetryPolicy}: the event then lands in the DLQ on the very
 * first failure.</p>
 *
 * <p><b>Known warts in {@code flowwarden-stream-core:1.0.0-rc.1}:</b></p>
 * <ul>
 *   <li>{@code collection} — <b>ignored</b>. Every failed event lands
 *       in the hardcoded {@code _fw_dlq} collection regardless. Pin to
 *       {@code _fw_dlq} when querying the DLQ yourself for now (see
 *       {@code DlqController}).</li>
 *   <li>{@code ttlDays} — <b>partial</b>. The {@code expiresAt} field
 *       is computed and written on each DLQ document, but no TTL index
 *       is created on the collection, so entries accumulate
 *       indefinitely. Create the TTL index by hand if you need
 *       automatic cleanup.</li>
 *   <li>{@code enabled}, {@code includeOriginalDocument},
 *       {@code includeStackTrace} — <b>honoured</b>. The attributes
 *       declared below take effect.</li>
 * </ul>
 *
 * <p>The {@code collection} and {@code ttlDays} attributes are kept on
 * the annotation as forward-compatible declaration of intent.</p>
 */
@ChangeStream(collection = "orders-dlq", documentType = Order.class)
@RetryPolicy(
        maxAttempts = 2,
        initialDelay = "300ms",
        maxDelay = "2s",
        retryOn = PaymentRejectedException.class
)
@DeadLetterQueue(
        collection = "orders-dlq-failed",
        ttlDays = 7,
        includeOriginalDocument = true,
        includeStackTrace = true
)
public class DlqHandler {

    private static final Logger log = LoggerFactory.getLogger(DlqHandler.class);

    private final AtomicLong attempts = new AtomicLong();
    private final AtomicLong successes = new AtomicLong();

    @OnInsert
    void onInsert(Order order) {
        long n = attempts.incrementAndGet();
        if (order.getTotal() > 1500) {
            log.warn("[dlq] attempt #{} → reject big order {} (total={})",
                    n, order.getId().substring(0, 8), order.getTotal());
            throw new PaymentRejectedException(
                    "Downstream payment processor refused order " + order.getId());
        }
        successes.incrementAndGet();
        log.info("[dlq] ✓ {}", order);
    }

    public long getSuccesses() { return successes.get(); }
}
