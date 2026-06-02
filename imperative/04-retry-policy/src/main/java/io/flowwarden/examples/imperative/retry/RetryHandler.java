/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.imperative.retry;

import io.flowwarden.examples.common.model.Order;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.annotation.RetryPolicy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Demonstrates {@link RetryPolicy} with exponential backoff and jitter.
 *
 * <p>Roughly one insert in four (those with {@code total > 1500}) raises
 * a {@link TransientGatewayException}, which {@code @RetryPolicy}
 * captures and retries up to {@code maxAttempts} times. The same throw
 * happens every time in this sample (the predicate is deterministic on
 * the document), so the order eventually exhausts the attempts and is
 * given up on — FlowWarden logs an error and moves on to the next
 * event. Hook in {@code @DeadLetterQueue} (see {@code 05-dlq}) or
 * {@code @OnError} (see {@code 03-error-handling}) to take control of
 * what happens after all retries fail.</p>
 *
 * <p>The backoff schedule with these settings:</p>
 * <pre>
 *   attempt 1 → throw
 *   ~500 ms backoff (+/- 20% jitter) — attempt 2 → throw
 *   ~1 s backoff   (+/- 20% jitter) — attempt 3 → throw → give up
 * </pre>
 *
 * <p>Note: the default {@code noRetryOn} list includes
 * {@code IllegalArgumentException} (and a few other "programmer-error"
 * types). Throwing one of those would fail immediately without retry
 * — the validation pattern from {@code 03-error-handling} is unaffected
 * by {@code @RetryPolicy}.</p>
 */
@ChangeStream(collection = "orders-retry", documentType = Order.class)
@RetryPolicy(
        maxAttempts = 3,
        initialDelay = "500ms",
        maxDelay = "5s",
        multiplier = 2.0,
        retryOn = TransientGatewayException.class,
        jitter = true
)
public class RetryHandler {

    private static final Logger log = LoggerFactory.getLogger(RetryHandler.class);

    private final Map<String, AtomicLong> attemptsPerOrder = new ConcurrentHashMap<>();
    private final AtomicLong successes = new AtomicLong();
    private final AtomicLong maxAttemptsObserved = new AtomicLong();

    @OnInsert
    void onInsert(Order order) {
        long n = attemptsPerOrder
                .computeIfAbsent(order.getId(), k -> new AtomicLong())
                .incrementAndGet();
        maxAttemptsObserved.accumulateAndGet(n, Math::max);

        String shortId = order.getId().substring(0, 8);
        if (order.getTotal() > 1500) {
            log.warn("[retry] order {} attempt #{} → throw (total={})",
                    shortId, n, order.getTotal());
            throw new TransientGatewayException(
                    "Payment gateway timeout for order " + order.getId());
        }
        successes.incrementAndGet();
        log.info("[retry] order {} attempt #{} → ✓ ({})", shortId, n, order);
    }

    public long getSuccesses()             { return successes.get(); }
    public long getMaxAttemptsForAnyOrder() { return maxAttemptsObserved.get(); }
    public long getTotalAttempts() {
        return attemptsPerOrder.values().stream().mapToLong(AtomicLong::get).sum();
    }
}
