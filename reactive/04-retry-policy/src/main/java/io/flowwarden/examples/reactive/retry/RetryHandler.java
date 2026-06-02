/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.reactive.retry;

import io.flowwarden.examples.common.model.Order;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.annotation.RetryPolicy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * Reactive twin of {@code imperative/04-retry-policy}.
 *
 * <p>{@code @RetryPolicy} works the same way in reactive mode. The
 * difference is how the failure is signalled to the framework: the
 * handler returns {@code Mono.error(new TransientGatewayException(...))}
 * instead of throwing. FlowWarden subscribes to that {@code Mono}, sees
 * the error signal, and applies the same retry / backoff logic as for
 * a synchronous throw.</p>
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
    Mono<Void> onInsert(Order order) {
        long n = attemptsPerOrder
                .computeIfAbsent(order.getId(), k -> new AtomicLong())
                .incrementAndGet();
        maxAttemptsObserved.accumulateAndGet(n, Math::max);

        String shortId = order.getId().substring(0, 8);
        if (order.getTotal() > 1500) {
            log.warn("[retry] order {} attempt #{} → Mono.error (total={})",
                    shortId, n, order.getTotal());
            return Mono.error(new TransientGatewayException(
                    "Payment gateway timeout for order " + order.getId()));
        }
        return Mono.fromRunnable(() -> {
            successes.incrementAndGet();
            log.info("[retry] order {} attempt #{} → ✓ ({})", shortId, n, order);
        });
    }

    public long getSuccesses()             { return successes.get(); }
    public long getMaxAttemptsForAnyOrder() { return maxAttemptsObserved.get(); }
    public long getTotalAttempts() {
        return attemptsPerOrder.values().stream().mapToLong(AtomicLong::get).sum();
    }
}
