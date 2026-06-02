/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.reactive.dlq;

import io.flowwarden.examples.common.model.Order;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.DeadLetterQueue;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.annotation.RetryPolicy;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * Reactive twin of {@code imperative/05-dlq}.
 *
 * <p>Failure is signalled via {@code Mono.error(...)}, but the
 * downstream behaviour is identical: {@code @RetryPolicy} retries
 * twice, then {@code @DeadLetterQueue} archives the offending event.
 * See the imperative twin for the per-attribute audit of which
 * {@code @DeadLetterQueue} fields are honoured / ignored / partial in
 * {@code stream-core:1.0.0-rc.1}.</p>
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
    Mono<Void> onInsert(Order order) {
        long n = attempts.incrementAndGet();
        if (order.getTotal() > 1500) {
            log.warn("[dlq] attempt #{} → reject big order {} (total={})",
                    n, order.getId().substring(0, 8), order.getTotal());
            return Mono.error(new PaymentRejectedException(
                    "Downstream payment processor refused order " + order.getId()));
        }
        return Mono.fromRunnable(() -> {
            successes.incrementAndGet();
            log.info("[dlq] ✓ {}", order);
        });
    }

    public long getSuccesses() { return successes.get(); }
}
