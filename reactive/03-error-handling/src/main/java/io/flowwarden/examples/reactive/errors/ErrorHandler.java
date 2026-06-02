/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.reactive.errors;

import io.flowwarden.examples.common.model.Order;
import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.ErrorAction;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.OnError;
import io.flowwarden.stream.annotation.OnInsert;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * Reactive twin of {@code imperative/03}: the handler returns a failing
 * {@link Mono} (via {@code Mono.error(...)}) for invalid payloads,
 * which FlowWarden treats exactly like a thrown exception in imperative
 * mode and routes to the matching {@code @OnError} method.
 *
 * <p>The {@code @OnError} methods themselves return
 * {@link ErrorAction} <em>synchronously</em> — the contract is the same
 * as in imperative mode. Only the business handler shape changes.</p>
 */
@ChangeStream(collection = "orders-errors", documentType = Order.class)
public class ErrorHandler {

    private static final Logger log = LoggerFactory.getLogger(ErrorHandler.class);

    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong validationErrors = new AtomicLong();
    private final AtomicLong unexpectedErrors = new AtomicLong();

    @OnInsert
    Mono<Void> onInsert(Order order) {
        if (order.getStatus() == null) {
            return Mono.error(new IllegalArgumentException("Order.status cannot be null"));
        }
        return Mono.fromRunnable(() -> {
            accepted.incrementAndGet();
            log.info("[errors] ACCEPT {}", order);
        });
    }

    /** Typed @OnError — catches IllegalArgumentException (and subclasses). */
    @OnError(IllegalArgumentException.class)
    ErrorAction onValidationError(Throwable t, ChangeStreamContext<Order> ctx) {
        validationErrors.incrementAndGet();
        log.warn("[errors] SKIP validation error: {}", t.getMessage());
        return ErrorAction.SKIP;
    }

    /**
     * Catch-all @OnError. Never fires in this sample (no other exception
     * type produced); see imperative variant Javadoc for the routing
     * rule.
     */
    @OnError
    ErrorAction onAnyError(Throwable t, ChangeStreamContext<Order> ctx) {
        unexpectedErrors.incrementAndGet();
        log.error("[errors] SKIP unexpected error: {}", t.toString());
        return ErrorAction.SKIP;
    }

    public long getAccepted()         { return accepted.get(); }
    public long getValidationErrors() { return validationErrors.get(); }
    public long getUnexpectedErrors() { return unexpectedErrors.get(); }
}
