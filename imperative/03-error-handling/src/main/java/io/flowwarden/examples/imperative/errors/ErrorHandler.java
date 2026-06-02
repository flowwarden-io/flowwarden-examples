/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.imperative.errors;

import io.flowwarden.examples.common.model.Order;
import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.ErrorAction;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.OnError;
import io.flowwarden.stream.annotation.OnInsert;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Throws on invalid payloads, then catches the exception with
 * {@code @OnError}.
 *
 * <p>The shared data generator is configured with
 * {@code invalid-payload-ratio: 0.3}, so roughly one insert in three
 * arrives with {@code status == null}. The handler validates that
 * field and throws {@link IllegalArgumentException}, which is then
 * routed to the typed {@code @OnError(IllegalArgumentException.class)}
 * method — not to the catch-all.</p>
 *
 * <p>FlowWarden picks the most specific {@code @OnError} method (exact
 * exception type → super-type → catch-all). The two methods here
 * illustrate the lookup; replace IAE with another exception type and
 * watch the catch-all engage instead.</p>
 *
 * <p>Every {@code @OnError} method returns an {@link ErrorAction}:
 * here we always {@link ErrorAction#SKIP} so the stream keeps going
 * without any retry or DLQ involvement. Samples {@code 04-retry-policy}
 * and {@code 05-dlq} show {@link ErrorAction#RETRY} and
 * {@link ErrorAction#DLQ}.</p>
 */
@ChangeStream(collection = "orders-errors", documentType = Order.class)
public class ErrorHandler {

    private static final Logger log = LoggerFactory.getLogger(ErrorHandler.class);

    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong validationErrors = new AtomicLong();
    private final AtomicLong unexpectedErrors = new AtomicLong();

    @OnInsert
    void onInsert(Order order) {
        if (order.getStatus() == null) {
            throw new IllegalArgumentException("Order.status cannot be null");
        }
        accepted.incrementAndGet();
        log.info("[errors] ACCEPT {}", order);
    }

    /**
     * Typed handler. Catches {@link IllegalArgumentException} (and any
     * subclass) thrown by {@link #onInsert}. Returning
     * {@link ErrorAction#SKIP} tells FlowWarden to log a warning and
     * move on to the next event without retrying.
     */
    @OnError(IllegalArgumentException.class)
    ErrorAction onValidationError(Throwable t, ChangeStreamContext<Order> ctx) {
        validationErrors.incrementAndGet();
        log.warn("[errors] SKIP validation error: {}", t.getMessage());
        return ErrorAction.SKIP;
    }

    /**
     * Catch-all (empty {@code value()} on {@code @OnError}). Reached
     * only when no typed handler above matches. In this sample no other
     * exception type is produced, so this method's counter stays at
     * zero — make {@link #onInsert} throw a {@code RuntimeException}
     * (or anything not assignable to {@code IllegalArgumentException})
     * and the routing will land here.
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
