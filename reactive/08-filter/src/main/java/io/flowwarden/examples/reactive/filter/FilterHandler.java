/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.reactive.filter;

import io.flowwarden.examples.common.model.Order;
import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Filter;
import io.flowwarden.stream.annotation.OnInsert;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * Reactive twin of {@code imperative/08-filter}.
 *
 * <p><b>The {@code @Filter} method itself stays synchronous</b> — the
 * lib does not offer a {@code Mono<Boolean>} signature. The decision
 * must be made without blocking I/O; if the predicate depends on async
 * state, cache it in a field and refresh it out-of-band. Only the
 * handler ({@code @OnInsert} below) returns {@code Mono<Void>}.</p>
 */
@ChangeStream(collection = "orders-filter", documentType = Order.class)
public class FilterHandler {

    private static final Logger log = LoggerFactory.getLogger(FilterHandler.class);

    static final String KEPT_STATUS = "CONFIRMED";

    private final AtomicLong received = new AtomicLong();

    @Filter
    boolean keepConfirmedOnly(ChangeStreamContext<Order> ctx) {
        return ctx.getFullDocument(Order.class)
                .map(o -> KEPT_STATUS.equals(o.getStatus()))
                .orElse(false);
    }

    @OnInsert
    Mono<Void> onInsert(Order order) {
        return Mono.fromRunnable(() -> {
            long n = received.incrementAndGet();
            log.info("[filter] #{} {}", n, order);
        });
    }

    public long getReceived() { return received.get(); }
}
