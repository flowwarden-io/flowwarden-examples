/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.imperative.filter;

import io.flowwarden.examples.common.model.Order;
import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Filter;
import io.flowwarden.stream.annotation.OnInsert;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client-side filter via {@code @Filter}.
 *
 * <p>Compared with {@code 07-pipeline}: same predicate intent (drop
 * events that don't match), opposite trade-off. The event crosses the
 * wire and is decoded by the driver before this method even runs, so
 * filtering here doesn't save MongoDB → client bandwidth. In exchange,
 * the predicate can read Spring beans, hit an in-memory cache, depend
 * on a feature flag, or change at runtime — none of which a
 * server-side {@code @Pipeline} can do.</p>
 *
 * <p>Pick {@code @Pipeline} when the criterion is a stable static
 * shape; pick {@code @Filter} when it depends on runtime state. Stack
 * both for a coarse server filter + a fine application refinement.</p>
 */
@ChangeStream(collection = "orders-filter", documentType = Order.class)
public class FilterHandler {

    private static final Logger log = LoggerFactory.getLogger(FilterHandler.class);

    static final String KEPT_STATUS = "CONFIRMED";

    private final AtomicLong received = new AtomicLong();

    /**
     * Keep only orders with {@code status == CONFIRMED}. Events without
     * a full document (e.g. deletes when no pre-image is captured) are
     * dropped — the Optional returns empty and {@code orElse(false)}
     * filters them out.
     */
    @Filter
    boolean keepConfirmedOnly(ChangeStreamContext<Order> ctx) {
        return ctx.getFullDocument(Order.class)
                .map(o -> KEPT_STATUS.equals(o.getStatus()))
                .orElse(false);
    }

    @OnInsert
    void onInsert(Order order) {
        long n = received.incrementAndGet();
        log.info("[filter] #{} {}", n, order);
    }

    public long getReceived() { return received.get(); }
}
