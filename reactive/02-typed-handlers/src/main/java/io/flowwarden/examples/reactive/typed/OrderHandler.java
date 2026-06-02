/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.reactive.typed;

import io.flowwarden.examples.common.model.Order;
import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.OnChange;
import io.flowwarden.stream.annotation.OnDelete;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.annotation.OnReplace;
import io.flowwarden.stream.annotation.OnUpdate;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * Reactive twin of imperative/02 — same five typed methods, each
 * returning a {@link Mono} that FlowWarden subscribes to.
 *
 * <p>For pure side-effects ({@code log + counter}) we use
 * {@code Mono.fromRunnable}. A production reactive handler would
 * compose async work (DB writes, HTTP calls) and return the resulting
 * publisher.</p>
 *
 * <p>{@link OnChange} is the catch-all: it fires only for operations
 * that have no specific handler on this class. In this sample it stays
 * at zero, since the four specific handlers cover everything.</p>
 */
@ChangeStream(collection = "orders-typed", documentType = Order.class)
public class OrderHandler {

    private static final Logger log = LoggerFactory.getLogger(OrderHandler.class);

    private final AtomicLong inserts = new AtomicLong();
    private final AtomicLong updates = new AtomicLong();
    private final AtomicLong replaces = new AtomicLong();
    private final AtomicLong deletes = new AtomicLong();
    private final AtomicLong total = new AtomicLong();

    @OnInsert
    Mono<Void> created(Order order) {
        return Mono.fromRunnable(() -> {
            inserts.incrementAndGet();
            log.info("[typed] INSERT  {}", order);
        });
    }

    @OnUpdate
    Mono<Void> updated(ChangeStreamContext<Order> ctx) {
        return Mono.fromRunnable(() -> {
            updates.incrementAndGet();
            log.info("[typed] UPDATE  {}", ctx.summary());
        });
    }

    @OnReplace
    Mono<Void> replaced(Order order) {
        return Mono.fromRunnable(() -> {
            replaces.incrementAndGet();
            log.info("[typed] REPLACE {}", order);
        });
    }

    @OnDelete
    Mono<Void> deleted(ChangeStreamContext<Order> ctx) {
        return Mono.fromRunnable(() -> {
            deletes.incrementAndGet();
            log.info("[typed] DELETE  {}", ctx.summary());
        });
    }

    /**
     * Never invoked in this sample.
     *
     * <p>{@code @OnChange} is a catch-all: FlowWarden only routes an
     * event here when no specific {@code @OnXxx} method on this class
     * matches. The four specific handlers above cover every operation
     * the data generator produces, so this method's counter stays at
     * zero. Comment out one of {@link #created}, {@link #updated},
     * {@link #replaced} or {@link #deleted} and re-run to see this
     * method pick up the slack.</p>
     */
    @OnChange
    Mono<Void> any(ChangeStreamContext<Order> ctx) {
        return Mono.fromRunnable(total::incrementAndGet);
    }

    public long getInserts()  { return inserts.get(); }
    public long getUpdates()  { return updates.get(); }
    public long getReplaces() { return replaces.get(); }
    public long getDeletes()  { return deletes.get(); }
    public long getTotal()    { return total.get(); }
}
