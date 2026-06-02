/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.imperative.typed;

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

/**
 * One handler class, five typed methods.
 *
 * <p>This sample shows that the typed callbacks come in two shapes:
 * the simple {@code (Order)} form (no metadata, easiest to write) and
 * the richer {@code (ChangeStreamContext&lt;Order&gt;)} form (gives
 * access to the operation type, resume token, cluster time, etc.).
 * You mix and match per method, depending on what each event handler
 * actually needs.</p>
 *
 * <p>{@link OnChange} is the catch-all: it fires only for operations
 * that have no specific handler on this class. In this sample all four
 * operations have a dedicated handler, so {@code any()} stays at zero —
 * remove one of the specific methods and you'll see {@code @OnChange}
 * pick up the slack. Useful when you don't care about the operation
 * type, or for handling future operations the lib doesn't yet model.</p>
 *
 * <p>For {@link OnDelete}, the POJO form is not usable because
 * MongoDB only forwards the document {@code _id} on a delete event —
 * the full document is gone. Use {@code ChangeStreamContext} instead.</p>
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
    void created(Order order) {
        inserts.incrementAndGet();
        log.info("[typed] INSERT  {}", order);
    }

    @OnUpdate
    void updated(ChangeStreamContext<Order> ctx) {
        updates.incrementAndGet();
        log.info("[typed] UPDATE  {}", ctx.summary());
    }

    @OnReplace
    void replaced(Order order) {
        replaces.incrementAndGet();
        log.info("[typed] REPLACE {}", order);
    }

    @OnDelete
    void deleted(ChangeStreamContext<Order> ctx) {
        deletes.incrementAndGet();
        log.info("[typed] DELETE  {}", ctx.summary());
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
    void any(ChangeStreamContext<Order> ctx) {
        total.incrementAndGet();
    }

    public long getInserts()  { return inserts.get(); }
    public long getUpdates()  { return updates.get(); }
    public long getReplaces() { return replaces.get(); }
    public long getDeletes()  { return deletes.get(); }
    public long getTotal()    { return total.get(); }
}
