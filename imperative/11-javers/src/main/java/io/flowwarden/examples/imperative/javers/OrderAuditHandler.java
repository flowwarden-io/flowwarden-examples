/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.imperative.javers;

import io.flowwarden.examples.common.model.Order;
import io.flowwarden.javers.JaversChangeContext;
import io.flowwarden.javers.annotation.JaversStream;
import io.flowwarden.javers.annotation.OnInitial;
import io.flowwarden.javers.annotation.OnTerminal;
import io.flowwarden.javers.annotation.OnUpdate;
import io.flowwarden.stream.annotation.Checkpoint;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Receives Javers audit events for {@link Order} via FlowWarden.
 *
 * <p>{@link JaversStream} watches the {@code jv_snapshots} collection
 * and filters snapshots by {@link #entityType()} — only commits on
 * {@link Order} reach this handler. The three lifecycle hooks map to
 * the Javers {@code SnapshotType}: {@code INITIAL} (creation),
 * {@code UPDATE} (modification), {@code TERMINAL} (deletion).</p>
 *
 * <p>{@link Checkpoint} persists the MongoDB resume token after every
 * event so a restart resumes exactly where it left off.</p>
 *
 * <p>Counters are kept so the smoke test can assert that messages
 * actually flow through.</p>
 */
@Component
@JaversStream(entityType = Order.class)
@Checkpoint(saveEveryN = 1)
public class OrderAuditHandler {

    private static final Logger log = LoggerFactory.getLogger(OrderAuditHandler.class);

    private final AtomicLong created = new AtomicLong();
    private final AtomicLong updated = new AtomicLong();
    private final AtomicLong deleted = new AtomicLong();

    @OnInitial
    void onCreated(Order order, JaversChangeContext<Order> ctx) {
        long n = created.incrementAndGet();
        log.info("[javers] created #{} — id={} customer={} total={} by {}",
                n, order.getId(), order.getCustomer(), order.getTotal(),
                ctx.getCommitMetadata().getAuthor());
    }

    @OnUpdate
    void onUpdated(Order order, JaversChangeContext<Order> ctx) {
        long n = updated.incrementAndGet();
        log.info("[javers] updated #{} — id={} changed={} v{} by {}",
                n, order.getId(), ctx.getChangedProperties(), ctx.getVersion(),
                ctx.getCommitMetadata().getAuthor());
    }

    @OnTerminal
    void onDeleted(JaversChangeContext<Order> ctx) {
        long n = deleted.incrementAndGet();
        log.info("[javers] deleted #{} — id={} v{} by {}",
                n, ctx.getEntityId(), ctx.getVersion(),
                ctx.getCommitMetadata().getAuthor());
    }

    public long getCreated() { return created.get(); }
    public long getUpdated() { return updated.get(); }
    public long getDeleted() { return deleted.get(); }
}
