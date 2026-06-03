/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.reactive.transactions;

import io.flowwarden.examples.common.model.Order;
import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.TransactionInfo;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.OnInsert;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * Reactive twin of {@code imperative/10-transactions}. See the
 * imperative twin's Javadoc for the {@code getTransactionInfo()} story.
 */
@ChangeStream(collection = "orders-transactions", documentType = Order.class)
public class TransactionHandler {

    private static final Logger log = LoggerFactory.getLogger(TransactionHandler.class);

    private final AtomicLong txnEvents = new AtomicLong();
    private final AtomicLong standaloneEvents = new AtomicLong();
    private final Map<Long, AtomicLong> txnGroupSizes = new ConcurrentHashMap<>();

    @OnInsert
    Mono<Void> onInsert(Order order, ChangeStreamContext<Order> ctx) {
        return Mono.fromRunnable(() ->
                ctx.getTransactionInfo().ifPresentOrElse(
                        txn -> recordTransactional(order, txn),
                        () -> recordStandalone(order)));
    }

    private void recordTransactional(Order order, TransactionInfo txn) {
        txnEvents.incrementAndGet();
        long groupSize = txnGroupSizes
                .computeIfAbsent(txn.txnNumber(), k -> new AtomicLong())
                .incrementAndGet();
        log.info("[txn] INSERT {} — txnNumber={} group-size={}",
                order, txn.txnNumber(), groupSize);
    }

    private void recordStandalone(Order order) {
        standaloneEvents.incrementAndGet();
        log.info("[standalone] INSERT {}", order);
    }

    public long getTxnEvents()        { return txnEvents.get(); }
    public long getStandaloneEvents() { return standaloneEvents.get(); }
    public long getMaxTxnGroupSize() {
        return txnGroupSizes.values().stream()
                .mapToLong(AtomicLong::get).max().orElse(0L);
    }
}
