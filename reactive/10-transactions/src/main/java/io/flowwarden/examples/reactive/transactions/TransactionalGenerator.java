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
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;

/**
 * Reactive variant: composes the transactional batch as a
 * {@link Flux} of inserts and wraps it in a
 * {@link TransactionalOperator}, then subscribes (the
 * {@code @Scheduled} method runs on a sync worker).
 */
@Component
@ConditionalOnProperty(name = "examples.transactions.generator.enabled",
        havingValue = "true", matchIfMissing = true)
class TransactionalGenerator {

    private static final Logger log = LoggerFactory.getLogger(TransactionalGenerator.class);

    static final String COLLECTION = "orders-transactions";
    static final int TXN_BATCH = 3;

    private static final String[] CUSTOMERS = {
            "alice@example.com", "bob@example.com", "charlie@example.com",
            "diana@example.com", "eve@example.com"
    };
    private static final String[] STATUSES = {"PENDING", "CONFIRMED", "CANCELLED"};

    private final ReactiveMongoTemplate mongoTemplate;
    private final TransactionalOperator transactionalOperator;

    TransactionalGenerator(ReactiveMongoTemplate mongoTemplate,
                           TransactionalOperator transactionalOperator) {
        this.mongoTemplate = mongoTemplate;
        this.transactionalOperator = transactionalOperator;
    }

    @Scheduled(fixedDelay = 2000, initialDelay = 1000)
    void emitTransactionalBatch() {
        Flux.range(0, TXN_BATCH)
                .concatMap(i -> mongoTemplate.save(randomOrder(), COLLECTION))
                .as(transactionalOperator::transactional)
                .doOnComplete(() -> log.debug("Inserted {} orders in one transaction", TXN_BATCH))
                .blockLast();
    }

    @Scheduled(fixedDelay = 1000, initialDelay = 500)
    void emitStandalone() {
        mongoTemplate.save(randomOrder(), COLLECTION).block();
    }

    private static Order randomOrder() {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        return new Order(
                CUSTOMERS[rng.nextInt(CUSTOMERS.length)],
                STATUSES[rng.nextInt(STATUSES.length)],
                Math.round(rng.nextDouble(5, 2000) * 100.0) / 100.0);
    }
}
