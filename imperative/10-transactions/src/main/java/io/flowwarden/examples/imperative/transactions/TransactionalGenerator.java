/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.imperative.transactions;

import io.flowwarden.examples.common.model.Order;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * In-sample data source so the sample is observable interactively
 * (the shared {@code examples-common} generator is disabled in the yml
 * because it doesn't know about transactions).
 *
 * <p>Every 2 seconds, opens a transaction and inserts {@link #TXN_BATCH}
 * orders inside it. Every 1 second, inserts a single standalone order.
 * The shapes produce a steady mix of transactional and non-transactional
 * events for {@link TransactionHandler} to bucket.</p>
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

    private final MongoTemplate mongoTemplate;
    private final TransactionTemplate transactionTemplate;

    TransactionalGenerator(MongoTemplate mongoTemplate, TransactionTemplate transactionTemplate) {
        this.mongoTemplate = mongoTemplate;
        this.transactionTemplate = transactionTemplate;
    }

    @Scheduled(fixedDelay = 2000, initialDelay = 1000)
    void emitTransactionalBatch() {
        transactionTemplate.executeWithoutResult(status -> {
            for (int i = 0; i < TXN_BATCH; i++) {
                mongoTemplate.save(randomOrder(), COLLECTION);
            }
            log.debug("Inserted {} orders in one transaction", TXN_BATCH);
        });
    }

    @Scheduled(fixedDelay = 1000, initialDelay = 500)
    void emitStandalone() {
        mongoTemplate.save(randomOrder(), COLLECTION);
    }

    private static Order randomOrder() {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        return new Order(
                CUSTOMERS[rng.nextInt(CUSTOMERS.length)],
                STATUSES[rng.nextInt(STATUSES.length)],
                Math.round(rng.nextDouble(5, 2000) * 100.0) / 100.0);
    }
}
