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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.flowwarden.examples.common.model.Order;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Drives the events itself so the assertions are deterministic — the
 * scheduled {@link TransactionalGenerator} is silenced via the
 * {@code examples.transactions.generator.enabled=false} override.
 */
@SpringBootTest
@Testcontainers
@TestPropertySource(properties = "examples.transactions.generator.enabled=false")
class TransactionSmokeTest {

    @Container
    @ServiceConnection
    static MongoDBContainer mongo = new MongoDBContainer("mongo:6.0");

    @Autowired
    MongoTemplate mongoTemplate;

    @Autowired
    TransactionTemplate transactionTemplate;

    @Autowired
    TransactionHandler handler;

    @Test
    void transactionalInsertsShareTxnNumber() {
        // Standalone first — proves the stream is alive and that
        // standaloneEvents counter moves independently of the
        // transactional path.
        mongoTemplate.save(new Order("alice@example.com", "PENDING", 12.5),
                TransactionalGenerator.COLLECTION);
        await().atMost(30, SECONDS).until(() -> handler.getStandaloneEvents() >= 1);

        // Three inserts in a single transaction. All three events
        // should arrive at the handler with the same TransactionInfo
        // — the canonical proof of the dual-token model's value here.
        transactionTemplate.executeWithoutResult(status -> {
            for (int i = 0; i < TransactionalGenerator.TXN_BATCH; i++) {
                mongoTemplate.save(
                        new Order("bob@example.com", "CONFIRMED", 100.0 + i),
                        TransactionalGenerator.COLLECTION);
            }
        });

        await().atMost(30, SECONDS)
                .until(() -> handler.getTxnEvents() >= TransactionalGenerator.TXN_BATCH);

        assertThat(handler.getTxnEvents())
                .as("the 3 inserts emitted inside the transaction must reach @OnInsert "
                        + "and be classified as transactional via ctx.getTransactionInfo()")
                .isGreaterThanOrEqualTo(TransactionalGenerator.TXN_BATCH);

        assertThat(handler.getMaxTxnGroupSize())
                .as("the 3 events shared the same lsid + txnNumber, so they "
                        + "should land in a single bucket of size >= TXN_BATCH")
                .isGreaterThanOrEqualTo(TransactionalGenerator.TXN_BATCH);

        assertThat(handler.getStandaloneEvents())
                .as("the warm-up standalone insert must remain on the non-transactional path")
                .isGreaterThanOrEqualTo(1);
    }
}
