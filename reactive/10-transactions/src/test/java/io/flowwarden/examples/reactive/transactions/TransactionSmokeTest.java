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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.flowwarden.examples.common.model.Order;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;

@SpringBootTest
@Testcontainers
@TestPropertySource(properties = "examples.transactions.generator.enabled=false")
class TransactionSmokeTest {

    @Container
    @ServiceConnection
    static MongoDBContainer mongo = new MongoDBContainer("mongo:6.0");

    @Autowired
    ReactiveMongoTemplate mongoTemplate;

    @Autowired
    TransactionalOperator transactionalOperator;

    @Autowired
    TransactionHandler handler;

    @Test
    void transactionalInsertsShareTxnNumber() {
        mongoTemplate.save(new Order("alice@example.com", "PENDING", 12.5),
                TransactionalGenerator.COLLECTION).block();
        await().atMost(30, SECONDS).until(() -> handler.getStandaloneEvents() >= 1);

        Flux.range(0, TransactionalGenerator.TXN_BATCH)
                .concatMap(i -> mongoTemplate.save(
                        new Order("bob@example.com", "CONFIRMED", 100.0 + i),
                        TransactionalGenerator.COLLECTION))
                .as(transactionalOperator::transactional)
                .blockLast();

        await().atMost(30, SECONDS)
                .until(() -> handler.getTxnEvents() >= TransactionalGenerator.TXN_BATCH);

        assertThat(handler.getTxnEvents())
                .as("the 3 inserts emitted inside the reactive transaction must reach @OnInsert "
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
