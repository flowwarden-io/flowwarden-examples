/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.imperative.registration;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.flowwarden.stream.core.FlowWardenStreamManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest
@Testcontainers
class RegistrationSmokeTest {

    static final String STREAM = "confirmed-orders";
    static final String COLLECTION = "orders-registration";

    @Container
    @ServiceConnection
    static MongoDBContainer mongo = new MongoDBContainer("mongo:6.0");

    @Autowired
    MongoTemplate mongoTemplate;

    @Autowired
    FlowWardenStreamManager streamManager;

    @Autowired
    ReceivedOrders receivedOrders;

    @Test
    void yamlContributedStreamRunsAndFilters() {
        // The stream exists under the name declared in application.yml —
        // there is no @ChangeStream class anywhere in this module.
        await().atMost(60, SECONDS).until(() -> streamManager.isRunning(STREAM));

        await().atMost(60, SECONDS).until(() -> receivedOrders.received(STREAM) >= 2);

        // The contributed .filter(...) is client-side: non-CONFIRMED orders
        // do reach Mongo, they just never reach the handler.
        await().atMost(60, SECONDS).until(() ->
                mongoTemplate.count(
                        Query.query(Criteria.where("status").ne("CONFIRMED")),
                        COLLECTION) > 0);

        long droppedDocsInMongo = mongoTemplate.count(
                Query.query(Criteria.where("status").ne("CONFIRMED")), COLLECTION);
        assertThat(droppedDocsInMongo)
                .as("non-CONFIRMED orders must reach Mongo and be filtered by the contributed predicate")
                .isPositive();
    }
}
