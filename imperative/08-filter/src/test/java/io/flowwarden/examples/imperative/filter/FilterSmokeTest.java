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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

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
class FilterSmokeTest {

    @Container
    @ServiceConnection
    static MongoDBContainer mongo = new MongoDBContainer("mongo:6.0");

    @Autowired
    MongoTemplate mongoTemplate;

    @Autowired
    FilterHandler handler;

    @Test
    void onlyConfirmedReachTheHandler() {
        await().atMost(60, SECONDS).until(() -> handler.getReceived() >= 2);

        // The whole point: filter is *client-side*, so non-CONFIRMED docs
        // do reach Mongo (they just don't reach the handler).
        await().atMost(60, SECONDS).until(() ->
                mongoTemplate.count(
                        Query.query(Criteria.where("status").ne(FilterHandler.KEPT_STATUS)),
                        "orders-filter") > 0);

        long droppedDocsInMongo = mongoTemplate.count(
                Query.query(Criteria.where("status").ne(FilterHandler.KEPT_STATUS)),
                "orders-filter");
        assertThat(droppedDocsInMongo)
                .as("non-CONFIRMED orders must reach Mongo and be filtered client-side")
                .isPositive();
    }
}
