/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.reactive.checkpoint;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest
@Testcontainers
class CheckpointSmokeTest {

    private static final String CHECKPOINTS_COLLECTION = "_fw_checkpoints";
    private static final String STREAM_NAME = "checkpoint-handler";

    @Container
    @ServiceConnection
    static MongoDBContainer mongo = new MongoDBContainer("mongo:6.0");

    @Autowired
    ReactiveMongoTemplate mongoTemplate;

    @Autowired
    CheckpointHandler handler;

    @Test
    void resumeTokenIsPersistedForThisStream() {
        await().atMost(60, SECONDS).until(() -> handler.getReceived() > 0);

        Query query = Query.query(Criteria.where("_id").is(STREAM_NAME));
        await().atMost(30, SECONDS).until(() -> Boolean.TRUE.equals(
                mongoTemplate.count(query, CHECKPOINTS_COLLECTION)
                        .map(n -> n > 0)
                        .block()));

        long persisted = mongoTemplate.count(query, CHECKPOINTS_COLLECTION).block();
        assertThat(persisted)
                .as("a checkpoint document should exist in %s for stream '%s'",
                        CHECKPOINTS_COLLECTION, STREAM_NAME)
                .isPositive();
    }
}
