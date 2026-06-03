/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.reactive.fullstack;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.Date;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
class FullStackSmokeTest {

    private static final Logger log = LoggerFactory.getLogger(FullStackSmokeTest.class);

    @Container
    @ServiceConnection
    static MongoDBContainer mongo = new MongoDBContainer("mongo:6.0");

    @Autowired
    ReactiveMongoTemplate mongoTemplate;

    @Autowired
    FullStackHandler handler;

    @Test
    void everyAnnotationFiresAndCheckpointIsPersisted() {
        await().atMost(60, SECONDS).until(() -> handler.getHandledInsert() > 0);
        await().atMost(60, SECONDS).until(() -> handler.getClientRejectedByFilter() > 0);

        Query checkpointQuery = Query.query(Criteria.where("_id").is(MetricsController.STREAM_NAME));
        await().atMost(60, SECONDS).until(() -> {
            Document d = mongoTemplate.findOne(checkpointQuery, Document.class,
                    MetricsController.CHECKPOINTS_COLLECTION).block();
            return d != null
                    && d.get("lastSeenTimestamp", Date.class) != null
                    && d.get("lastProcessedTimestamp", Date.class) != null;
        });

        Document checkpoint = mongoTemplate.findOne(checkpointQuery, Document.class,
                MetricsController.CHECKPOINTS_COLLECTION).block();
        Date seenAt = checkpoint.get("lastSeenTimestamp", Date.class);
        Date procAt = checkpoint.get("lastProcessedTimestamp", Date.class);
        Long divergenceMs = MetricsController.divergenceMillis(seenAt, procAt);
        log.info("[fullstack/test] lastSeenTimestamp={} lastProcessedTimestamp={} divergenceMillis={}",
                seenAt, procAt, divergenceMs);

        assertThat(handler.getClientRejectedByFilter())
                .as("@Filter must reject events")
                .isPositive();
        assertThat(handler.getHandledInsert())
                .as("@OnInsert must fire on confirmed events")
                .isPositive();
        assertThat(divergenceMs)
                .as("lastSeenTimestamp must be strictly ahead of lastProcessedTimestamp")
                .isPositive();
    }
}
