/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.imperative.dlq;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest
@Testcontainers
class DlqSmokeTest {

    @Container
    @ServiceConnection
    static MongoDBContainer mongo = new MongoDBContainer("mongo:6.0");

    @Autowired
    MongoTemplate mongoTemplate;

    @Autowired
    DlqHandler handler;

    @Test
    void exhaustedRetriesLandInDlqCollection() {
        // Stream is flowing.
        await().atMost(60, SECONDS).until(() -> handler.getSuccesses() > 0);

        // After exhaustion of the 2 retry attempts on a big-total order,
        // FlowWarden writes one document into the DLQ collection. The
        // collection is the hardcoded `_fw_dlq` in stream-core 1.0.0-rc.1
        // (see DlqController Javadoc).
        org.springframework.data.mongodb.core.query.Query streamQuery =
                org.springframework.data.mongodb.core.query.Query.query(
                        org.springframework.data.mongodb.core.query.Criteria
                                .where("streamName").is(DlqController.STREAM_NAME));

        await().atMost(90, SECONDS).until(() ->
                mongoTemplate.count(streamQuery, DlqController.DLQ_COLLECTION) > 0);

        long dlqSize = mongoTemplate.count(streamQuery, DlqController.DLQ_COLLECTION);
        assertThat(dlqSize)
                .as("exhausted retries should be written to %s for stream '%s'",
                        DlqController.DLQ_COLLECTION, DlqController.STREAM_NAME)
                .isPositive();
    }
}
