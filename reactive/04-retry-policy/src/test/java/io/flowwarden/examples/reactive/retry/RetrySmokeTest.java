/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.reactive.retry;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest
@Testcontainers
class RetrySmokeTest {

    @Container
    @ServiceConnection
    static MongoDBContainer mongo = new MongoDBContainer("mongo:6.0");

    @Autowired
    RetryHandler handler;

    @Test
    void atLeastOneFailingOrderIsRetried() {
        await().atMost(60, SECONDS).until(() -> handler.getSuccesses() > 0);
        await().atMost(90, SECONDS).until(() -> handler.getMaxAttemptsForAnyOrder() >= 2);

        assertThat(handler.getMaxAttemptsForAnyOrder())
                .as("@RetryPolicy must have re-invoked the handler on a failing order")
                .isGreaterThanOrEqualTo(2);
    }
}
