/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.imperative.errors;

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
class ErrorsSmokeTest {

    @Container
    @ServiceConnection
    static MongoDBContainer mongo = new MongoDBContainer("mongo:6.0");

    @Autowired
    ErrorHandler handler;

    @Test
    void typedHandlerCatchesValidationErrors() {
        // The generator sends ≈70% valid + ≈30% invalid inserts at 3/s,
        // so within a minute we should see both counters move.
        await().atMost(60, SECONDS).until(() -> handler.getAccepted() > 0);
        await().atMost(60, SECONDS).until(() -> handler.getValidationErrors() > 0);

        // The catch-all never fires in this sample (no other exception type
        // is produced) — verify it stays at zero.
        assertThat(handler.getUnexpectedErrors())
                .as("catch-all @OnError should not be invoked when the typed one matches")
                .isZero();
    }
}
