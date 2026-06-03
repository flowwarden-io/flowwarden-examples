/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.imperative.fullstack;

import java.util.Date;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodic single-line summary so you don't have to keep curl'ing
 * {@code /metrics}. Useful when running the sample interactively.
 *
 * <p>The {@code divergence} value is the concrete number of
 * milliseconds {@code lastSeenToken} is ahead of
 * {@code lastProcessedToken} in {@code _fw_checkpoints}. It's the
 * thing you cannot get from a raw MongoDB change stream.</p>
 */
@Component
class MetricsLogger {

    private static final Logger log = LoggerFactory.getLogger(MetricsLogger.class);

    private final FullStackHandler handler;
    private final MongoTemplate mongoTemplate;

    MetricsLogger(FullStackHandler handler, MongoTemplate mongoTemplate) {
        this.handler = handler;
        this.mongoTemplate = mongoTemplate;
    }

    @Scheduled(fixedRate = 5000, initialDelay = 5000)
    void summarise() {
        long handled = handler.getHandledTotal();
        long rejected = handler.getClientRejectedByFilter();
        long retried = handler.getTransientFailureAttempts();
        long dlq = mongoTemplate.count(new Query(), MetricsController.DLQ_COLLECTION);

        Document cp = mongoTemplate.findById(
                MetricsController.STREAM_NAME, Document.class,
                MetricsController.CHECKPOINTS_COLLECTION);
        String divergence = "no checkpoint yet";
        if (cp != null) {
            Long ms = MetricsController.divergenceMillis(
                    cp.get("lastSeenTimestamp", Date.class),
                    cp.get("lastProcessedTimestamp", Date.class));
            divergence = ms == null ? "tokens not both set" : (ms + " ms");
        }

        log.info("[fullstack/metrics] handled={} filter-rejected={} retry-attempts={} dlq={} — lastSeen ahead of lastProcessed by {}",
                handled, rejected, retried, dlq, divergence);
    }
}
