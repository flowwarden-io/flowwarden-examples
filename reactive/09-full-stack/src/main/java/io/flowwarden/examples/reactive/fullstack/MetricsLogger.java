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

import java.util.Date;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodic single-line summary. {@code @Scheduled} runs on a sync
 * worker; the DLQ size and checkpoint lookups are awaited via
 * {@code block()} since we are intentionally synchronous here.
 */
@Component
class MetricsLogger {

    private static final Logger log = LoggerFactory.getLogger(MetricsLogger.class);

    private final FullStackHandler handler;
    private final ReactiveMongoTemplate mongoTemplate;

    MetricsLogger(FullStackHandler handler, ReactiveMongoTemplate mongoTemplate) {
        this.handler = handler;
        this.mongoTemplate = mongoTemplate;
    }

    @Scheduled(fixedRate = 5000, initialDelay = 5000)
    void summarise() {
        long handled = handler.getHandledTotal();
        long rejected = handler.getClientRejectedByFilter();
        long retried = handler.getTransientFailureAttempts();
        Long dlq = mongoTemplate.count(new Query(), MetricsController.DLQ_COLLECTION).block();

        Document cp = mongoTemplate.findById(
                        MetricsController.STREAM_NAME, Document.class,
                        MetricsController.CHECKPOINTS_COLLECTION)
                .block();
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
