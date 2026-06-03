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

import java.time.Duration;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import org.bson.Document;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * Reactive twin of {@code imperative/09-full-stack}'s controller. See
 * the imperative Javadoc for the meaning of {@code divergenceMillis}.
 */
@RestController
public class MetricsController {

    static final String CHECKPOINTS_COLLECTION = "_fw_checkpoints";
    static final String DLQ_COLLECTION = "orders-full-dlq";
    static final String STREAM_NAME = "full-stack-handler";

    private final ReactiveMongoTemplate mongoTemplate;
    private final FullStackHandler handler;

    public MetricsController(ReactiveMongoTemplate mongoTemplate, FullStackHandler handler) {
        this.mongoTemplate = mongoTemplate;
        this.handler = handler;
    }

    @GetMapping("/metrics")
    public Mono<Map<String, Object>> metrics() {
        Mono<Map<String, Object>> checkpoint = readCheckpoint();
        Mono<Long> dlqSize = mongoTemplate.count(new Query(), DLQ_COLLECTION);
        return Mono.zip(checkpoint, dlqSize)
                .map(tuple -> {
                    Map<String, Object> out = new LinkedHashMap<>();
                    out.put("stream", STREAM_NAME);
                    out.put("checkpoint", tuple.getT1());
                    out.put("counters", counters(tuple.getT2()));
                    return out;
                });
    }

    private Mono<Map<String, Object>> readCheckpoint() {
        return mongoTemplate.findById(STREAM_NAME, Document.class, CHECKPOINTS_COLLECTION)
                .map(doc -> {
                    Map<String, Object> view = new LinkedHashMap<>();
                    Date seenAt = doc.get("lastSeenTimestamp", Date.class);
                    Date procAt = doc.get("lastProcessedTimestamp", Date.class);
                    view.put("lastSeenTimestamp", seenAt);
                    view.put("lastProcessedTimestamp", procAt);
                    view.put("divergenceMillis", divergenceMillis(seenAt, procAt));
                    view.put("lastSeenToken", doc.get("lastSeenToken"));
                    view.put("lastProcessedToken", doc.get("lastProcessedToken"));
                    return view;
                })
                .defaultIfEmpty(Map.of("status", "no checkpoint yet"));
    }

    /** Positive = lastSeen ahead of lastProcessed. See imperative twin. */
    static Long divergenceMillis(Date seenAt, Date procAt) {
        if (seenAt == null || procAt == null) return null;
        return Duration.between(procAt.toInstant(), seenAt.toInstant()).toMillis();
    }

    private Map<String, Object> counters(long dlqSize) {
        Map<String, Object> c = new LinkedHashMap<>();
        c.put("handled_insert", handler.getHandledInsert());
        c.put("handled_update", handler.getHandledUpdate());
        c.put("handled_replace", handler.getHandledReplace());
        c.put("client_rejected_by_filter", handler.getClientRejectedByFilter());
        c.put("transient_failure_attempts", handler.getTransientFailureAttempts());
        c.put("dlq_collection_size", dlqSize);
        return c;
    }
}
