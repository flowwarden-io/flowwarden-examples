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

import java.time.Duration;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * {@code GET /metrics} — surfaces the FlowWarden dual-token state and
 * the in-handler counters in a single JSON view, so the divergence is
 * visible at a glance without poking around {@code mongosh}.
 *
 * <p>The interesting field is {@code divergenceMillis}:
 * {@code lastSeenTimestamp - lastProcessedTimestamp}. A positive value
 * means {@code lastSeenToken} has advanced over events that
 * {@code lastProcessedToken} hasn't caught up with — which is exactly
 * what the dual-token model promises (and what a raw change stream
 * cannot give you).</p>
 *
 * <p>The raw BSON resume tokens are also exposed for completeness, but
 * comparing them by eye is unhelpful — they're opaque {@code _data}
 * strings. The timestamps make the gap concrete.</p>
 */
@RestController
public class MetricsController {

    static final String CHECKPOINTS_COLLECTION = "_fw_checkpoints";
    static final String DLQ_COLLECTION = "orders-full-dlq";
    static final String STREAM_NAME = "full-stack-handler";

    private final MongoTemplate mongoTemplate;
    private final FullStackHandler handler;

    public MetricsController(MongoTemplate mongoTemplate, FullStackHandler handler) {
        this.mongoTemplate = mongoTemplate;
        this.handler = handler;
    }

    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("stream", STREAM_NAME);
        out.put("checkpoint", readCheckpoint());
        out.put("counters", counters());
        return out;
    }

    private Map<String, Object> readCheckpoint() {
        Document doc = mongoTemplate.findById(STREAM_NAME, Document.class, CHECKPOINTS_COLLECTION);
        if (doc == null) {
            return Map.of("status", "no checkpoint yet");
        }
        Map<String, Object> view = new LinkedHashMap<>();
        Date seenAt = doc.get("lastSeenTimestamp", Date.class);
        Date procAt = doc.get("lastProcessedTimestamp", Date.class);
        view.put("lastSeenTimestamp", seenAt);
        view.put("lastProcessedTimestamp", procAt);
        view.put("divergenceMillis", divergenceMillis(seenAt, procAt));
        view.put("lastSeenToken", doc.get("lastSeenToken"));
        view.put("lastProcessedToken", doc.get("lastProcessedToken"));
        return view;
    }

    /**
     * Positive = {@code lastSeen} is ahead of {@code lastProcessed}.
     * That's the dual-token divergence. A raw change stream couldn't
     * produce this gap because it only has one token.
     */
    static Long divergenceMillis(Date seenAt, Date procAt) {
        if (seenAt == null || procAt == null) return null;
        return Duration.between(procAt.toInstant(), seenAt.toInstant()).toMillis();
    }

    private Map<String, Object> counters() {
        Map<String, Object> c = new LinkedHashMap<>();
        c.put("handled_insert", handler.getHandledInsert());
        c.put("handled_update", handler.getHandledUpdate());
        c.put("handled_replace", handler.getHandledReplace());
        c.put("client_rejected_by_filter", handler.getClientRejectedByFilter());
        c.put("transient_failure_attempts", handler.getTransientFailureAttempts());
        c.put("dlq_collection_size",
                mongoTemplate.count(new Query(), DLQ_COLLECTION));
        return c;
    }
}
