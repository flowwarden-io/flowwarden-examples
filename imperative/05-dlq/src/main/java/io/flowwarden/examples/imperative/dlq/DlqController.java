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

import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Browsable view of the DLQ collection written by FlowWarden.
 *
 * <p>{@code GET /dlq} returns the most recent entries scoped to this
 * sample's stream — useful to eyeball what the framework actually
 * stored for failed events. In production you'd wrap this with auth
 * and pagination.</p>
 *
 * <p>Note: in {@code stream-core:1.0.0-rc.1} the DLQ collection name
 * is always {@code _fw_dlq} (the {@code collection} attribute on
 * {@code @DeadLetterQueue} is not yet honoured). We filter by
 * {@code streamName} so this endpoint only shows entries written by
 * the {@code dlq-handler} stream — relevant when several DLQ-enabled
 * samples share the same database.</p>
 */
@RestController
public class DlqController {

    /** Hardcoded DLQ collection of stream-core 1.0.0-rc.1. */
    static final String DLQ_COLLECTION = "_fw_dlq";

    /** Auto-generated stream name (kebab-case of the handler class). */
    static final String STREAM_NAME = "dlq-handler";

    private final MongoTemplate mongoTemplate;

    public DlqController(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @GetMapping("/dlq")
    public List<Map<String, Object>> listDlq() {
        Query query = Query.query(Criteria.where("streamName").is(STREAM_NAME))
                .with(Sort.by(Sort.Direction.DESC, "_id"))
                .limit(50);
        return mongoTemplate.find(query, Document.class, DLQ_COLLECTION)
                .stream()
                .map(d -> (Map<String, Object>) d)
                .toList();
    }
}
