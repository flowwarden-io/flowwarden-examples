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
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Browsable view of the DLQ collection written by FlowWarden.
 *
 * <p>{@code GET /dlq} returns the most recent entries from the
 * collection declared on {@link DlqHandler}'s {@code @MongoDlqOptions}.
 * In production you'd wrap this with auth and pagination.</p>
 */
@RestController
public class DlqController {

    /**
     * Custom DLQ collection declared on {@link DlqHandler} via
     * {@code @MongoDlqOptions(collection = ...)}. Must match exactly.
     */
    static final String DLQ_COLLECTION = "orders-dlq-failed";

    private final MongoTemplate mongoTemplate;

    public DlqController(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @GetMapping("/dlq")
    public List<Map<String, Object>> listDlq() {
        Query query = new Query()
                .with(Sort.by(Sort.Direction.DESC, "_id"))
                .limit(50);
        return mongoTemplate.find(query, Document.class, DLQ_COLLECTION)
                .stream()
                .map(d -> (Map<String, Object>) d)
                .toList();
    }
}
