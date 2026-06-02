/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.reactive.dlq;

import java.util.Map;
import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

/**
 * Browsable view of the DLQ collection — reactive variant returning a
 * {@link Flux} of {@link Map} entries. See the imperative twin's
 * Javadoc for the policy / routing split rationale.
 */
@RestController
public class DlqController {

    /**
     * Custom DLQ collection declared on {@link DlqHandler} via
     * {@code @MongoDlqOptions(collection = ...)}.
     */
    static final String DLQ_COLLECTION = "orders-dlq-failed";

    private final ReactiveMongoTemplate mongoTemplate;

    public DlqController(ReactiveMongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @GetMapping("/dlq")
    public Flux<Map<String, Object>> listDlq() {
        Query query = new Query()
                .with(Sort.by(Sort.Direction.DESC, "_id"))
                .limit(50);
        return mongoTemplate.find(query, Document.class, DLQ_COLLECTION)
                .map(d -> (Map<String, Object>) d);
    }
}
