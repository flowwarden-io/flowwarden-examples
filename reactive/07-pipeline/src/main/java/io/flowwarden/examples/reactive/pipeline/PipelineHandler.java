/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.reactive.pipeline;

import io.flowwarden.examples.common.model.Order;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.OnInsert;
import io.flowwarden.stream.annotation.Pipeline;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import reactor.core.publisher.Mono;

/**
 * Reactive twin of {@code imperative/07-pipeline}. The {@code @Pipeline}
 * method itself stays synchronous — it's a one-shot declaration of the
 * aggregation stages, not a per-event hot path.
 */
@ChangeStream(collection = "orders-pipeline", documentType = Order.class)
public class PipelineHandler {

    private static final Logger log = LoggerFactory.getLogger(PipelineHandler.class);

    static final double THRESHOLD = 1000.0;

    private final AtomicLong received = new AtomicLong();
    private final AtomicLong minObservedTotalX100 = new AtomicLong(Long.MAX_VALUE);

    @Pipeline
    List<AggregationOperation> pipeline() {
        return List.of(
                Aggregation.match(Criteria.where("fullDocument.total").gt(THRESHOLD))
        );
    }

    @OnInsert
    Mono<Void> onInsert(Order order) {
        return Mono.fromRunnable(() -> {
            long n = received.incrementAndGet();
            minObservedTotalX100.accumulateAndGet((long) (order.getTotal() * 100), Math::min);
            log.info("[pipeline] #{} {}", n, order);
        });
    }

    public long getReceived() { return received.get(); }
    public double getMinObservedTotal() {
        long v = minObservedTotalX100.get();
        return v == Long.MAX_VALUE ? Double.NaN : v / 100.0;
    }
}
