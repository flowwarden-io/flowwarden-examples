/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.imperative.registration;

import io.flowwarden.examples.common.model.Order;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Plain Spring bean the contributed handlers delegate to. Nothing
 * FlowWarden-specific here — this is what a "real" handler would be:
 * a service, a repository, a cache — captured by the lambda.
 */
@Component
public class ReceivedOrders {

    private static final Logger log = LoggerFactory.getLogger(ReceivedOrders.class);

    private final ConcurrentHashMap<String, AtomicLong> perStream = new ConcurrentHashMap<>();

    void record(String streamName, Order order) {
        long n = perStream.computeIfAbsent(streamName, k -> new AtomicLong()).incrementAndGet();
        log.info("[registration:{}] #{} {}", streamName, n, order);
    }

    public long received(String streamName) {
        AtomicLong counter = perStream.get(streamName);
        return counter == null ? 0 : counter.get();
    }
}
