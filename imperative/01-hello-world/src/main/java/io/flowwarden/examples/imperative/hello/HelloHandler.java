/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.imperative.hello;

import io.flowwarden.examples.common.model.Order;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.OnInsert;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The simplest possible FlowWarden handler: listen for inserts on
 * {@code orders-hello} and log them.
 *
 * <p>{@link ChangeStream} declares this class as a Spring component
 * managed by FlowWarden. The {@code documentType} attribute tells the
 * runtime to deserialise each change into an {@link Order}, so
 * {@link OnInsert} can take the typed POJO directly.</p>
 *
 * <p>A counter is kept so the smoke test can assert that messages
 * actually flow through.</p>
 */
@ChangeStream(collection = "orders-hello", documentType = Order.class)
public class HelloHandler {

    private static final Logger log = LoggerFactory.getLogger(HelloHandler.class);

    private final AtomicLong received = new AtomicLong();

    @OnInsert
    void onInsert(Order order) {
        long n = received.incrementAndGet();
        log.info("[hello] insert #{} — {}", n, order);
    }

    public long getReceived() {
        return received.get();
    }
}
