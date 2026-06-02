/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.reactive.hello;

import io.flowwarden.examples.common.model.Order;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.OnInsert;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * Reactive twin of the imperative HelloHandler: same wiring, but the
 * handler method returns a {@link Mono} that FlowWarden subscribes to.
 *
 * <p>Returning {@code Mono.fromRunnable(...)} is fine for fire-and-forget
 * side-effects. A real reactive handler would typically compose with
 * other reactive operations (Mongo writes, HTTP calls, ...) and return
 * the resulting publisher.</p>
 */
@ChangeStream(collection = "orders-hello", documentType = Order.class)
public class HelloHandler {

    private static final Logger log = LoggerFactory.getLogger(HelloHandler.class);

    private final AtomicLong received = new AtomicLong();

    @OnInsert
    Mono<Void> onInsert(Order order) {
        return Mono.fromRunnable(() -> {
            long n = received.incrementAndGet();
            log.info("[hello] insert #{} — {}", n, order);
        });
    }

    public long getReceived() {
        return received.get();
    }
}
