/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.reactive.checkpoint;

import io.flowwarden.examples.common.model.Order;
import io.flowwarden.stream.OnHistoryLost;
import io.flowwarden.stream.StartPosition;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.OnInsert;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * Reactive twin of {@code imperative/06-checkpoint}. Same checkpoint
 * settings, same {@code _fw_checkpoints/checkpoint-handler} document —
 * only the handler return type changes to {@link Mono}.
 */
@ChangeStream(collection = "orders-checkpoint", documentType = Order.class)
@Checkpoint(
        saveEveryN = 1,
        saveIntervalSeconds = 3,
        idleHeartbeatIntervalSeconds = 30,
        startPosition = StartPosition.RESUME,
        onHistoryLost = OnHistoryLost.RESUME_FROM_NOW
)
public class CheckpointHandler {

    private static final Logger log = LoggerFactory.getLogger(CheckpointHandler.class);

    private final AtomicLong received = new AtomicLong();

    @OnInsert
    Mono<Void> onInsert(Order order) {
        return Mono.fromRunnable(() -> {
            long n = received.incrementAndGet();
            log.info("[checkpoint] #{} {}", n, order);
        });
    }

    public long getReceived() { return received.get(); }
}
