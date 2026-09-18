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

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import io.flowwarden.examples.common.model.Order;
import io.flowwarden.stream.ErrorAction;
import io.flowwarden.stream.registration.CheckpointSpec;
import io.flowwarden.stream.registration.DeadLetterQueueSpec;
import io.flowwarden.stream.registration.StreamDefinitionContributor;
import io.flowwarden.stream.registration.StreamRegistration;
import io.flowwarden.stream.registration.StreamSpec;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Annotation-free stream registration.
 *
 * <p>Every other example declares its streams with {@code @ChangeStream}
 * on a class. Here there is no annotated class at all: this contributor
 * reads {@link RegistrationProperties} (bound from {@code application.yml})
 * and turns each entry into a {@link StreamSpec} through the
 * {@link StreamRegistration} builder. FlowWarden discovers the bean,
 * calls {@link #contribute} once at bootstrap — after all singletons
 * exist, before any stream starts — and runs the contributed streams
 * through the exact same validation as annotated ones.</p>
 *
 * <p>Each builder call below maps 1:1 to an annotation the other
 * examples use: {@code .pipeline(...)} is {@code @Pipeline} (07),
 * {@code .filter(...)} is {@code @Filter} (08), {@code .onError(...)}
 * is {@code @OnError} (03), {@code .checkpoint(...)} is
 * {@code @Checkpoint} (06), {@code .deadLetterQueue(...)} is
 * {@code @DeadLetterQueue} (05), {@code .onInsert(...)} is
 * {@code @OnInsert}. Same defaults, same fail-fast rules.</p>
 */
@Component
public class YamlStreamContributor implements StreamDefinitionContributor {

    private static final Logger log = LoggerFactory.getLogger(YamlStreamContributor.class);

    private final RegistrationProperties properties;
    private final ReceivedOrders receivedOrders;

    public YamlStreamContributor(RegistrationProperties properties, ReceivedOrders receivedOrders) {
        this.properties = properties;
        this.receivedOrders = receivedOrders;
    }

    @Override
    public void contribute(StreamRegistration registration) {
        for (RegistrationProperties.StreamEntry entry : properties.getStreams()) {
            log.info("[registration] contributing stream '{}' on collection '{}'",
                    entry.getName(), entry.getCollection());

            StreamSpec.Builder<Order> stream = registration
                    .stream(entry.getName(), Order.class)
                    .collection(entry.getCollection())
                    .checkpoint(CheckpointSpec.defaults())
                    // Typed handler: a DocumentHandler<Order> lambda — the
                    // deserialised document plus the context, like
                    // `void onInsert(Order order, ChangeStreamContext<Order> ctx)`.
                    .onInsert((order, ctx) -> receivedOrders.record(entry.getName(), order))
                    // Scoped error handler (equivalent of @OnError(IllegalStateException.class)):
                    // skip the event instead of retrying it.
                    .onError((ex, ctx) -> {
                        log.warn("[registration:{}] skipping event after {}: {}",
                                entry.getName(), ex.getClass().getSimpleName(), ex.getMessage());
                        return ErrorAction.SKIP;
                    }, IllegalStateException.class);

            if (entry.isInsertsOnly()) {
                // Server-side, evaluated once at stream start (07-pipeline).
                stream.pipeline(() -> List.of(
                        Aggregates.match(Filters.eq("operationType", "insert"))));
            }
            if (entry.getKeepStatus() != null) {
                // Client-side, evaluated on every event (08-filter).
                String keep = entry.getKeepStatus();
                stream.filter(ctx -> ctx.getFullDocument(Order.class)
                        .map(o -> keep.equals(o.getStatus()))
                        .orElse(false));
            }
            if (entry.isDlq()) {
                stream.deadLetterQueue(DeadLetterQueueSpec.defaults());
            }
        }
    }
}
