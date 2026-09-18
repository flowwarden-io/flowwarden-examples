/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.reactive.registration;

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
import reactor.core.publisher.Mono;

/**
 * Reactive twin of {@code imperative/12-registration}.
 *
 * <p>Same contributor, same YAML catalog. The only difference is the
 * handler shape: {@code .onInsertReactive(...)} takes a
 * {@code ReactiveDocumentHandler<Order>} returning {@code Mono<Void>},
 * mirroring a {@code Mono<Void> onInsert(Order, ChangeStreamContext)}
 * annotated method. The {@code .filter(...)} predicate and the
 * {@code .onError(...)} handler stay synchronous in both modes — the
 * lib offers no {@code Mono<Boolean>} filter signature.</p>
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
                    .onInsertReactive((order, ctx) ->
                            Mono.fromRunnable(() -> receivedOrders.record(entry.getName(), order)))
                    .onError((ex, ctx) -> {
                        log.warn("[registration:{}] skipping event after {}: {}",
                                entry.getName(), ex.getClass().getSimpleName(), ex.getMessage());
                        return ErrorAction.SKIP;
                    }, IllegalStateException.class);

            if (entry.isInsertsOnly()) {
                stream.pipeline(() -> List.of(
                        Aggregates.match(Filters.eq("operationType", "insert"))));
            }
            if (entry.getKeepStatus() != null) {
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
