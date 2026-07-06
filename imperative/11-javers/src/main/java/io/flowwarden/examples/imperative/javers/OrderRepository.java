/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.imperative.javers;

import io.flowwarden.examples.common.model.Order;
import org.javers.spring.annotation.JaversSpringDataAuditable;
import org.springframework.data.mongodb.repository.MongoRepository;

/**
 * Spring Data MongoDB repository for {@link Order}, audited by Javers.
 *
 * <p>The {@link JaversSpringDataAuditable} annotation is what makes
 * audit work — Javers intercepts {@code save} / {@code delete} calls on
 * this interface and writes snapshots to the {@code jv_snapshots}
 * collection. Direct {@code MongoTemplate} writes are <strong>not</strong>
 * audited, which is why this sample ships a small {@link
 * AuditedOrderWriter} instead of using the shared {@code DataGenerator}.</p>
 */
@JaversSpringDataAuditable
public interface OrderRepository extends MongoRepository<Order, String> {
}
