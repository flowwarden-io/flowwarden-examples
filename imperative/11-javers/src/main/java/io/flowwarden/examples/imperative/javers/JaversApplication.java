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

import io.flowwarden.stream.annotation.EnableFlowWarden;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

/**
 * Entry point of the {@code 11-javers} imperative sample.
 *
 * <p>{@link EnableFlowWarden} activates FlowWarden's discovery of
 * {@code @ChangeStream} and {@code @JaversStream} beans. The Javers
 * Spring Boot starter on the classpath auto-configures a Javers instance
 * backed by the same MongoDB; the audited {@link OrderRepository} writes
 * snapshots that the handler consumes.</p>
 *
 * <p>{@link EnableMongoRepositories} is scoped to this package — the
 * shared examples-common package contains no repositories.</p>
 */
@SpringBootApplication(scanBasePackages = "io.flowwarden.examples")
@EnableMongoRepositories(basePackageClasses = OrderRepository.class)
@EnableFlowWarden
public class JaversApplication {

    public static void main(String[] args) {
        SpringApplication.run(JaversApplication.class, args);
    }
}
