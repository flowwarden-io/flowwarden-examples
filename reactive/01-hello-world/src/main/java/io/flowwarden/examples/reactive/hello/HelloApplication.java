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

import io.flowwarden.stream.annotation.EnableFlowWarden;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point of the {@code 01-hello-world} reactive sample.
 *
 * <p>The application brings in {@code spring-boot-starter-data-mongodb-reactive},
 * so the shared {@code DataGeneratorConfiguration} from
 * {@code examples-common} activates the {@code ReactiveDataGenerator}.
 * FlowWarden picks up the reactive variant of {@code @ChangeStream}
 * via the same {@link EnableFlowWarden} switch.</p>
 */
@SpringBootApplication(scanBasePackages = "io.flowwarden.examples")
@EnableFlowWarden
public class HelloApplication {

    public static void main(String[] args) {
        SpringApplication.run(HelloApplication.class, args);
    }
}
