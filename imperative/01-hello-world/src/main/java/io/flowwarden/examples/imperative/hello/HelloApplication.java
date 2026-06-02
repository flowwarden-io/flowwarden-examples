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

import io.flowwarden.stream.annotation.EnableFlowWarden;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point of the {@code 01-hello-world} imperative sample.
 *
 * <p>{@link EnableFlowWarden} activates FlowWarden's discovery of
 * {@code @ChangeStream} beans. Scanning the broader
 * {@code io.flowwarden.examples} package picks up the shared
 * {@code DataGeneratorConfiguration} from {@code examples-common}.</p>
 */
@SpringBootApplication(scanBasePackages = "io.flowwarden.examples")
@EnableFlowWarden
public class HelloApplication {

    public static void main(String[] args) {
        SpringApplication.run(HelloApplication.class, args);
    }
}
