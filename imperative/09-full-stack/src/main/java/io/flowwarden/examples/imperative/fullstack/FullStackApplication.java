/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.imperative.fullstack;

import io.flowwarden.stream.annotation.EnableFlowWarden;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * {@link EnableScheduling} is on for the periodic metrics log (see
 * {@code MetricsLogger}); FlowWarden itself doesn't need it.
 */
@SpringBootApplication(scanBasePackages = "io.flowwarden.examples")
@EnableFlowWarden
@EnableScheduling
public class FullStackApplication {

    public static void main(String[] args) {
        SpringApplication.run(FullStackApplication.class, args);
    }
}
