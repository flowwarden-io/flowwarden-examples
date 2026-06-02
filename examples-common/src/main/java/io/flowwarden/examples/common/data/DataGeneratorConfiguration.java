/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.common.data;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;

/**
 * Wires the right {@code DataGenerator} for each sample.
 *
 * <p>Selection is driven by the {@code examples.data-generator.mode}
 * property: {@code imperative} (default) wires an
 * {@link ImperativeDataGenerator} backed by {@link MongoTemplate};
 * {@code reactive} wires a {@link ReactiveDataGenerator} backed by
 * {@link ReactiveMongoTemplate}.</p>
 *
 * <p>Explicit selection — rather than auto-detecting which driver is on
 * the classpath — keeps the choice visible to the reader of each
 * sample's {@code application.yml}.</p>
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(DataGeneratorProperties.class)
@ConditionalOnProperty(prefix = "examples.data-generator",
        name = "enabled", havingValue = "true", matchIfMissing = true)
public class DataGeneratorConfiguration {

    @Bean(initMethod = "start", destroyMethod = "stop")
    @ConditionalOnProperty(prefix = "examples.data-generator",
            name = "mode", havingValue = "imperative", matchIfMissing = true)
    public ImperativeDataGenerator imperativeDataGenerator(
            MongoTemplate mongoTemplate, DataGeneratorProperties props) {
        return new ImperativeDataGenerator(mongoTemplate, props);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    @ConditionalOnProperty(prefix = "examples.data-generator",
            name = "mode", havingValue = "reactive")
    public ReactiveDataGenerator reactiveDataGenerator(
            ReactiveMongoTemplate mongoTemplate, DataGeneratorProperties props) {
        return new ReactiveDataGenerator(mongoTemplate, props);
    }
}
