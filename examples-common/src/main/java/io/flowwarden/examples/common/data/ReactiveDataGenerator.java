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

import io.flowwarden.examples.common.model.Order;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Reactive twin of {@link ImperativeDataGenerator}, driving the
 * {@code reactive/} samples against a {@link ReactiveMongoTemplate}.
 *
 * <p>Each operation is a {@code Flux.interval(...)} merged into a single
 * subscription. Lifecycle is managed via a {@link Disposables.Composite}
 * that {@link #stop()} disposes.</p>
 */
public class ReactiveDataGenerator {

    private static final Logger log = LoggerFactory.getLogger(ReactiveDataGenerator.class);

    private static final String[] CUSTOMERS = {
            "alice@example.com", "bob@example.com", "charlie@example.com",
            "diana@example.com", "eve@example.com"
    };
    private static final String[] STATUSES = {"PENDING", "CONFIRMED", "CANCELLED"};

    private final ReactiveMongoTemplate mongoTemplate;
    private final DataGeneratorProperties props;
    private final Set<String> knownIds = new ConcurrentSkipListSet<>();
    private final Disposable.Composite disposables = Disposables.composite();
    private final AtomicLong inserts = new AtomicLong();
    private final AtomicLong updates = new AtomicLong();
    private final AtomicLong deletes = new AtomicLong();
    private final AtomicLong replaces = new AtomicLong();

    public ReactiveDataGenerator(ReactiveMongoTemplate mongoTemplate, DataGeneratorProperties props) {
        this.mongoTemplate = mongoTemplate;
        this.props = props;
    }

    public void start() {
        DataGeneratorProperties.Rates rates = props.getRates();
        scheduleFlux(rates.getInsertsPerSecond(), this::insertOne);
        scheduleFlux(rates.getUpdatesPerSecond(), this::updateOne);
        scheduleFlux(rates.getDeletesPerSecond(), this::deleteOne);
        scheduleFlux(rates.getReplacesPerSecond(), this::replaceOne);
        log.info("ReactiveDataGenerator started on collection '{}' — rates: {}/{}/{}/{} ops/s (i/u/d/r)",
                props.getCollection(),
                rates.getInsertsPerSecond(), rates.getUpdatesPerSecond(),
                rates.getDeletesPerSecond(), rates.getReplacesPerSecond());
    }

    public void stop() {
        disposables.dispose();
        log.info("ReactiveDataGenerator stopped — counters: inserts={} updates={} deletes={} replaces={}",
                inserts.get(), updates.get(), deletes.get(), replaces.get());
    }

    private void scheduleFlux(double ratePerSecond, java.util.function.Supplier<Mono<?>> op) {
        if (ratePerSecond <= 0) return;
        Duration period = Duration.ofNanos((long) (1_000_000_000.0 / ratePerSecond));
        Disposable d = Flux.interval(Duration.ofMillis(500), period, Schedulers.parallel())
                .flatMap(tick -> op.get().onErrorResume(e -> {
                    log.warn("data-generator op failed: {}", e.toString());
                    return Mono.empty();
                }))
                .subscribe();
        disposables.add(d);
    }

    private Mono<?> insertOne() {
        Order o = randomOrder();
        return mongoTemplate.save(o, props.getCollection())
                .doOnNext(saved -> {
                    rememberId(saved.getId());
                    inserts.incrementAndGet();
                });
    }

    private Mono<?> updateOne() {
        return pickId().flatMap(id -> {
            String newStatus = STATUSES[ThreadLocalRandom.current().nextInt(STATUSES.length)];
            return mongoTemplate.updateFirst(
                            Query.query(Criteria.where("_id").is(id)),
                            Update.update("status", newStatus),
                            props.getCollection())
                    .doOnNext(r -> updates.incrementAndGet());
        });
    }

    private Mono<?> deleteOne() {
        return pickId().flatMap(id -> mongoTemplate.remove(
                        Query.query(Criteria.where("_id").is(id)),
                        props.getCollection())
                .doOnNext(r -> {
                    forgetId(id);
                    deletes.incrementAndGet();
                }));
    }

    private Mono<?> replaceOne() {
        return pickId().flatMap(id -> {
            Order replacement = randomOrder();
            replacement.setId(id);
            return mongoTemplate.save(replacement, props.getCollection())
                    .doOnNext(saved -> replaces.incrementAndGet());
        });
    }

    private Order randomOrder() {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        String customer = CUSTOMERS[rng.nextInt(CUSTOMERS.length)];
        String status = STATUSES[rng.nextInt(STATUSES.length)];
        double total = Math.round(rng.nextDouble(5, 2000) * 100.0) / 100.0;
        Order o = new Order(customer, status, total);
        if (rng.nextDouble() < props.getInvalidPayloadRatio()) {
            o.setStatus(null);
        }
        return o;
    }

    private Mono<String> pickId() {
        if (props.getPool().getRefreshStrategy() == DataGeneratorProperties.RefreshStrategy.QUERY_EACH_TICK) {
            return queryRandomId();
        }
        if (knownIds.isEmpty()) return Mono.empty();
        int idx = ThreadLocalRandom.current().nextInt(knownIds.size());
        String id = knownIds.stream().skip(idx).findFirst().orElse(null);
        return id == null ? Mono.empty() : Mono.just(id);
    }

    private Mono<String> queryRandomId() {
        return mongoTemplate.count(new Query(), props.getCollection())
                .flatMap(count -> {
                    if (count == 0) return Mono.empty();
                    long skip = ThreadLocalRandom.current().nextLong(count);
                    return mongoTemplate.findOne(
                                    new Query().with(Sort.by("_id")).skip(skip).limit(1),
                                    Order.class,
                                    props.getCollection())
                            .map(Order::getId);
                });
    }

    private void rememberId(String id) {
        knownIds.add(id);
        if (knownIds.size() > props.getPool().getMaxSize()) {
            knownIds.remove(knownIds.iterator().next());
        }
    }

    private void forgetId(String id) {
        knownIds.remove(id);
    }
}
