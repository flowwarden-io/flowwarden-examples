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
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Drives Javers audit events by writing to the audited
 * {@link OrderRepository}.
 *
 * <p>The shared {@code ImperativeDataGenerator} writes via
 * {@code MongoTemplate}, which bypasses Spring Data repositories and
 * therefore <strong>doesn't</strong> trigger Javers audit. To produce
 * snapshots — the events the {@link OrderAuditHandler} consumes — we
 * have to go through the repository. This writer schedules
 * {@code save} / {@code delete} calls on it.</p>
 *
 * <p>Insert: 1/s. Update: 1/2s (rewrites the status). Delete: 1/5s.
 * Together they exercise {@code @OnInitial}, {@code @OnUpdate} and
 * {@code @OnTerminal}.</p>
 */
@Component
public class AuditedOrderWriter {

    private static final Logger log = LoggerFactory.getLogger(AuditedOrderWriter.class);

    private static final String[] CUSTOMERS = {
            "alice@example.com", "bob@example.com", "charlie@example.com",
            "diana@example.com", "eve@example.com"
    };
    private static final String[] STATUSES = {"PENDING", "CONFIRMED", "CANCELLED"};

    private final OrderRepository repository;
    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(1, r -> {
                Thread t = new Thread(r, "audited-order-writer");
                t.setDaemon(true);
                return t;
            });

    public AuditedOrderWriter(OrderRepository repository) {
        this.repository = repository;
    }

    @PostConstruct
    void start() {
        scheduler.scheduleAtFixedRate(this::safeInsert, 1, 1, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(this::safeUpdate, 2, 2, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(this::safeDelete, 5, 5, TimeUnit.SECONDS);
        log.info("AuditedOrderWriter started — 1 insert/s, 1 update/2s, 1 delete/5s");
    }

    @PreDestroy
    void stop() {
        scheduler.shutdownNow();
    }

    private void safeInsert() { run("insert", this::insertOne); }
    private void safeUpdate() { run("update", this::updateOne); }
    private void safeDelete() { run("delete", this::deleteOne); }

    private void run(String label, Runnable task) {
        try { task.run(); }
        catch (Exception e) { log.warn("audited-writer {} failed: {}", label, e.toString()); }
    }

    private void insertOne() {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        Order o = new Order(
                CUSTOMERS[rng.nextInt(CUSTOMERS.length)],
                STATUSES[rng.nextInt(STATUSES.length)],
                Math.round(rng.nextDouble(5, 2000) * 100.0) / 100.0);
        repository.save(o);
    }

    private void updateOne() {
        pickRandom().ifPresent(o -> {
            String next = STATUSES[ThreadLocalRandom.current().nextInt(STATUSES.length)];
            if (!next.equals(o.getStatus())) {
                o.setStatus(next);
                repository.save(o);
            }
        });
    }

    private void deleteOne() {
        pickRandom().ifPresent(o -> repository.deleteById(o.getId()));
    }

    private Optional<Order> pickRandom() {
        long count = repository.count();
        if (count == 0) return Optional.empty();
        long skip = ThreadLocalRandom.current().nextLong(count);
        return repository.findAll().stream().skip(skip).findFirst();
    }
}
