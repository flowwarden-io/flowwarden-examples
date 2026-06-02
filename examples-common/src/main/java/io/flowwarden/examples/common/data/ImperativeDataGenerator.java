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
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

/**
 * Writes {@link Order} documents to MongoDB at independently-configurable
 * insert / update / delete / replace rates.
 *
 * <p>This is the imperative variant, used by samples in the
 * {@code imperative/} tree. The reactive equivalent lives in
 * {@link ReactiveDataGenerator}.</p>
 *
 * <p>Rates are expressed in operations per second; a rate of {@code 0}
 * disables that operation. The generator maintains a small in-memory pool
 * of known ids to drive updates / deletes / replaces; the pool is bounded
 * by {@link DataGeneratorProperties.Pool#getMaxSize()} and evicts oldest
 * entries when full.</p>
 */
public class ImperativeDataGenerator {

    private static final Logger log = LoggerFactory.getLogger(ImperativeDataGenerator.class);

    private static final String[] CUSTOMERS = {
            "alice@example.com", "bob@example.com", "charlie@example.com",
            "diana@example.com", "eve@example.com"
    };
    private static final String[] STATUSES = {"PENDING", "CONFIRMED", "CANCELLED"};

    private final MongoTemplate mongoTemplate;
    private final DataGeneratorProperties props;
    private final ScheduledExecutorService scheduler;
    private final Set<String> knownIds = new ConcurrentSkipListSet<>();
    private final AtomicLong inserts = new AtomicLong();
    private final AtomicLong updates = new AtomicLong();
    private final AtomicLong deletes = new AtomicLong();
    private final AtomicLong replaces = new AtomicLong();

    public ImperativeDataGenerator(MongoTemplate mongoTemplate, DataGeneratorProperties props) {
        this.mongoTemplate = mongoTemplate;
        this.props = props;
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "examples-datagen");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        DataGeneratorProperties.Rates rates = props.getRates();
        schedule("insert", rates.getInsertsPerSecond(), this::insertOne);
        schedule("update", rates.getUpdatesPerSecond(), this::updateOne);
        schedule("delete", rates.getDeletesPerSecond(), this::deleteOne);
        schedule("replace", rates.getReplacesPerSecond(), this::replaceOne);
        log.info("ImperativeDataGenerator started on collection '{}' — rates: {}/{}/{}/{} ops/s (i/u/d/r)",
                props.getCollection(),
                rates.getInsertsPerSecond(), rates.getUpdatesPerSecond(),
                rates.getDeletesPerSecond(), rates.getReplacesPerSecond());
    }

    public void stop() {
        scheduler.shutdownNow();
        log.info("ImperativeDataGenerator stopped — counters: inserts={} updates={} deletes={} replaces={}",
                inserts.get(), updates.get(), deletes.get(), replaces.get());
    }

    private void schedule(String label, double ratePerSecond, Runnable task) {
        if (ratePerSecond <= 0) return;
        long periodMicros = (long) (1_000_000.0 / ratePerSecond);
        long initialDelayMicros = 500_000L;
        scheduler.scheduleAtFixedRate(
                () -> safely(label, task),
                initialDelayMicros,
                periodMicros,
                TimeUnit.MICROSECONDS);
    }

    private void safely(String label, Runnable task) {
        try { task.run(); }
        catch (Exception e) { log.warn("data-generator {} failed: {}", label, e.toString()); }
    }

    private void insertOne() {
        Order o = randomOrder();
        mongoTemplate.save(o, props.getCollection());
        rememberId(o.getId());
        inserts.incrementAndGet();
    }

    private void updateOne() {
        String id = pickId();
        if (id == null) return;
        String newStatus = STATUSES[ThreadLocalRandom.current().nextInt(STATUSES.length)];
        mongoTemplate.updateFirst(
                Query.query(Criteria.where("_id").is(id)),
                Update.update("status", newStatus),
                props.getCollection());
        updates.incrementAndGet();
    }

    private void deleteOne() {
        String id = pickId();
        if (id == null) return;
        mongoTemplate.remove(
                Query.query(Criteria.where("_id").is(id)),
                props.getCollection());
        forgetId(id);
        deletes.incrementAndGet();
    }

    private void replaceOne() {
        String id = pickId();
        if (id == null) return;
        Order replacement = randomOrder();
        replacement.setId(id);
        mongoTemplate.save(replacement, props.getCollection());
        replaces.incrementAndGet();
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

    private String pickId() {
        if (props.getPool().getRefreshStrategy() == DataGeneratorProperties.RefreshStrategy.QUERY_EACH_TICK) {
            return queryRandomId();
        }
        if (knownIds.isEmpty()) return null;
        int idx = ThreadLocalRandom.current().nextInt(knownIds.size());
        return knownIds.stream().skip(idx).findFirst().orElse(null);
    }

    private String queryRandomId() {
        long count = mongoTemplate.count(new Query(), props.getCollection());
        if (count == 0) return null;
        long skip = ThreadLocalRandom.current().nextLong(count);
        Order one = mongoTemplate.findOne(
                new Query().with(Sort.by("_id")).skip(skip).limit(1),
                Order.class,
                props.getCollection());
        return one == null ? null : one.getId();
    }

    private void rememberId(String id) {
        knownIds.add(id);
        if (knownIds.size() > props.getPool().getMaxSize()) {
            knownIds.iterator().next(); // O(1) head peek + remove not provided, accept overshoot
            knownIds.remove(knownIds.iterator().next());
        }
    }

    private void forgetId(String id) {
        knownIds.remove(id);
    }
}
