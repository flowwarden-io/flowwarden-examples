/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.common.model;

import java.time.Instant;
import java.util.UUID;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Single shared POJO used across all examples.
 *
 * <p>The choice of an "order" domain is arbitrary: it provides natural
 * fields to demonstrate filtering ({@code status}), pipelines
 * ({@code total}), retries (lifecycle transitions) and DLQ (invalid
 * payloads). All examples use this same shape so readers don't have to
 * re-learn a new model when jumping between samples.</p>
 *
 * <p>The collection name is sample-specific (e.g. {@code orders-hello},
 * {@code orders-typed}) so several examples can run in parallel without
 * stepping on each other. Samples that write through {@code MongoTemplate}
 * pass the collection explicitly, so the {@link Document#collection()}
 * default below is only used by samples that rely on Spring Data
 * repositories (e.g. {@code 11-javers}).</p>
 */
@Document(collection = "orders-javers")
public class Order {

    @Id
    private String id;

    private String customer;
    private String status;
    private double total;
    private Instant createdAt;

    public Order() {}

    public Order(String customer, String status, double total) {
        this.id = UUID.randomUUID().toString();
        this.customer = customer;
        this.status = status;
        this.total = total;
        this.createdAt = Instant.now();
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getCustomer() { return customer; }
    public void setCustomer(String customer) { this.customer = customer; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public double getTotal() { return total; }
    public void setTotal(double total) { this.total = total; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    @Override
    public String toString() {
        return "Order{id=" + id + ", customer=" + customer
                + ", status=" + status + ", total=" + total + "}";
    }
}
