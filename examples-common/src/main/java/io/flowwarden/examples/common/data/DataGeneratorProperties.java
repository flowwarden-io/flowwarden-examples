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

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Tunes how fast the {@link DataGenerator} writes against MongoDB.
 *
 * <p>Each sample overrides what it needs in its {@code application.yml}.
 * Rates of {@code 0} disable that operation; the kill switch
 * {@link #isEnabled()} stops the generator entirely.</p>
 *
 * <p>Mapped under prefix {@code examples.data-generator}.</p>
 */
@ConfigurationProperties(prefix = "examples.data-generator")
public class DataGeneratorProperties {

    private boolean enabled = true;
    private Mode mode = Mode.IMPERATIVE;
    private String collection = "orders";
    private Rates rates = new Rates();
    private Pool pool = new Pool();
    private double invalidPayloadRatio = 0.0;

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public Mode getMode() { return mode; }
    public void setMode(Mode mode) { this.mode = mode; }

    public String getCollection() { return collection; }
    public void setCollection(String collection) { this.collection = collection; }

    public Rates getRates() { return rates; }
    public void setRates(Rates rates) { this.rates = rates; }

    public Pool getPool() { return pool; }
    public void setPool(Pool pool) { this.pool = pool; }

    public double getInvalidPayloadRatio() { return invalidPayloadRatio; }
    public void setInvalidPayloadRatio(double invalidPayloadRatio) {
        this.invalidPayloadRatio = invalidPayloadRatio;
    }

    public static class Rates {
        private double insertsPerSecond = 1.0;
        private double updatesPerSecond = 0.0;
        private double deletesPerSecond = 0.0;
        private double replacesPerSecond = 0.0;

        public double getInsertsPerSecond() { return insertsPerSecond; }
        public void setInsertsPerSecond(double v) { this.insertsPerSecond = v; }
        public double getUpdatesPerSecond() { return updatesPerSecond; }
        public void setUpdatesPerSecond(double v) { this.updatesPerSecond = v; }
        public double getDeletesPerSecond() { return deletesPerSecond; }
        public void setDeletesPerSecond(double v) { this.deletesPerSecond = v; }
        public double getReplacesPerSecond() { return replacesPerSecond; }
        public void setReplacesPerSecond(double v) { this.replacesPerSecond = v; }
    }

    public static class Pool {
        /** Maximum number of known ids kept in memory for update/delete/replace. */
        private int maxSize = 1000;
        private RefreshStrategy refreshStrategy = RefreshStrategy.IN_MEMORY;

        public int getMaxSize() { return maxSize; }
        public void setMaxSize(int maxSize) { this.maxSize = maxSize; }
        public RefreshStrategy getRefreshStrategy() { return refreshStrategy; }
        public void setRefreshStrategy(RefreshStrategy s) { this.refreshStrategy = s; }
    }

    /** Which template the generator should write through. */
    public enum Mode { IMPERATIVE, REACTIVE }

    public enum RefreshStrategy {
        /** Track ids of generated documents in memory (fastest, lost on restart). */
        IN_MEMORY,
        /** Query a random document from MongoDB on each tick (slower, survives restart). */
        QUERY_EACH_TICK
    }
}
