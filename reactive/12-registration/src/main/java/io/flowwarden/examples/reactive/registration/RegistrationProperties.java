/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.reactive.registration;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * The "outside the JVM" source of truth for this example: stream
 * definitions live in {@code application.yml} under
 * {@code examples.registration.streams}, not in annotated classes.
 *
 * <p>Swap this for a database table, a feature-flag service, or a
 * remote config server — the contributor doesn't care where the
 * entries come from, only that they are available at bootstrap.</p>
 */
@ConfigurationProperties(prefix = "examples.registration")
public class RegistrationProperties {

    private List<StreamEntry> streams = new ArrayList<>();

    public List<StreamEntry> getStreams() { return streams; }
    public void setStreams(List<StreamEntry> streams) { this.streams = streams; }

    public static class StreamEntry {

        /** Stream name — must be unique across annotated and contributed streams. */
        private String name;

        /** Source collection. */
        private String collection;

        /** Server-side: only forward inserts (a {@code $match} pipeline stage). */
        private boolean insertsOnly = true;

        /** Client-side: keep only orders with this status; {@code null} disables the filter. */
        private String keepStatus;

        /** Attach a Dead Letter Queue with the annotation defaults. */
        private boolean dlq = false;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public String getCollection() { return collection; }
        public void setCollection(String collection) { this.collection = collection; }

        public boolean isInsertsOnly() { return insertsOnly; }
        public void setInsertsOnly(boolean insertsOnly) { this.insertsOnly = insertsOnly; }

        public String getKeepStatus() { return keepStatus; }
        public void setKeepStatus(String keepStatus) { this.keepStatus = keepStatus; }

        public boolean isDlq() { return dlq; }
        public void setDlq(boolean dlq) { this.dlq = dlq; }
    }
}
