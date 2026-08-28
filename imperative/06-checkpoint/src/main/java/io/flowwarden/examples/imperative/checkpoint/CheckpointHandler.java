/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.imperative.checkpoint;

import io.flowwarden.examples.common.model.Order;
import io.flowwarden.stream.OnHistoryLost;
import io.flowwarden.stream.StartPosition;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.Checkpoint;
import io.flowwarden.stream.annotation.OnInsert;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Why this sample exists: prove that resume tokens survive a restart.
 *
 * <p>With {@code @Checkpoint} on, FlowWarden writes the per-stream
 * resume token to MongoDB ({@code _fw_checkpoints}, keyed by
 * {@code streamName}). On restart, {@code StartPosition.RESUME}
 * replays from that token — no events skipped, no events lost (modulo
 * the dual-token cascade detailed on the annotation itself).</p>
 *
 * <p>{@code lastProcessedToken} is persisted by a count-or-time policy,
 * whichever threshold is reached first: {@code saveEveryN = 1} writes at
 * every terminally settled event, {@code saveIntervalSeconds = 3} bounds
 * the age of a dirty anchor in time (a clean anchor is never rewritten).
 * Keeping an <em>idle</em> stream recoverable is the separate job of
 * {@code idleHeartbeatIntervalSeconds} — the sole writer of
 * {@code lastSeenToken}: when the cursor has settled nothing for that
 * long, a bounded server probe certifies the interval empty and persists
 * the certified position, so an idle stream never loses its resume point
 * to oplog rollover. It is on by default (300s); the sample pins a short
 * value so the effect is visible — set {@code 0} to opt out (and
 * re-expose the stream to rollover). On restart the cascade is fixed:
 * the processed anchor first (strict at-least-once), the certified seen
 * position as the safety net, then {@code onHistoryLost}.
 * {@code RESUME_FROM_NOW} is a pragmatic fallback when the oplog has
 * rolled over both anchors anyway (rather than {@code FAIL}, which would
 * refuse to start).</p>
 *
 * <p>Restart demo (manual): run the app, watch a few inserts arrive,
 * Ctrl-C, then re-run. The handler picks up where it left off rather
 * than from "now".</p>
 */
@ChangeStream(collection = "orders-checkpoint", documentType = Order.class)
@Checkpoint(
        saveEveryN = 1,
        saveIntervalSeconds = 3,
        idleHeartbeatIntervalSeconds = 30,
        startPosition = StartPosition.RESUME,
        onHistoryLost = OnHistoryLost.RESUME_FROM_NOW
)
public class CheckpointHandler {

    private static final Logger log = LoggerFactory.getLogger(CheckpointHandler.class);

    private final AtomicLong received = new AtomicLong();

    @OnInsert
    void onInsert(Order order) {
        long n = received.incrementAndGet();
        log.info("[checkpoint] #{} {}", n, order);
    }

    public long getReceived() { return received.get(); }
}
