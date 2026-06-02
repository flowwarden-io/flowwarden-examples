/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.imperative.dlq;

/**
 * A failure that is genuinely persistent — retries won't help.
 *
 * <p>In this sample it stands in for "the downstream payment processor
 * keeps rejecting this transaction": a few retries are still attempted
 * (to be tolerant of transient flakes), and once exhausted the event
 * lands in the DLQ for an operator to triage.</p>
 */
public class PaymentRejectedException extends RuntimeException {

    public PaymentRejectedException(String message) {
        super(message);
    }
}
