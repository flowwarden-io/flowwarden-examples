/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.imperative.retry;

/**
 * Stand-in for a transient downstream failure — the kind of error worth
 * retrying because it is likely to clear on its own (network timeout,
 * 503 from an upstream API, brief broker hiccup, ...).
 *
 * <p>Choosing a dedicated exception type is what lets the
 * {@code @RetryPolicy(retryOn = TransientGatewayException.class)} target
 * exactly the failures you want to retry, while letting permanent
 * errors (validation, mapping) fail fast.</p>
 */
public class TransientGatewayException extends RuntimeException {

    public TransientGatewayException(String message) {
        super(message);
    }
}
