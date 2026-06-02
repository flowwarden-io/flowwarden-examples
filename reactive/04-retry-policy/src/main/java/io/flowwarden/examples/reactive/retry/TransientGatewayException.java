/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.reactive.retry;

/**
 * Stand-in for a transient downstream failure — see the imperative
 * twin in {@code imperative/04-retry-policy} for the rationale.
 */
public class TransientGatewayException extends RuntimeException {

    public TransientGatewayException(String message) {
        super(message);
    }
}
