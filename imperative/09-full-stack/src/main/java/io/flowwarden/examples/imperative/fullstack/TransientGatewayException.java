/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.flowwarden.examples.imperative.fullstack;

/**
 * Stand-in for a transient downstream failure. Same idea as
 * {@code 04-retry-policy} — redeclared here so this sample stays
 * self-contained.
 */
public class TransientGatewayException extends RuntimeException {

    public TransientGatewayException(String message) {
        super(message);
    }
}
