# 04 — Retry policy (reactive)

Reactive twin of `imperative/04-retry-policy`. Same `@RetryPolicy`
annotation, same backoff schedule, same custom exception — the only
difference is how the handler signals failure.

## What you'll learn

- That `@RetryPolicy` works identically in reactive mode: place it on
  the `@ChangeStream` class and the framework wraps every handler call
- That a failing reactive handler returns `Mono.error(...)` instead of
  throwing; FlowWarden's retry layer sees that signal and applies the
  configured backoff before resubscribing
- That the `retryOn`/`noRetryOn` rules are evaluated on the inner
  `Throwable` carried by the error signal — no surprises

## Annotations used

Identical to the imperative variant; only the handler return type
becomes `Mono<Void>`.

## Run it

```bash
docker compose -f docker/docker-compose.yml up -d
./mvnw -pl reactive/04-retry-policy -am spring-boot:run
```

## Key files

- `src/main/java/.../RetryHandler.java` — class-level `@RetryPolicy`, `Mono.error(...)` for failure
- `src/main/java/.../TransientGatewayException.java` — custom retryable exception
- `src/main/resources/application.yml` — 2 inserts/s on `orders-retry`
