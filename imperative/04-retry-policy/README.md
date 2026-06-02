# 04 — Retry policy (imperative)

`@RetryPolicy` retries a failing handler with exponential backoff and
jitter, scoped to a specific exception type.

## What you'll learn

- How to declare a retry policy on a `@ChangeStream` class
- Why `retryOn` is usually narrower than "all exceptions": a dedicated
  exception type lets you retry transient downstream failures without
  also retrying programmer errors (validation, mapping, ...)
- That the default `noRetryOn` list (`IllegalArgumentException`,
  `NullPointerException`, `ClassCastException`) prevents fast-failing
  bugs from looping needlessly
- How jitter spreads concurrent retries so they don't synchronise into
  a thundering herd

## Annotations used

- `@RetryPolicy(maxAttempts = 3, initialDelay = "500ms", maxDelay = "5s", multiplier = 2.0, retryOn = TransientGatewayException.class, jitter = true)`
- `@ChangeStream(collection = "orders-retry", documentType = Order.class)`
- `@OnInsert` — throws `TransientGatewayException` when `total > 1500`

## How the failures are produced

`Order.total` is drawn uniformly from `[5, 2000]` by the shared data
generator, so roughly one insert in four crosses the `1500` threshold
and triggers `TransientGatewayException` — a custom exception type
defined in this sample to stand in for a flaky downstream call.

The same predicate is re-evaluated on every retry, so the failure is
**deterministic**: every retry will throw again, and after
`maxAttempts = 3` FlowWarden gives up. Hook in `@DeadLetterQueue` (see
`05-dlq`) or `@OnError` (see `03-error-handling`) to control what
happens after the last attempt.

## Run it

```bash
docker compose -f docker/docker-compose.yml up -d
./mvnw -pl imperative/04-retry-policy -am spring-boot:run
```

## Expected logs

The handler tracks attempts **per order**, so the retry shows up as the
same id reappearing with an incrementing counter:

```
ImperativeDataGenerator started on collection 'orders-retry' — rates: 2.0/0.0/0.0/0.0
[retry] order 06d5d0f5 attempt #1 → throw (total=1731.40)
[retry] order 06d5d0f5 attempt #2 → throw (total=1731.40)   (~500 ms later)
[retry] order 06d5d0f5 attempt #3 → throw (total=1731.40)   (~1 s later)
... (FlowWarden gives up after maxAttempts=3, logs error, moves on)
[retry] order f9cbca42 attempt #1 → ✓ Order{id=..., total=42.18}
```

The exact backoff timing varies (jitter is ±20%), but a failing order
will appear with attempt counts `#1 → #2 → #3` and the smoke test
asserts exactly that — `maxAttemptsForAnyOrder >= 2` is the only
condition strong enough to prove the framework actually retried, not
just observed one throw.

## Key files

- `src/main/java/.../RetryHandler.java` — class-level `@RetryPolicy`
- `src/main/java/.../TransientGatewayException.java` — custom retryable exception
- `src/main/resources/application.yml` — 2 inserts/s on `orders-retry`
