# 03 — Error handling (imperative)

Two `@OnError` methods on the same `@ChangeStream` — one typed on a
specific exception, one catch-all — both returning an `ErrorAction` to
control what happens next.

## What you'll learn

- The supported `@OnError` signature: `ErrorAction handle(Throwable ex, ChangeStreamContext<?> ctx)`
- That multiple `@OnError` methods are allowed: FlowWarden picks the
  most specific match (exact type → super-type → catch-all)
- The four possible `ErrorAction` values — `SKIP`, `RETRY`, `DLQ`,
  `RETHROW` — and when each one is appropriate (this sample uses `SKIP`;
  see samples `04-retry-policy` and `05-dlq` for the others)

## Annotations used

- `@EnableFlowWarden` on `ErrorsApplication`
- `@ChangeStream(collection = "orders-errors", documentType = Order.class)`
- `@OnInsert` — throws `IllegalArgumentException` when `status` is null
- `@OnError(IllegalArgumentException.class)` — typed catch
- `@OnError` — catch-all (never fires here, by construction)

## How the failures are produced

The shared data generator has an `invalid-payload-ratio` knob:

```yaml
examples:
  data-generator:
    invalid-payload-ratio: 0.3   # ≈30% of inserts arrive with status=null
```

A `null` status inside `onInsert` throws `IllegalArgumentException`, which
FlowWarden routes to the typed `@OnError(IllegalArgumentException.class)`
method. The catch-all stays untouched.

## Run it

```bash
docker compose -f docker/docker-compose.yml up -d
./mvnw -pl imperative/03-error-handling -am spring-boot:run
```

## Expected logs

```
ImperativeDataGenerator started on collection 'orders-errors' — rates: 3.0/0.0/0.0/0.0
[errors] ACCEPT Order{id=..., status=CONFIRMED, total=42.18}
[errors] ACCEPT Order{id=..., status=PENDING, total=137.50}
[errors] SKIP validation error: Order.status cannot be null
[errors] ACCEPT Order{id=..., status=CANCELLED, total=812.50}
[errors] SKIP validation error: Order.status cannot be null
```

> **Note on the catch-all `@OnError`** — it never fires in this sample
> because the only exception thrown is `IllegalArgumentException`, which
> matches the typed handler first. Change `onInsert` to throw a
> `RuntimeException` (or anything not assignable to IAE) and the
> catch-all will engage.

## Key files

- `src/main/java/.../ErrorHandler.java` — the validating handler + two `@OnError` methods
- `src/main/resources/application.yml` — sets `invalid-payload-ratio: 0.3`
