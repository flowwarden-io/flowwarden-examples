# 05 — Dead Letter Queue (imperative)

`@DeadLetterQueue` paired with `@RetryPolicy`: events that fail every
retry attempt are routed to a dedicated DLQ collection instead of being
silently lost.

## What you'll learn

- How to declare a DLQ on a `@ChangeStream` class, and how it composes
  with `@RetryPolicy` (retries first, DLQ after exhaustion)
- The split between **policy** (`@DeadLetterQueue` — `retentionDays`,
  what to include) and **routing** (`@MongoDlqOptions` — Mongo
  collection name). Policy is backend-agnostic; routing is per-backend
- What FlowWarden actually stores in the DLQ collection (the original
  document, the exception message, optionally the full stack trace)
- That `@DeadLetterQueue` also works standalone — without
  `@RetryPolicy` the event lands in the DLQ on the very first failure
- A practical pattern for inspecting the DLQ from a browser via a tiny
  REST endpoint (`GET /dlq`)

## Annotations used

- `@ChangeStream(collection = "orders-dlq", documentType = Order.class)`
- `@RetryPolicy(maxAttempts = 2, initialDelay = "300ms", maxDelay = "2s", retryOn = PaymentRejectedException.class)`
- `@DeadLetterQueue(retentionDays = 7, includeOriginalDocument = true, includeStackTrace = true)`
- `@MongoDlqOptions(collection = "orders-dlq-failed")` — companion
  annotation that routes this stream's failures to a custom Mongo
  collection. Without it, failures land in the default
  `flowwarden.dlq.mongo.collection` (which itself defaults to `_fw_dlq`)
- `@OnInsert`

## How the failures are produced

Orders with `total > 1500` are rejected deterministically by the
handler (simulating a stuck downstream payment processor). Each such
order is retried twice (per `maxAttempts = 2`) and then sent to the DLQ.

## Run it

```bash
docker compose -f docker/docker-compose.yml up -d
./mvnw -pl imperative/05-dlq -am spring-boot:run
```

Once a failure has cycled through, browse to:

```
GET http://localhost:<random-port>/dlq
```

(the random port is in the startup log; `Tomcat started on port(s): XXXXX`)

## Expected logs

```
ImperativeDataGenerator started on collection 'orders-dlq' — rates: 3.0/0.0/0.0/0.0
[dlq] ✓ Order{id=..., total=42.18}
[dlq] attempt #2 → reject big order ab12cd34 (total=1731.40)
[dlq] attempt #3 → reject big order ab12cd34 (total=1731.40)   (after ~300 ms)
... FlowWarden writes one DLQ entry for ab12cd34, then moves on
[dlq] ✓ Order{id=..., total=137.50}
```

```bash
curl http://localhost:<port>/dlq | jq '.[0]'
# returns the DLQ document from `orders-dlq-failed`
```

## Key files

- `src/main/java/.../DlqHandler.java` — `@RetryPolicy` + `@DeadLetterQueue` + `@MongoDlqOptions`
- `src/main/java/.../PaymentRejectedException.java` — custom non-transient failure
- `src/main/java/.../DlqController.java` — `GET /dlq` to browse the collection
- `src/main/resources/application.yml` — 3 inserts/s on `orders-dlq`
