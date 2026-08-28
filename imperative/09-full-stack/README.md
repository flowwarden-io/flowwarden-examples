# 09 — Full stack (imperative)

The showcase sample. Combines every major FlowWarden annotation in one
handler and demonstrates the **dual-token model** — FlowWarden's
differentiator over a raw MongoDB change stream.

## Why this sample exists

A raw change stream gives you a single resume token that only advances
when your handler returns successfully. If 99% of events are dropped by
your business filter, that token stays stale: the oplog rolls over,
your stream cannot recover, and you crash on restart — even though
nothing was lost.

FlowWarden separates two tokens:

| Token | Advances when… | Persists what |
|---|---|---|
| `lastSeenToken` | every event received (incl. filtered, retried) — via heartbeat | resume safety |
| `lastProcessedToken` | every handler success (incl. DLQ ack) — via `saveEveryN` | at-least-once delivery |

When `@Filter` rejects an event, the wire delivered it → `lastSeen`
advances → restart is safe. The handler never ran → `lastProcessed`
stays put → no delivery semantic is broken.

## Pipeline diagram

```
MongoDB oplog
   │
   ▼  @Pipeline          → server-side match. Filtered events never
   │                       leave Mongo. Neither token advances.
   ▼  driver → app
   │
   ▼  @Filter             → client-side decision. Rejected events
   │                       cross the wire, so lastSeen advances via
   │                       heartbeat. lastProcessed does NOT.
   ▼  @RetryPolicy        → temporary divergence while retries spin.
   │
   ▼  @OnInsert / @OnUpdate / @OnDelete / @OnReplace
   │                      → success → lastProcessed advances.
   ▼  @DeadLetterQueue    → retry exhaustion → DLQ write acks the
                            event, lastProcessed advances.
```

## Annotations used

- `@ChangeStream(collection = "orders-full", documentType = Order.class, fullDocument = UPDATE_LOOKUP)`
- `@Pipeline` — `match(fullDocument.total > 50)` (server-side cut-off)
- `@Filter` — keep `status == "CONFIRMED"` (client-side business rule)
- `@OnInsert` / `@OnUpdate` / `@OnReplace` — typed handlers
- `@RetryPolicy(maxAttempts = 3, retryOn = TransientGatewayException.class, jitter = true)`
- `@DeadLetterQueue(retentionDays = 14, includeStackTrace = true)`
- `@MongoDlqOptions(collection = "orders-full-dlq")`
- `@Checkpoint(saveEveryN = 1, saveIntervalSeconds = 2, startPosition = RESUME, ...)`

`saveIntervalSeconds = 2` is intentionally short so the divergence is
visible within a few seconds.

> **No `@OnDelete`** — the lib statically refuses `@Filter` +
> `@OnDelete` on the same class because delete events carry no
> `fullDocument` for the filter predicate to inspect. Recommended
> workarounds: a server-side `@Pipeline` stage that excludes deletes,
> or inlining the filter logic in `@OnDelete`. This sample sidesteps
> by setting `deletes-per-second: 0` in the generator.

## Run it

```bash
docker compose -f docker/docker-compose.yml up -d
./mvnw -pl imperative/09-full-stack -am spring-boot:run
```

Then in another terminal:

```bash
# Periodic summary in the logs every 5s
# [fullstack/metrics] handled=12 filter-rejected=28 retry-attempts=4 dlq=1 — lastSeen ahead of lastProcessed by 1168 ms

# Or query the JSON metrics endpoint
curl http://localhost:<port>/metrics | jq
```

`/metrics` shape (real values from the smoke test, with the lib's
`saveCheckpointIfNeeded` fix applied):

```json
{
  "stream": "full-stack-handler",
  "checkpoint": {
    "lastSeenTimestamp": "2026-06-02T23:31:40.245Z",
    "lastProcessedTimestamp": "2026-06-02T23:31:39.077Z",
    "divergenceMillis": 1168,
    "lastSeenToken": { "_data": "..." },
    "lastProcessedToken": { "_data": "..." }
  },
  "counters": {
    "handled_insert": 14,
    "handled_update": 6,
    "handled_replace": 1,
    "client_rejected_by_filter": 47,
    "transient_failure_attempts": 9,
    "dlq_collection_size": 2
  }
}
```

The point is `divergenceMillis`: 1168 ms of events `lastSeenToken`
advanced over without `lastProcessedToken` following. Without
FlowWarden's dual-token model, the resume-on-restart safety would
collapse to whatever your handler
last touched — i.e. far behind the actual oplog position.

## Key files

- `src/main/java/.../FullStackHandler.java` — all annotations composed
- `src/main/java/.../TransientGatewayException.java` — custom retryable
- `src/main/java/.../MetricsController.java` — `GET /metrics`
- `src/main/java/.../MetricsLogger.java` — periodic INFO log
- `src/main/resources/application.yml` — 5/2/1/1 generator on `orders-full`
