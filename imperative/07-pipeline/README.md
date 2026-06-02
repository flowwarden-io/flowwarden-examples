# 07 — Pipeline (imperative)

`@Pipeline` declares a server-side aggregation. MongoDB applies it
inside the oplog cursor; only matching events cross the network.

## What it does

- `@Pipeline` returns a `List<AggregationOperation>` (or `List<Bson>`,
  or `Aggregation`) that's installed once at stream startup
- Predicate here: `fullDocument.total > 1000` — the watched document
  lives under `fullDocument` in the change-stream event envelope
- Generator emits totals uniformly in `[5, 2000]`, so ~50% are
  silently dropped before they reach the handler

Compare with `08-filter`, which applies the same predicate client-side
(every event still hits the wire).

## Run it

```bash
docker compose -f docker/docker-compose.yml up -d
./mvnw -pl imperative/07-pipeline -am spring-boot:run
```

## Verify the server-side filter

```bash
# Logs show only orders with total > 1000 reaching the handler
docker compose exec mongodb mongosh flowwarden-examples \
  --eval 'db["orders-pipeline"].countDocuments({total: {$lte: 1000}})'
# > 0 — these were written to Mongo but never seen by the handler
```

## Key files

- `src/main/java/.../PipelineHandler.java` — `@Pipeline` + `@OnInsert`
- `src/main/resources/application.yml` — 4 inserts/s on `orders-pipeline`
