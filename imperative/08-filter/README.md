# 08 — Filter (imperative)

`@Filter` rejects events client-side. Each event still crosses the
wire and is decoded, but the predicate decides whether the handler
sees it. Companion to `@Pipeline` (07-pipeline); the two can coexist.

## What it does

- Signature used here: `boolean keepConfirmedOnly(ChangeStreamContext<Order> ctx)`
  (also supported: `Predicate<ChangeStreamContext<?>> myFilter()` with
  no parameters)
- Predicate: keep only `status == CONFIRMED`; the data generator picks
  status uniformly from `{PENDING, CONFIRMED, CANCELLED}`, so ~1/3 of
  inserts reach the handler
- Events with no `fullDocument` (e.g. deletes without pre-image) are
  dropped by `Optional.orElse(false)`

## When to pick filter vs pipeline

- `@Pipeline` — predicate is a stable shape, predicate stable for the
  lifetime of the stream, want to save bandwidth from MongoDB
- `@Filter` — predicate depends on Spring beans, runtime config, a
  feature flag, or has to call out to other services
- Both — server-side coarse filter + client-side refinement

## Run it

```bash
docker compose -f docker/docker-compose.yml up -d
./mvnw -pl imperative/08-filter -am spring-boot:run
```

## Verify the client-side filtering

```bash
docker compose exec mongodb mongosh flowwarden-examples \
  --eval 'db["orders-filter"].countDocuments({status: {$ne: "CONFIRMED"}})'
# > 0 — these were written to Mongo, crossed the wire, but the handler
# never saw them (FilterHandler rejected them in Java)
```

## Key files

- `src/main/java/.../FilterHandler.java` — `@Filter` + `@OnInsert`
- `src/main/resources/application.yml` — 4 inserts/s on `orders-filter`
