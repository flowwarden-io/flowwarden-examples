# 10 — Transactions (imperative)

`ChangeStreamContext.getTransactionInfo()` exposes the
`lsid` + `txnNumber` MongoDB attaches to every event emitted inside a
transaction. Handlers can use them to **group events that belong to
the same transaction** — the canonical use cases are audit trails,
aggregation, and atomicity-preserving downstream propagation.

Events emitted outside a transaction return `Optional.empty()`.

## What it does

- `TransactionHandler` reads `ctx.getTransactionInfo()` on every
  `@OnInsert` and routes it to one of two counters
  (`txnEvents` vs `standaloneEvents`) plus a `Map<Long, AtomicLong>`
  that buckets events by `txnNumber`
- `TransactionalGenerator` (`@Scheduled`) emits a steady mix:
  3 inserts every 2 s inside a transaction, plus 1 insert every 1 s
  outside any transaction
- `MongoTransactionConfig` registers the `MongoTransactionManager`
  Spring Boot doesn't auto-configure, so `@Transactional` and
  `TransactionTemplate` work against MongoDB sessions
- `@EnableTransactionManagement` on the application class wires it up

## Annotations used

- `@EnableFlowWarden`, `@EnableTransactionManagement`, `@EnableScheduling`
- `@ChangeStream(collection = "orders-transactions", documentType = Order.class)`
- `@OnInsert void onInsert(Order order, ChangeStreamContext<Order> ctx)` —
  the typed-POJO + context signature

## Run it

```bash
docker compose -f docker/docker-compose.yml up -d
./mvnw -pl imperative/10-transactions -am spring-boot:run
```

## Expected logs

```
TransactionalGenerator starts emitting at t=1s

[standalone] INSERT Order{...alice...status=PENDING...total=42.18}
[txn] INSERT Order{...bob...total=100.0} — txnNumber=7 group-size=1
[txn] INSERT Order{...bob...total=101.0} — txnNumber=7 group-size=2
[txn] INSERT Order{...bob...total=102.0} — txnNumber=7 group-size=3
[standalone] INSERT Order{...diana...status=CONFIRMED...total=812.5}
[txn] INSERT Order{...charlie...total=33.0} — txnNumber=8 group-size=1
[txn] INSERT Order{...charlie...total=34.0} — txnNumber=8 group-size=2
[txn] INSERT Order{...charlie...total=35.0} — txnNumber=8 group-size=3
```

The pattern is the headline: each transactional batch lands as three
events sharing the same `txnNumber`, with the in-memory group size
incrementing from 1 to 3. Standalone inserts are interleaved on their
own log line.

## Key files

- `src/main/java/.../TransactionHandler.java` — `getTransactionInfo()` routing
- `src/main/java/.../TransactionalGenerator.java` — `@Scheduled` mix of transactional + standalone inserts
- `src/main/java/.../MongoTransactionConfig.java` — registers `MongoTransactionManager`
- `src/main/java/.../TransactionApplication.java` — `@EnableTransactionManagement`
- `src/main/resources/application.yml` — silences the shared generator, enables this one
