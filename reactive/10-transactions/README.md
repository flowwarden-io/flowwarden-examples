# 10 — Transactions (reactive)

Reactive twin of `imperative/10-transactions`. Same handler contract —
`ChangeStreamContext.getTransactionInfo()` — driven from a
`TransactionalOperator` instead of `TransactionTemplate`.

## What changes vs imperative

- `@OnInsert` returns `Mono<Void>` and wraps the counter increments in
  `Mono.fromRunnable(...)`
- `TransactionalGenerator` composes the batch as a
  `Flux<Order>` and applies `transactionalOperator::transactional`,
  then `blockLast()` (the `@Scheduled` worker is intentionally sync)
- `ReactiveMongoTransactionConfig` registers a
  `ReactiveMongoTransactionManager` and a derived `TransactionalOperator`

## Run it

```bash
docker compose -f docker/docker-compose.yml up -d
./mvnw -pl reactive/10-transactions -am spring-boot:run
```

See the imperative README for the expected log shape and the
explanation of the `txnNumber` grouping.

## Key files

- `src/main/java/.../TransactionHandler.java` — `Mono<Void>` handler
- `src/main/java/.../TransactionalGenerator.java` — `Flux` + `TransactionalOperator`
- `src/main/java/.../ReactiveMongoTransactionConfig.java` — manager + operator
- `src/main/resources/application.yml`
