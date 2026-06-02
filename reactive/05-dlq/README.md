# 05 — Dead Letter Queue (reactive)

Reactive twin of `imperative/05-dlq`. Same `@DeadLetterQueue` +
`@RetryPolicy` combination, same DLQ collection — the handler signals
its failure via `Mono.error(...)` and the REST endpoint is a WebFlux
`Flux` controller.

## What you'll learn

- That `@DeadLetterQueue` works identically in reactive mode — there's
  no `Mono`-aware variant
- That a `Mono.error(PaymentRejectedException)` from the handler is
  treated by FlowWarden the same way as a thrown exception in
  imperative mode: retries first, then DLQ archive

## Run it

```bash
docker compose -f docker/docker-compose.yml up -d
./mvnw -pl reactive/05-dlq -am spring-boot:run
```

Then browse to `http://localhost:<random-port>/dlq` for a live Flux of
the DLQ collection (Netty port is in the startup log).

## Key files

- `src/main/java/.../DlqHandler.java` — `@RetryPolicy` + `@DeadLetterQueue`, `Mono<Void>` handler
- `src/main/java/.../DlqController.java` — `GET /dlq` returning `Flux<Map>`
- `src/main/resources/application.yml` — 3 inserts/s on `orders-dlq`
