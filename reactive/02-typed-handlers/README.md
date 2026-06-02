# 02 — Typed handlers (reactive)

Reactive twin of `imperative/02-typed-handlers` — same five typed
callbacks, each returning `Mono<Void>`.

## What you'll learn

- That every typed annotation works the same way in reactive mode: the
  return type just becomes `Mono<Void>`
- That `Mono.fromRunnable` is the idiomatic way to wrap pure
  side-effects (logging, counters, in-memory state) inside the reactive
  pipeline
- That a real reactive handler would chain async work (Mongo writes,
  HTTP calls) and return the resulting publisher to backpressure-aware
  the upstream subscriber

## Annotations used

Same five as the imperative variant — `@OnInsert`, `@OnUpdate`,
`@OnDelete`, `@OnReplace`, `@OnChange` — plus `@ChangeStream`,
`@EnableFlowWarden`.

## Run it

```bash
docker compose -f docker/docker-compose.yml up -d
./mvnw -pl reactive/02-typed-handlers -am spring-boot:run
```

> **Note on `@OnChange`** — it is a catch-all and FlowWarden only routes
> an event to it when no specific `@OnXxx` method on the class matches.
> In this sample the four typed handlers cover every operation type, so
> `@OnChange` is never called and its counter stays at zero. Comment
> out one of the specific methods (say `deleted`) and re-run — deletes
> will then land in `any()`.

## Key files

- `src/main/java/.../OrderHandler.java` — five `Mono<Void>` methods
- `src/main/resources/application.yml` — generator wired for all four ops
