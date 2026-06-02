# 03 — Error handling (reactive)

Reactive twin of `imperative/03-error-handling`. The handler signals an
error by returning `Mono.error(...)` — FlowWarden routes it to the
matching `@OnError` method exactly as if a thrown exception had bubbled
up in imperative mode.

## What you'll learn

- That in reactive mode, errors propagate via `Mono.error(...)` instead
  of thrown exceptions, but the `@OnError` routing rule is the same
- That `@OnError` methods still return `ErrorAction` **synchronously**
  in reactive mode — they are not themselves `Mono<ErrorAction>`
- The exception-type lookup order (exact → super-type → catch-all)

## Annotations used

Same as the imperative variant — `@OnInsert` returning `Mono<Void>`,
typed `@OnError(IllegalArgumentException.class)`, catch-all `@OnError`.

## Run it

```bash
docker compose -f docker/docker-compose.yml up -d
./mvnw -pl reactive/03-error-handling -am spring-boot:run
```

> **Note on the catch-all `@OnError`** — it never fires in this sample;
> see the imperative README for the full explanation.

## Key files

- `src/main/java/.../ErrorHandler.java` — reactive validating handler + two `@OnError` methods
- `src/main/resources/application.yml` — sets `invalid-payload-ratio: 0.3`
