# 08 — Filter (reactive)

Reactive twin of `imperative/08-filter`.

The `@Filter` method **stays synchronous** in reactive mode — the lib
does not offer a `Mono<Boolean>` signature. Only the handler
(`@OnInsert`) returns `Mono<Void>`. If your predicate depends on async
state, cache it in a field and refresh it out-of-band.

## Run it

```bash
docker compose -f docker/docker-compose.yml up -d
./mvnw -pl reactive/08-filter -am spring-boot:run
```

See the imperative README for the trade-off discussion vs `@Pipeline`.

## Key files

- `src/main/java/.../FilterHandler.java`
- `src/main/resources/application.yml`
