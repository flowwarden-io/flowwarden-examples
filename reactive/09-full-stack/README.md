# 09 — Full stack (reactive)

Reactive twin of `imperative/09-full-stack`. Same composition, same
diagram, same dual-token story.

What changes:
- Typed handlers return `Mono<Void>` instead of `void`
- The transient failure is signalled via `Mono.error(...)` instead of
  a thrown exception
- `MetricsController` returns `Mono<Map<String, Object>>` (composes
  naturally with the WebFlux pipeline)
- `@Filter` stays synchronous (`boolean`) — the lib does not offer a
  `Mono<Boolean>` variant (see sample 08 for the rationale)
- `@Pipeline` stays synchronous (it's a startup declaration, not a
  per-event path)

## Run it

```bash
docker compose -f docker/docker-compose.yml up -d
./mvnw -pl reactive/09-full-stack -am spring-boot:run

# In another terminal
curl http://localhost:<port>/metrics | jq
```

See the imperative README for the diagram, the per-annotation tier
table, and the explanation of the dual-token divergence.

## Key files

- `src/main/java/.../FullStackHandler.java` — all annotations, `Mono<Void>` handlers
- `src/main/java/.../MetricsController.java` — `GET /metrics` returning `Mono<Map>`
- `src/main/java/.../MetricsLogger.java` — periodic INFO log (sync `@Scheduled`)
- `src/main/resources/application.yml` — 5/2/1/1 generator on `orders-full`
