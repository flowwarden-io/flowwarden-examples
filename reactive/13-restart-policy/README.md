# 13 — Restart policy (reactive)

Reactive twin of [`imperative/13-restart-policy`](../../imperative/13-restart-policy/README.md).
Same restart loop, same fault injector, same `@RestartPolicy` and
`@Checkpoint(onHistoryLost = RESUME_FROM_NOW)` — read the imperative README
for the full walkthrough. Requires `flowwarden-stream-core` ≥ `1.0.0-rc.7`.

## What differs in reactive mode

- The handler returns `Mono<Void>`.
- A drop does not surface as an error: the reactive change stream
  **completes** on the `invalidate`. FlowWarden treats that unexpected
  completion exactly like a cursor error — `onStreamStopped(CRASHED)`,
  then the restart loop — so the observable cycle is identical.
- `CollectionDropper` drops through `ReactiveMongoTemplate` (blocking on
  the scheduler thread, which is fine for a fault injector).
- Restart attempts run on `fw-stream-restart-reactive` instead of
  `fw-stream-restart`.

## Run it

```bash
docker compose -f docker/docker-compose.yml up -d
./mvnw -pl reactive/13-restart-policy -am spring-boot:run
```

## Expected logs

```
ReactiveDataGenerator started on collection 'orders-restart' — rates: 2.0/0.0/0.0/0.0
[restart] #1 ✓ order 06d5d0f5 (total=1731.40)
...
[restart-demo] dropping collection 'orders-restart' — the change stream cursor will be invalidated
Watched collection invalidated for stream 'restart-handler' (cause: DROP)
[restart] stream 'restart-handler' invalidated (cause: DROP) — the cursor is gone
[restart] stream 'restart-handler' CRASHED — handed to the restart loop
Stream 'restart-handler' died at runtime — resubscription attempt 1 in 487ms
Stream 'restart-handler' resubscribed after runtime death (attempt 1)
[restart] stream 'restart-handler' resubscribed on attempt #1
[restart] #23 ✓ order f9cbca42 (total=42.18)
```

## Key files

- `src/main/java/.../RestartHandler.java` — `@RestartPolicy` + `@Checkpoint(onHistoryLost = RESUME_FROM_NOW)`, `Mono<Void>` handler
- `src/main/java/.../CollectionDropper.java` — the fault injector
- `src/main/java/.../RestartObserver.java` — logs the restart signals
- `src/main/resources/application.yml` — 2 inserts/s, drop cadence
