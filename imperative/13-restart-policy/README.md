# 13 — Restart policy (imperative)

`@RestartPolicy` tunes the **managed restart loop**: what FlowWarden does
when a stream dies *at runtime* — the cursor is closed by a network
outage, a primary stepdown, a non-resumable server error, or the watched
collection is dropped. Requires `flowwarden-stream-core` ≥ `1.0.0-rc.7`.

## What you'll learn

- That a runtime cursor death is **not** a stop: FlowWarden reports the
  stream `CRASHED`, then resubscribes it on its own, re-entering the full
  startup path (resume cascade included)
- How `@RestartPolicy` controls the cadence of those attempts and,
  optionally, a ceiling after which the loop gives up
- Why the defaults — 1s doubling up to 60s, unlimited attempts, no
  jitter — are exactly what a stream without the annotation gets
- Why `@Checkpoint(onHistoryLost = RESUME_FROM_NOW)` is needed for a
  *dropped* collection to be restartable at all

## Annotations used

- `@RestartPolicy(maxAttempts = 10, initialDelay = "500ms", maxDelay = "5s", multiplier = 2.0, jitter = true)`
- `@Checkpoint(onHistoryLost = RESUME_FROM_NOW)`
- `@ChangeStream(collection = "orders-restart", documentType = Order.class)`
- `@OnInsert` — counts and logs every insert

## How the cursor deaths are produced

`CollectionDropper` drops `orders-restart` every 20 seconds (10 seconds
after startup). MongoDB answers a drop with an `invalidate` event and
closes the cursor — the same shape of death as a server-side failure, but
deterministic and reproducible from inside the JVM. The shared data
generator keeps inserting at 2/s, so the collection is recreated on the
next insert and the resubscribed stream has events to process again.

A dropped collection is also a **history loss**: the persisted resume
token points into a namespace that no longer exists. With the default
`onHistoryLost = FAIL`, FlowWarden treats the invalidation as terminal
and never enters the restart loop — deliberately, so that silent data
loss is never papered over. `RESUME_FROM_NOW` tells it to repair the
checkpoint to a fresh position instead; events inserted between the drop
and the resubscription are skipped, which is the trade-off this sample
accepts.

`RestartObserver` is a minimal `StreamMetricsProvider` that logs the
three signals of one restart cycle: `onStreamInvalidated`,
`onStreamStopped(CRASHED)`, `onStreamRestarted(attempt)`.

## Run it

```bash
docker compose -f docker/docker-compose.yml up -d
./mvnw -pl imperative/13-restart-policy -am spring-boot:run
```

## Expected logs

```
ImperativeDataGenerator started on collection 'orders-restart' — rates: 2.0/0.0/0.0/0.0
[restart] #1 ✓ order 06d5d0f5 (total=1731.40)
...
[restart-demo] dropping collection 'orders-restart' — the change stream cursor will be invalidated
Watched collection invalidated for stream 'restart-handler' (cause: DROP)
[restart] stream 'restart-handler' invalidated (cause: DROP) — the cursor is gone
[restart] stream 'restart-handler' CRASHED — handed to the restart loop
Stream 'restart-handler' checkpoint self-repaired after invalidate — ...
Stream 'restart-handler' died at runtime — resubscription attempt 1 in 512ms
Stream 'restart-handler' resubscribed after runtime death (attempt 1)
[restart] stream 'restart-handler' resubscribed on attempt #1
[restart] #23 ✓ order f9cbca42 (total=42.18)
```

Every 20 seconds the same cycle repeats. The smoke test asserts the
cycle end to end: events flow, an invalidation and a `CRASHED` are
observed, `onStreamRestarted` fires, and events flow again afterwards.

## Try it: watch the loop give up

Set `maxAttempts = 2` on the handler, then stop MongoDB while the sample
runs:

```bash
docker compose -f docker/docker-compose.yml stop
```

The cursor dies, attempt 1 fails after ~500ms, attempt 2 after ~1s, and
the loop gives up: a second `CRASHED` is reported, the stream stays
stopped after `docker compose … start`, and only
`POST /actuator/flowwarden/restart-handler/restart` brings it back.
With the default `maxAttempts = 0` the loop would keep trying every 5s
(this sample's `maxDelay`) and resubscribe by itself once MongoDB is back.

## Key files

- `src/main/java/.../RestartHandler.java` — `@RestartPolicy` + `@Checkpoint(onHistoryLost = RESUME_FROM_NOW)`
- `src/main/java/.../CollectionDropper.java` — the fault injector
- `src/main/java/.../RestartObserver.java` — logs the restart signals
- `src/main/resources/application.yml` — 2 inserts/s, drop cadence
