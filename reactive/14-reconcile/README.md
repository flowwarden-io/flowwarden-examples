# 14 — Reconcile (reactive)

`@Reconcile` declares the application's **reconciliation of a lost history
window**: what to do when a stream resumes after both of its saved positions
expired from the MongoDB oplog, and the events in between are gone for good.
Requires `flowwarden-stream-core` ≥ `1.0.0-rc.8`.

## What you'll learn

- That a self-repairing `onHistoryLost` strategy (`RESUME_FROM_NOW`,
  `RESUME_FROM_OPLOG_START`) **skips** the lost window: fine for a
  notification, wrong for anything that must have seen every event
- How a `@Reconcile` method repairs a **derived view** — here an in-memory
  order index — from the source of truth, **before** the stream resumes and
  before any checkpoint write, so the repair never interleaves with live events
- What the `HistoryLostContext` tells you: the bound of the lost window
  (`lostWindowStart()` → `lostWindowEnd()`), the strategy, the attempt
- Why the hook must be idempotent, and why a hook that throws leaves the
  stream **down** rather than resuming on a stale view

## Annotations used

- `@Checkpoint(onHistoryLost = RESUME_FROM_NOW)`
- `@Reconcile` — `Mono<Void> rebuild(HistoryLostContext ctx)`
- `@ChangeStream(name = "reconcile-handler", collection = "orders-reconcile", documentType = Order.class)`
- `@OnInsert` — `Mono<Void> onInsert(Order order)`, feeds the index

## How the history loss is produced

In production the sequence is: the stream is down longer than the oplog
window, MongoDB discards the entries its saved positions point to, the
next start finds both positions expired and reaches **resume cascade
level 3**. The shared MongoDB of these samples has an oplog far too large
to roll over in a demo, so `HistoryLossInjector` simulates the outcome,
deterministically, every 30 seconds:

1. `streamManager.stopStream("reconcile-handler")` — the stream is down
2. 3 seconds of downtime, during which the shared data generator keeps
   inserting at 2/s: **those orders are the lost window**
3. both saved tokens in `_fw_checkpoints` are overwritten with one that no
   longer resolves, the real checkpoint timestamps are kept — so the window
   handed to the hook is the actual downtime
4. `streamManager.startStream("reconcile-handler")`

The resume cascade cannot tell this apart from a genuine expiry (it is the
technique the library's own integration tests use): both positions fail,
`onResumeHistoryLost` fires, `RESUME_FROM_NOW` resolves a fresh position —
and before persisting it or opening the cursor, FlowWarden calls the hook.

## What the hook does

`ReconcileHandler.rebuild(HistoryLostContext)` asks the collection for every
order created since `lostWindowStart()` and (re)indexes it. Three properties
worth noticing:

- **It resyncs, it does not replay.** There is no journal of lost events to
  replay; the hook queries the current state and brings the view in line.
  Orders already indexed are simply overwritten with identical data — the
  hook is idempotent, which matters because a crash mid-reconciliation
  re-runs it on the next start (the expired tokens stay in the checkpoint
  until the hook has returned).
- **It runs before anything is delivered.** Events written after
  `lostWindowEnd()` are delivered by the stream once the hook returned, so
  an order is never indexed by the resync and the stream at the same time.
- **The window is wide on purpose.** `lostWindowStart()` is the last moment
  the stream is known to have been current, not the first lost event. A
  real model would key the resync on an `updatedAt` field to also catch
  modifications; the shared `Order` only has `createdAt`, which is why this
  sample generates inserts only.

`ReconcileObserver` is a minimal `StreamMetricsProvider` logging the signals
of one cycle: `onResumeHistoryLost`, then `onHistoryLostReconciled(attempt,
elapsed)` — or `onHistoryLostReconciliationFailed` if the hook threw.

## Run it

```bash
docker compose -f docker/docker-compose.yml up -d
./mvnw -pl reactive/14-reconcile -am spring-boot:run
```

## Expected logs

```
ReactiveDataGenerator started on collection 'orders-reconcile' — rates: 2.0/0.0/0.0/0.0
[reconcile] #1 indexed order 3f9a2c1e (SHIPPED, total=812.40)
...
[reconcile-demo] stopping stream 'reconcile-handler' — simulating a downtime longer than the oplog window
[reconcile] stream 'reconcile-handler' stopped (GRACEFUL)
[reconcile-demo] both saved positions of 'reconcile-handler' replaced by an expired token (last activity kept at ...) — the next start reaches cascade level 3
Resume token expired for stream 'reconcile-handler' (last checkpoint: ...). Applying strategy: RESUME_FROM_NOW
[reconcile] stream 'reconcile-handler' reached cascade level 3 — both saved positions expired
Stream 'reconcile-handler': invoking the @Reconcile hook before resuming (attempt 1): the application reconciles the lost window [... -> ...] — nothing is delivered meanwhile, the expired checkpoint tokens stay until it returns
[reconcile] history lost on 'reconcile-handler' (RESUME_FROM_NOW, attempt 1) — re-syncing the index for orders created since ... (lost window end: ...)
[reconcile] re-synced 6 order(s) from the collection — the stream now resumes after ...
Stream 'reconcile-handler': @Reconcile hook returned after 12 ms (attempt 1)
[reconcile] stream 'reconcile-handler' reconciled in 12 ms (attempt 1) — resuming
[reconcile] #31 indexed order 9c0b7d44 (PENDING, total=120.00)
```

Every 30 seconds the same cycle repeats. The smoke test asserts it end to
end: the index is fed, a level-3 resume is observed, the hook ran exactly
once before the resume and re-synced at least one order, and the index is
fed again afterwards.

## Try it: a reconciliation that fails

Make `rebuild` throw (an `IllegalStateException("search engine down")`
will do). On the next injected loss the start fails with
`ReconciliationFailedException` — a `HistoryLostException`, so FlowWarden
treats it as terminal: no restart loop, `onHistoryLostReconciliationFailed`
then `onStreamStopped(CRASHED)`, and the stream stays **down**. The expired
tokens are kept in `_fw_checkpoints` as the "reconciliation due" marker:
`POST /actuator/flowwarden/reconcile-handler/restart` (or the next
injector cycle) runs the hook again. A failed reconciliation never lets the
stream resume on a stale view.

## Key files

- `src/main/java/.../ReconcileHandler.java` — `@Reconcile` + `@Checkpoint(onHistoryLost = RESUME_FROM_NOW)`
- `src/main/java/.../OrderSearchIndex.java` — the derived view and its resync (`ReactiveMongoTemplate`)
- `src/main/java/.../HistoryLossInjector.java` — the fault injector
- `src/main/java/.../ReconcileObserver.java` — logs the history-loss signals
- `src/main/resources/application.yml` — 2 inserts/s, downtime and cadence
