# 06 — Checkpoint (imperative)

`@Checkpoint` persists resume tokens so the stream survives restarts
without losing events.

## What it does

- `saveEveryN = 1` — writes the resume token after every handler success
- `saveIntervalSeconds = 3` — heartbeat that advances the "last seen"
  token even when no handler runs
- `startPosition = RESUME` — on (re)start, pick up from the persisted
  token (3-level cascade documented in the lib Javadoc)
- `onHistoryLost = RESUME_FROM_NOW` — pragmatic fallback if both tokens
  have rolled off the oplog (default would be `FAIL`)
- `resumeStrategy = PROCESSED_FIRST` — the default (introduced in
  `stream-core 1.0.0-rc.2`). Cascade starts from the last *processed*
  token, giving strict at-least-once: in-flight events not yet
  acknowledged at crash time are replayed. Switch to `SEEN_FIRST` for
  fast restart on low-volume or heavily-filtered streams — cascade
  then starts from the heartbeat-fresh seen token at the cost of
  dropping in-flight events

Tokens are stored in the `_fw_checkpoints` collection, keyed by stream
name (here: `checkpoint-handler`).

## Run it

```bash
docker compose -f docker/docker-compose.yml up -d
./mvnw -pl imperative/06-checkpoint -am spring-boot:run
```

## Restart demo (the actual reason this sample exists)

1. Run as above, watch a handful of `[checkpoint] #N` lines
2. `Ctrl-C` to stop the app
3. (Optional: let the generator from another sample keep writing to
   `orders-checkpoint` while you're down — or just wait a few seconds.
   The Mongo oplog accumulates events regardless)
4. Re-run the same command

Without `@Checkpoint` the handler would restart at "now" and silently
miss whatever happened during the downtime. With it, the resume token
in `_fw_checkpoints/checkpoint-handler` rewinds the stream to where it
stopped.

The `pool.refresh-strategy: QUERY_EACH_TICK` in the yml exists for the
same reason: the in-memory id pool of the data generator is gone after
restart, so we query a random doc from MongoDB instead.

## Key files

- `src/main/java/.../CheckpointHandler.java` — `@Checkpoint` attrs
- `src/main/resources/application.yml` — generator + refresh strategy
