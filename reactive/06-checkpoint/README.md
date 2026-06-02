# 06 — Checkpoint (reactive)

Reactive twin of `imperative/06-checkpoint`. Same `@Checkpoint`
settings, same `_fw_checkpoints/checkpoint-handler` document —
handler returns `Mono<Void>`.

## Run it

```bash
docker compose -f docker/docker-compose.yml up -d
./mvnw -pl reactive/06-checkpoint -am spring-boot:run
```

See the imperative README for the restart demo procedure and the
attribute-by-attribute commentary.

## Key files

- `src/main/java/.../CheckpointHandler.java`
- `src/main/resources/application.yml`
