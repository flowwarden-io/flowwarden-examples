# 07 — Pipeline (reactive)

Reactive twin of `imperative/07-pipeline`. Same `@Pipeline` declaration
— the method itself stays synchronous (it's a one-shot startup
declaration, not a per-event hot path), only the handler return type
changes to `Mono<Void>`.

## Run it

```bash
docker compose -f docker/docker-compose.yml up -d
./mvnw -pl reactive/07-pipeline -am spring-boot:run
```

See the imperative README for the full commentary and the verification
recipe.

## Key files

- `src/main/java/.../PipelineHandler.java`
- `src/main/resources/application.yml`
