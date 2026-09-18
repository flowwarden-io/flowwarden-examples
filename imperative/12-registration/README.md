# 12 — Registration API (imperative)

Streams declared **without annotations**. A `StreamDefinitionContributor`
bean reads the stream catalog from `application.yml` and registers each
entry as a `StreamSpec` at bootstrap. There is no `@ChangeStream` class
in this module.

Use it when the stream configuration lives outside the JVM — a YAML
file, a database table, a feature-flag service — and cannot be a
compile-time annotation.

## What it does

- `YamlStreamContributor` implements `StreamDefinitionContributor`;
  FlowWarden discovers the bean and calls `contribute(...)` once, after
  all singletons exist and before any stream starts
- Each YAML entry becomes `registration.stream(name, Order.class)` with:
  - `.collection(...)`, `.checkpoint(CheckpointSpec.defaults())`
  - `.onInsert((order, ctx) -> ...)` — typed functional handler
  - `.pipeline(() -> List.of($match operationType = insert))` — server-side (07)
  - `.filter(ctx -> status == CONFIRMED)` — client-side (08)
  - `.onError((ex, ctx) -> SKIP, IllegalStateException.class)` — scoped (03)
  - `.deadLetterQueue(DeadLetterQueueSpec.defaults())` — DLQ (05)
- Same validation and defaults as the annotated equivalents; a duplicate
  stream name (annotated or contributed) fails startup

## What StreamSpec covers

Everything `@ChangeStream` offers except `zone`. Registration is
**bootstrap-only**: a contributed stream is fixed for the lifetime of the
application context — there is no hot registration on a running
instance.

## Run it

```bash
docker compose -f docker/docker-compose.yml up -d
./mvnw -pl imperative/12-registration -am spring-boot:run
```

## Verify

```bash
# The stream name comes from application.yml, not from a class
curl -s localhost:<port>/actuator/health | jq '.components.flowWarden'
```

Then change `keep-status` to `PENDING` in `application.yml` and restart:
the handler now only sees pending orders — no recompilation, no
annotation touched.

## Key files

- `src/main/java/.../YamlStreamContributor.java` — the contributor
- `src/main/java/.../RegistrationProperties.java` — the YAML binding
- `src/main/resources/application.yml` — the stream catalog
