# 12 — Registration API (reactive)

Reactive twin of `imperative/12-registration`: streams declared
**without annotations**, from a YAML catalog, through a
`StreamDefinitionContributor` bean. No `@ChangeStream` class in this
module.

## What differs from the imperative twin

- Handler: `.onInsertReactive((order, ctx) -> Mono<Void>)` instead of
  `.onInsert(...)` — the `ReactiveDocumentHandler` shape, same as an
  annotated `Mono<Void> onInsert(Order, ChangeStreamContext)` method
- `.filter(...)` and `.onError(...)` stay synchronous in both modes: the
  lib offers no `Mono<Boolean>` filter, so the predicate must decide
  without blocking I/O (cache async state in a field, refresh it
  out-of-band)
- Everything else — `.pipeline(...)`, `.checkpoint(...)`,
  `.deadLetterQueue(...)`, validation, defaults — is identical

## Run it

```bash
docker compose -f docker/docker-compose.yml up -d
./mvnw -pl reactive/12-registration -am spring-boot:run
```

## Key files

- `src/main/java/.../YamlStreamContributor.java` — the contributor
- `src/main/java/.../RegistrationProperties.java` — the YAML binding
- `src/main/resources/application.yml` — the stream catalog
