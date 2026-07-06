# 11 — Javers (imperative)

Native Javers audit stream integration using
[`flowwarden-javers`](https://github.com/flowwarden-io/flowwarden-javers).
Every time the audited `OrderRepository` writes a commit, Javers stores a
snapshot in `jv_snapshots` and FlowWarden delivers it to the handler as
a deserialised `Order` with full audit metadata.

> **Imperative only.** Javers ships
> `javers-spring-boot-starter-mongo` (synchronous) but no reactive
> equivalent, so there is no `reactive/11-javers`.

## What you'll learn

- How `@JaversStream` watches the Javers snapshot collection
- How `@OnInitial` / `@OnUpdate` / `@OnTerminal` map to Javers
  `SnapshotType` (creation / modification / deletion)
- How to read commit metadata, changed properties and version from
  `JaversChangeContext<T>`
- Why writes must go through a `@JaversSpringDataAuditable` repository,
  not `MongoTemplate`

## Annotations used

- `@EnableFlowWarden` — on the application class
- `@JaversSpringDataAuditable` — on `OrderRepository` (from Javers)
- `@JaversStream(entityType = Order.class)` — on the handler class
- `@Checkpoint(saveEveryN = 1)` — persist the resume token after each event
- `@OnInitial`, `@OnUpdate`, `@OnTerminal` — on the handler methods

## Run it

```bash
# from the repo root
docker compose -f docker/docker-compose.yml up -d
./mvnw -pl imperative/11-javers -am spring-boot:run
```

`AuditedOrderWriter` writes 1 insert/sec, 1 update/2s and 1 delete/5s to
the `orders-javers` collection via the audited repository. Javers
records a commit per write to `jv_snapshots`; `OrderAuditHandler`
receives them in real time.

## Expected logs

```
AuditedOrderWriter started — 1 insert/s, 1 update/2s, 1 delete/5s
[javers] created #1 — id=... customer=bob@example.com total=42.18 by unauthenticated
[javers] created #2 — id=... customer=alice@example.com total=137.50 by unauthenticated
[javers] updated #1 — id=... changed=[status] v2 by unauthenticated
[javers] deleted #1 — id=... v3 by unauthenticated
```

The default Javers author is `unauthenticated` — provide an
`AuthorProvider` bean if you want the commit metadata to reflect a real
user.

## Why no shared `DataGenerator`?

The shared `ImperativeDataGenerator` writes via `MongoTemplate.save()`,
which bypasses Spring Data repositories. Javers' Spring Data plugin
only intercepts repository methods, so MongoTemplate writes produce
zero audit. This sample disables the shared generator
(`examples.data-generator.enabled: false`) and ships a local writer
that goes through `OrderRepository`.

## Key files

- `src/main/java/.../JaversApplication.java` — entry point with `@EnableFlowWarden`
- `src/main/java/.../OrderRepository.java` — `@JaversSpringDataAuditable` repository
- `src/main/java/.../OrderAuditHandler.java` — handler class with `@JaversStream` + lifecycle hooks
- `src/main/java/.../AuditedOrderWriter.java` — generates audit traffic through the repository
- `src/main/resources/application.yml` — Mongo URI, disables the shared generator
