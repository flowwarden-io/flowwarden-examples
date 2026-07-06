# FlowWarden Examples

Copy-paste-friendly Spring Boot samples for
[`flowwarden-stream-core`](https://central.sonatype.com/artifact/io.flowwarden/flowwarden-stream-core).
One module per FlowWarden annotation, mirrored in **imperative** and
**reactive** variants.

## Prerequisites

- Java 17+
- Docker (for the MongoDB replica set)

## Quickstart

```bash
# 1. start a single-node MongoDB replica set
docker compose -f docker/docker-compose.yml up -d

# 2. run the simplest sample
./mvnw -pl imperative/01-hello-world -am spring-boot:run
```

A new `Order` is written to the `orders-hello` collection every second
and surfaces in the handler as an `@OnInsert` event.

## Repository layout

```
flowwarden-examples/
├── docker/docker-compose.yml          MongoDB 6.0 + replica set init
├── examples-common/                   Shared POJO + configurable data generator
├── imperative/                        Spring MVC / data-mongodb variant
│   └── 01-hello-world/
└── reactive/                          Spring WebFlux / data-mongodb-reactive variant
    └── 01-hello-world/
```

## Annotation → sample matrix

| Annotation | Sample | Status |
|---|---|---|
| `@EnableFlowWarden`, `@ChangeStream`, `@OnInsert` | `01-hello-world` | available |
| `@OnInsert`, `@OnUpdate`, `@OnDelete`, `@OnReplace`, `@OnChange` | `02-typed-handlers` | available |
| `@OnError` | `03-error-handling` | available |
| `@RetryPolicy` | `04-retry-policy` | available |
| `@DeadLetterQueue`, `@MongoDlqOptions` | `05-dlq` | available |
| `@Checkpoint`, `ResumeStrategy` | `06-checkpoint` | available |
| `@Pipeline` | `07-pipeline` | available |
| `@Filter` | `08-filter` | available |
| **every major annotation + dual-token divergence demo** | `09-full-stack` | available |
| `TransactionInfo` — group events by `lsid`/`txnNumber` | `10-transactions` | available |
| `@JaversStream`, `@OnInitial`, `@OnUpdate`, `@OnTerminal` (flowwarden-javers) | `11-javers` | available (imperative only) |

Each available sample exists in two flavours: `imperative/<name>` and
`reactive/<name>`. Pick the one that matches your stack — they are
deliberately mirrored so you can compare them side by side.

Sample `11-javers` is imperative-only — Javers' MongoDB Spring Boot
starter is synchronous and has no reactive equivalent.

## Imperative vs reactive

Both trees share the same examples-common module and the same `Order`
POJO. Each sample brings exactly **one** Mongo starter:

- `imperative/` → `spring-boot-starter-data-mongodb` → `MongoTemplate`
- `reactive/` → `spring-boot-starter-data-mongodb-reactive` → `ReactiveMongoTemplate`

The shared `DataGeneratorConfiguration` picks the right generator at
context refresh based on which `MongoTemplate` bean is on the
classpath.

The handler signatures differ:

```java
// imperative
@OnInsert void onInsert(Order order) { ... }

// reactive
@OnInsert Mono<Void> onInsert(Order order) { ... }
```

## Tuning the data generator

Each sample has a `examples.data-generator` block in its `application.yml`.

```yaml
examples:
  data-generator:
    enabled: true              # kill switch
    collection: orders-hello   # different per sample to avoid collisions
    rates:
      inserts-per-second: 1
      updates-per-second: 0
      deletes-per-second: 0
      replaces-per-second: 0
    invalid-payload-ratio: 0.0 # used by error/retry/dlq samples
```

Raise or lower individual rates to observe how a handler behaves under
load. Set `enabled: false` to silence the generator without rebuilding.

## Running several samples in parallel

Every sample sets `server.port: 0` (random port). You can run as many
samples concurrently as you want — each one uses a dedicated MongoDB
collection so they don't step on each other:

```bash
./mvnw -pl imperative/01-hello-world -am spring-boot:run    # one terminal
./mvnw -pl reactive/01-hello-world  -am spring-boot:run    # another terminal
```

Look for the random port in the startup banner (`Tomcat started on
port(s): XXXXX` or `Netty started on port XXXXX`).

## License

Apache 2.0 — see [LICENSE](LICENSE).
