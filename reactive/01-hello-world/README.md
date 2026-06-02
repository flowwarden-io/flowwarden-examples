# 01 — Hello World (reactive)

Reactive twin of `imperative/01-hello-world`: same wiring, same shape,
the handler simply returns a `Mono<Void>` instead of `void`.

## What you'll learn

- That `@EnableFlowWarden` and `@ChangeStream` work the same way in
  reactive mode
- How `@OnInsert` can return a reactive publisher that FlowWarden
  subscribes to
- That swapping `spring-boot-starter-data-mongodb` for
  `spring-boot-starter-data-mongodb-reactive` is enough to switch modes —
  no other code change

## Annotations used

- `@EnableFlowWarden` — on the application class
- `@ChangeStream(collection = "orders-hello", documentType = Order.class)` — on the handler class
- `@OnInsert` returning `Mono<Void>` — on the handler method

## Run it

```bash
# from the repo root
docker compose -f docker/docker-compose.yml up -d
./mvnw -pl reactive/01-hello-world -am spring-boot:run
```

## Expected logs

```
ReactiveDataGenerator started on collection 'orders-hello' — rates: 1.0/0.0/0.0/0.0 ops/s (i/u/d/r)
[hello] insert #1 — Order{id=..., customer=bob@example.com, status=CONFIRMED, total=42.18}
[hello] insert #2 — Order{id=..., customer=alice@example.com, status=PENDING, total=137.50}
```

The Netty port is chosen at random (`server.port: 0`) — start it in
parallel with the imperative variant or any other sample without
adjusting ports.

## Key files

- `src/main/java/.../HelloApplication.java` — entry point with `@EnableFlowWarden`
- `src/main/java/.../HelloHandler.java` — handler returning `Mono<Void>`
- `src/main/resources/application.yml` — generator rates and Mongo URI
