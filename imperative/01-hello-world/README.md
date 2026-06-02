# 01 — Hello World (imperative)

The minimal end-to-end example: one Spring Boot application, one handler,
one annotation per concern.

## What you'll learn

- How `@EnableFlowWarden` activates the framework
- How `@ChangeStream` declares a handler bean
- How `@OnInsert` consumes a typed POJO from a MongoDB change stream

## Annotations used

- `@EnableFlowWarden` — on the application class
- `@ChangeStream(collection = "orders-hello", documentType = Order.class)` — on the handler class
- `@OnInsert` — on the handler method

## Run it

```bash
# from the repo root
docker compose -f docker/docker-compose.yml up -d
./mvnw -pl imperative/01-hello-world -am spring-boot:run
```

The shared `DataGenerator` (in `examples-common`) writes one `Order`
per second to the `orders-hello` collection. Each insert triggers
`HelloHandler#onInsert`.

## Expected logs

```
ImperativeDataGenerator started on collection 'orders-hello' — rates: 1.0/0.0/0.0/0.0 ops/s (i/u/d/r)
[hello] insert #1 — Order{id=..., customer=bob@example.com, status=CONFIRMED, total=42.18}
[hello] insert #2 — Order{id=..., customer=alice@example.com, status=PENDING, total=137.50}
```

The Tomcat port is chosen at random (the `application.yml` sets
`server.port: 0`) so you can start several samples in parallel.
Look for `Tomcat started on port(s): XXXXX` in the logs.

## Key files

- `src/main/java/.../HelloApplication.java` — entry point with `@EnableFlowWarden`
- `src/main/java/.../HelloHandler.java` — handler class with `@ChangeStream` + `@OnInsert`
- `src/main/resources/application.yml` — generator rates and Mongo URI
