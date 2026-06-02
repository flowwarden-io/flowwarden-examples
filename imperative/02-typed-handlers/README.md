# 02 — Typed handlers (imperative)

All five typed callbacks (`@OnInsert`, `@OnUpdate`, `@OnDelete`,
`@OnReplace`, `@OnChange`) on a single handler class.

## What you'll learn

- The two handler signature shapes — POJO direct (`Order order`) vs
  rich context (`ChangeStreamContext<Order>`) — and when each one is
  appropriate
- That `@OnDelete` cannot use the POJO form: MongoDB only forwards the
  document `_id` on a delete; the full document is gone
- That `@OnChange` is a **catch-all**: it fires only for operations
  that have no specific handler on the same class. In this sample all
  four operations are handled, so `@OnChange` stays at zero — remove
  one of the specific methods and you'll see it pick up the slack

## Annotations used

- `@EnableFlowWarden` on `TypedApplication`
- `@ChangeStream(collection = "orders-typed", documentType = Order.class)`
- `@OnInsert`, `@OnUpdate`, `@OnDelete`, `@OnReplace`, `@OnChange`

## Run it

```bash
docker compose -f docker/docker-compose.yml up -d
./mvnw -pl imperative/02-typed-handlers -am spring-boot:run
```

The data generator is wired to produce a mix of all four operations
(4 inserts/s, 2 updates/s, 1 delete/s, 1 replace/s) — within a few
seconds you should see every callback fire **except `@OnChange`**.

> **Note on `@OnChange`** — it is a catch-all and FlowWarden only routes
> an event to it when no specific `@OnXxx` method on the class matches.
> In this sample the four typed handlers cover every operation type, so
> `@OnChange` is never called and its counter stays at zero. Comment
> out one of the specific methods (say `deleted`) and re-run — deletes
> will then land in `any()`.

## Expected logs

```
ImperativeDataGenerator started on collection 'orders-typed' — rates: 4.0/2.0/1.0/1.0
[typed] INSERT  Order{id=..., status=PENDING, total=812.50}
[typed] INSERT  Order{id=..., status=CONFIRMED, total=37.10}
[typed] UPDATE  hello-handler INSERT orderId=... clusterTime=...
[typed] REPLACE Order{id=..., status=CANCELLED, total=412.75}
[typed] DELETE  hello-handler DELETE orderId=... clusterTime=...
```

## Key files

- `src/main/java/.../OrderHandler.java` — one class, five typed methods
- `src/main/resources/application.yml` — generator wired for all four ops
