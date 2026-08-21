---
"@kronos-ts/core": minor
"@kronos-ts/rabbitmq": minor
"@kronos-ts/axon-server": minor
"@kronos-ts/kronosdb": minor
"@kronos-ts/postgres": minor
"@kronos-ts/drizzle": minor
"@kronos-ts/knex": minor
"@kronos-ts/kysely": minor
"@kronos-ts/prisma": minor
"@kronos-ts/typeorm": minor
"@kronos-ts/otlp": minor
"@kronos-ts/test": minor
---

Types are function signatures. The `interface` keyword is extinct across every package: 581 declarations are now `type` aliases, `extends` is an intersection, and an interface that was nothing but a call signature is a bare arrow (`ContextSendFunction`, `ContextQueryFunction`, `EmitUpdateFunction`). Nothing changed shape — a record of functions is still a record, because that is what shared state looks like — so this is source-compatible for anyone who was not declaration-merging our types, which nothing in the emitted `.d.ts` ever invited.

The handler definitions lose the word "Definition". It was the name for a shape that had to be registered somewhere, and nothing is registered any more:

- `CommandHandlerDefinition` → `CommandHandler`
- `QueryHandlerDefinition` → `QueryHandler`
- `EventHandlerDefinition` → `EventHandler`
- `StateModule` → `State`

Type and value namespaces are separate, so `type CommandHandler` and the `commandHandler` function coexist. The entry types (`CommandHandlerEntry`, `QueryHandlerEntry`, `EventHandlerEntry`) keep their names: an entry is a different thing from the handler it points at.

The last behaviour classes are closures. `unitOfWork()` no longer instantiates a `ManagedUnitOfWork` — the phase buckets, the status and the correlation map are closed over, `phase`/`closed` are accessors because only the lifecycle advances them, and the execute-once guard still throws synchronously. In `@kronos-ts/rabbitmq` the three AMQP components are constructed with a call instead of `new`:

- `new AmqpRabbitMqCommandTransport(config, channels)` → `amqpRabbitMqCommandTransport(config, channels)`
- `new AmqpRabbitMqQueryTransport(config, channels)` → `amqpRabbitMqQueryTransport(config, channels)`
- `new AmqpDistributedSubscriberRegistry(config, channels)` → `amqpDistributedSubscriberRegistry(config, channels)`

The two transport names survive as the TYPES those functions return. Classes are Errors only now, everywhere — `instanceof` is the one thing a type alias cannot do, and error discrimination is the only place we need it.

No types were removed. Every candidate we went looking for is still load-bearing: `AnyTagCriteria` is the `{ kind: "any-tag" }` shape four stores switch on, `EventBus`/`SubscribableEventSource`/`EventStorageEngine` are the halves `EventStore` is declared as, and `TagCriteria`/`TypeRestrictedCriteria`/`EitherCriteria` have live readers (`SchemaRegistry` and `IntermediateEventRepresentation` were still live at this point; both were deleted by later changes in this same release — see the upcasting and validation changesets).
