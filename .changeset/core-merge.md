---
"@kronos-ts/core": minor
"@kronos-ts/test": minor
"@kronos-ts/drizzle": minor
"@kronos-ts/knex": minor
"@kronos-ts/kysely": minor
"@kronos-ts/prisma": minor
"@kronos-ts/typeorm": minor
"@kronos-ts/postgres": minor
"@kronos-ts/rabbitmq": minor
"@kronos-ts/kronosdb": minor
"@kronos-ts/axon-server": minor
---

One core package, and edge verbs instead of gateways.

`@kronos-ts/common`, `@kronos-ts/messaging`, `@kronos-ts/eventsourcing`,
`@kronos-ts/modelling` and `@kronos-ts/app` are gone. They were one thing split
five ways: every one of them depended on the next, no host ever installed a
subset, and the split bought nothing but five version numbers to keep in step.
They are now `@kronos-ts/core`, whose internal layout still separates them —
`unit-of-work/`, `messages/`, `query/`, `buses/`, `handlers/`, `processor/`,
`state/`, `stores/`, `assembly/` — because the SECTIONS were the real idea and
the package boundaries were not.

```ts
// before
import { qn } from "@kronos-ts/common"
import { commandHandler, simpleCommandBus } from "@kronos-ts/messaging"
import { inMemoryEventStore } from "@kronos-ts/eventsourcing"
import { state } from "@kronos-ts/modelling"
import { kronos } from "@kronos-ts/app"

// after
import { qn, commandHandler, simpleCommandBus, inMemoryEventStore, state, kronos } from "@kronos-ts/core"
```

## Edge verbs replace the gateways

A gateway was an object with one method that closed over a bus. The verb is the
same operation with the bus as its first argument — a function of all its real
arguments, which a host partially applies itself if it wants the bus fixed.

```ts
// before
const result = await app.commandGateway.send(CreateCourse, { courseId }, metadata)
const view   = await app.queryGateway.query(GetCourse, { courseId })

// after
const result = await send(commandBus, CreateCourse, { courseId }, metadata)
const view   = await query(queryBus, GetCourse, { courseId })
```

`subscriptionQuery(queryBus, descriptor, payload, metadata?)` completes the set.
`App` loses both gateways and its state managers: it is now
`{ processors: ReadonlyMap<string, RunningProcessor>; stop(): Promise<void> }`.

## One interceptor, one function

`correlatingCommandBus`/`correlatingQueryBus` took a VARIADIC list of metadata
providers, so two far-apart call sites could fight over the order. There is one
seam and one function now, and plurality composes in function space where the
order is written down:

```ts
// before
correlatingCommandBus(bus, lineageProvider, tenancyProvider)

// after
interceptingCommandBus(bus, (m) => tenancy(lineage(m)))
```

`Intercept<M> = (message: M) => M` takes the MESSAGE, not its metadata, because
`causationId` is the message's identifier and a metadata transform cannot see
it. `lineage` is exported as the rule everybody needs. `MetadataProvider` and
`CorrelationDataProvider` are both deleted: `ctx` carries the handled message's
metadata outward on `send` / `query` / `append` — uniformly, command leg and
event leg alike — so there is nothing left for a provider seam to do.

## The processor is a value

The `trackingProcessor(...)` / `subscribingProcessor(...)` builders, the
`processors` list on `kronos`, `SequencingPolicy` objects and the whole
enqueue-policy machinery are deleted.

```ts
// before
processors: [{
  ...trackingProcessor("balances").eventHandlers(onDebited, onCredited).build(),
  eventStore, tokenStore, unitOfWork,
  sequencingPolicy: sequentialPerTag("accountId"),
  deadLetterQueue: dlq,
}]

// after
const balances = eventProcessor({
  name: "balances", eventStore, tokenStore, unitOfWork,
  sequence: sequentialPerTag("accountId"),
  deadLetterQueue: dlq,
})
eventHandlers: [
  { ...onDebited,  commandBus, queryBus, processor: balances },
  { ...onCredited, commandBus, queryBus, processor: balances },
]
```

`Sequence = (event) => string` is TOTAL — "no ordering constraint" is the lane
`(e) => e.identifier`, not a missing answer — and `eventProcessor` REJECTS a
`deadLetterQueue` given without a `sequence`, because parking is a lane
operation. Subscribing (on-commit) delivery is gone; delivery is tracked.

## `kronos` takes four lists and nothing else

```ts
kronos({ commandHandlers?, queryHandlers?, eventHandlers?, states? })
```

Buses ride on the entries that use them, under their own names, exactly as the
event store already did. Stores group by OBJECT identity; processors group by
NAME, because a token persists under that name across restarts — two entries
naming `"balances"` are one delivery, and two that name it with conflicting
config are a boot error naming both entries.

## Seams carry their partition per call

`SequencedDeadLetterQueue` now takes `processingGroup` as its first argument on
every method, mirroring `TokenStore`'s `processorName`. One queue object is one
table, and which partition a call touches is a property of the caller — not
something baked into a constructor, which made `clear()` mean two different
things depending on which object you were holding.

```ts
// before
const dlq = drizzleDeadLetterQueue(db, kronosDeadLetters, "balances")
await dlq.enqueue(letter, uow)

// after
const dlq = drizzleDeadLetterQueue(db)          // the table is the adapter's
await dlq.enqueue("balances", letter, uow)
```

Drizzle's table moved into the adapter and is exported as `kronosDeadLetters`
for migrations; it is no longer passed back in.

## Core contains zero tracing vocabulary

`SpanFactory`, `MetricsRecorder`, `tracingHandler`, `meteringHandler` and
`tracingCommandBus` are out of core. Observability is a package of functions
over the public shapes.

## Also

- The named type `UnitOfWorkFactory` is deleted — seams spell it `() => UnitOfWork`.
- `@kronos-ts/test`'s `testFixture` takes the four lists and exposes the
  `commandBus` / `queryBus` it built; there is no `processors` option.
