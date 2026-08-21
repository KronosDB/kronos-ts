# kronos-ts

A platform for DCB event sourcing in TypeScript.

It is a small core — message shapes, bus shapes, store shapes, a decision-model
shape, and one primitive (the unit of work) — plus a set of packages that are
nothing but functions over those shapes. Transports, persistence adapters and
observability are not plugin points. They are ordinary functions you call.

## The thesis

There is no container, no registration and no ambient state. You compose
functions. A bus is a record of functions; a store is a record of functions; a
transport takes your local bus and returns a bus of the same shape. Handlers are
functions of `(message, ctx)`, and `ctx` is built fresh per invocation as a
closure over that invocation's unit of work — nothing is reachable from a global,
so using a capability outside a handler is a compile error rather than a runtime
throw. Assembly is four flat lists whose entries carry, as bare properties, the
shared objects they run against. `kronos` follows those references and never
counts: two entries share one event store because they name the *same object*,
and two entries share one delivery because they name the same processor *name*.
Topology — which handler writes to which log, which projection has its own
cursor, what is local and what is remote — is not configuration. It is which
literals share which objects.

## A complete example

In-memory everything. This compiles and runs as written.

```ts
import { z } from "zod"
import {
  commandHandler, eventHandler, eventProcessor, inMemoryEventStore, inMemoryTokenStore,
  interceptingCommandBus, interceptingQueryBus, kronos, correlation, query, queryHandler,
  send, localCommandBus, localQueryBus, state, unitOfWork, withNamespace,
} from "@kronos-ts/core"

const ns = withNamespace("bank")
const Deposit = ns.command("Deposit", { payload: z.object({ accountId: z.string(), amount: z.number() }) })
const Deposited = ns.event("Deposited", {
  payload: z.object({ accountId: z.string(), amount: z.number() }),
  tags: { accountId: (p) => p.accountId },
})
const GetBalance = ns.query("GetBalance", { payload: z.object({ accountId: z.string() }) })

const Account = state({
  id: { accountId: z.string() },
  tags: (id) => ({ accountId: id.accountId }),
  evolve: [() => ({ balance: 0 }), [Deposited, (s, { payload }) => ({ balance: s.balance + payload.amount })]],
})

const deposit = commandHandler(Deposit, async ({ payload }, ctx) => {
  const account = await ctx.load(Account, { accountId: payload.accountId })
  if (account.balance + payload.amount > 10_000) throw new Error("deposit limit")
  ctx.append(Deposited, payload)
})

const balances = new Map<string, number>()
const projectDeposit = eventHandler(Deposited, ({ payload }) => {
  balances.set(payload.accountId, (balances.get(payload.accountId) ?? 0) + payload.amount)
})
const getBalance = queryHandler(GetBalance, ({ payload }) => balances.get(payload.accountId) ?? 0)

const eventStore = inMemoryEventStore()
const commandBus = interceptingCommandBus(localCommandBus(unitOfWork), correlation)
const queryBus = interceptingQueryBus(localQueryBus(unitOfWork), correlation)
const processor = eventProcessor({ name: "balances", eventStore, tokenStore: inMemoryTokenStore(), unitOfWork })

const app = kronos({
  commandHandlers: [{ ...deposit, eventStore, commandBus, queryBus }],
  queryHandlers: [{ ...getBalance, queryBus }],
  eventHandlers: [{ ...projectDeposit, commandBus, queryBus, processor }],
})

await send(commandBus, Deposit, { accountId: "acc-1", amount: 100 })
console.log(await query(queryBus, GetBalance, { accountId: "acc-1" }))
await app.stop()
```

Note what is absent. Nothing is registered. `Account` is a value; `deposit` is a
value; `processor` is a value — and `Account` appears in no list at all, because
`ctx.load` is handed the state at the call site and the log off the entry.
`kronos` registers behaviour; data needs no invitation. The spreads at the bottom
are the entire wiring story — swap `inMemoryEventStore()` for
`postgresEventStore(pool, …)` and not one line above the composition root
changes.

## Packages

| Package | What it is |
| --- | --- |
| `@kronos-ts/core` | The shapes and the primitive: messages, descriptors, buses, stores, state folds, the unit of work, the event processor, and `kronos`. In-memory implementations of every store seam. |
| `@kronos-ts/test` | `given(...).when(...).then(...)` — a test as a value, run at a fixture. |
| `@kronos-ts/rabbitmq` | Command and query transport over AMQP. A dumb pipe: routing happens client-side. |
| `@kronos-ts/kronosdb` | Event store plus its snapshotting and scheduling tiers, command/query transport and control plane over KronosDB. Server-side routing. |
| `@kronos-ts/axon-server` | The same family, over Axon Server. |
| `@kronos-ts/postgres` | The full persistence family with no ORM: event store plus its snapshotting and scheduling tiers, unit of work, token store, dead-letter queue, handler wrapper, plus the DDL. |
| `@kronos-ts/drizzle` | Token store, dead-letter queue, unit of work, transaction accessors and handler wrapper for Drizzle. |
| `@kronos-ts/knex` | The same family, for Knex. |
| `@kronos-ts/kysely` | The same family, for Kysely. |
| `@kronos-ts/prisma` | The same family, for Prisma. |
| `@kronos-ts/typeorm` | The same family, for TypeORM. |
| `@kronos-ts/otlp` | Tracing and metrics as OTLP over `fetch`. No `@opentelemetry/*` dependency, no SDK, no global tracer. |

## Documentation

- [How it works](docs/how-it-works.md) — the concepts, each with the code that
  implements it: messages and tags, the unit of work, buses and correlation, the
  three handler contexts, states and the derived DCB query, event processors and
  lanes, and what `kronos` does with three lists.
- [Building an application](docs/building-an-application.md) — the full
  walkthrough: slices as plain values, a composition root, edges, and the folder
  convention. Marks clearly which parts are library and which are house style.
- [Testing](docs/testing.md) — unit level (folds are reduces) and behaviour level
  (`given`/`when`/`then`, timelines, sagas, scenario tables).
- [Writing your own](docs/writing-your-own.md) — the three templates for a new
  package, worked against how `drizzle` and `otlp` are actually built.
- Package guides: [rabbitmq](docs/packages/rabbitmq.md) ·
  [kronosdb](docs/packages/kronosdb.md) ·
  [axon-server](docs/packages/axon-server.md) ·
  [postgres](docs/packages/postgres.md) ·
  [drizzle](docs/packages/drizzle.md) ·
  [otlp](docs/packages/otlp.md) ·
  [test](docs/packages/test.md)

## Requirements

TypeScript with `"module": "NodeNext"`. Descriptors take any
[Standard Schema](https://standardschema.dev) for payloads — zod v4, valibot,
arktype, or your own — and no schema library is a dependency of any Kronos
package. The examples use zod. Packages ship both TypeScript sources and built
`dist/`; the workspace itself is built and tested with Bun.

## License

Apache-2.0.
