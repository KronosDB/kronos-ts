---
"@kronos-ts/core": minor
"@kronos-ts/postgres": minor
"@kronos-ts/drizzle": minor
"@kronos-ts/knex": minor
"@kronos-ts/kysely": minor
"@kronos-ts/prisma": minor
"@kronos-ts/typeorm": minor
---

Core's folders now say what things MEAN, not what they technically are — and the adapter transaction glue, which only ever used the public phase API, has left core to live with the packages that use it.

**The folders name activities, not shapes.** `buses/`, `handlers/`, `stores/`, `processor/`, `primitives/`, `messages/`, `assembly/` are gone. A bus, a handler and a store are the same three shapes repeated once per message kind, so filing by shape scattered each kind's life across seven directories: to read what happens to a command you opened `buses/command-bus.ts`, `buses/local-command-bus.ts`, `buses/verbs.ts`, `handlers/command-handler.ts`, `handlers/ctx-send.ts`, `handlers/command-handling-module.ts` and `handlers/handler-context.ts`. Now you open one folder.

```
// before                              // after
src/buses/command-bus.ts               src/command-handling/bus.ts
src/buses/local-command-bus.ts         src/command-handling/local-bus.ts
src/buses/verbs.ts            ┐        src/command-handling/send.ts
src/handlers/ctx-send.ts      ┘        src/command-handling/handler.ts
src/handlers/command-handler.ts        src/command-handling/context.ts
src/handlers/command-handling-module.ts  src/command-handling/subscribe.ts
src/handlers/handler-context.ts
```

The lists `kronos` takes map 1:1 onto the activity folders, which is the whole organising idea — and `event-sourcing/` is the folder that takes no list, because what lives there is not behaviour to register but the log and the state values the handlers close over:

```ts
kronos({
  commandHandlers,   // ← command-handling/
  queryHandlers,     // ← query-handling/
  eventHandlers,     // ← event-processing/
})                   //   event-sourcing/ — no list; `ctx.load` is handed the state
```

with `message/` underneath all of them (what a message IS before any kind picks it up — and it imports from no activity folder, which is why `tag.ts` lives there: `event({ tags })` is where a tag is written, so a tag is message vocabulary and the sourcing side imports it, not the reverse), `event-scheduling/` for events that have not happened yet, and `unit-of-work/` for the task lifecycle all of it runs inside. `interception/` and `correlation/` are unchanged; `assembly/kronos.ts` is now `src/kronos.ts`.

Three things merged while moving, because each was half a concept on its own:

- **Both births of a command are one file.** The edge verb `send(bus, D, p)` and the `ctx.send` capability build the same message and call the same bus method; the only difference is that one of them has a task open to stamp the instant from. Same for `query`, and the `subscriptionQuery` verb now sits beside the `SubscriptionQueryResult` it returns.
- **Snapshot policy and snapshot store are one address.** A policy that decides a snapshot is due and a store seam it lands in are each half a concept; `state/snapshot-policy.ts` + `stores/snapshot-store.ts` are now `event-sourcing/snapshot.ts`.
- **The handler context splits three ways.** `handlers/handler-context.ts` held all three contexts; each now lives with its kind (`command-handling/context.ts`, `query-handling/context.ts`, `event-processing/context.ts`). What all three are BUILT from — the capability types and `HandlerContextDeps` — stays with the command context, the widest of them, rather than in a `shared/` folder that would name no concept at all.

Every export keeps its name and its signature. This is a move, not a redesign — but deep imports into core's internals will not survive it, and neither will the one subpath core published:

**BREAKING — `@kronos-ts/core/transaction` is gone.** Core exports its barrel and nothing else. The glue behind it — `transactionRegistry`, `adapterUnitOfWork`, `openTransaction`, `activeTransaction`, `claimed` — is now a package-private `src/transaction-glue.ts` in each of the six persistence packages.

Nothing changes for a host: `postgresUnitOfWork(unitOfWork, pg)`, `drizzleTransaction(uow)` and `activeDrizzleTransaction(uow)` are the same functions with the same semantics. It breaks for an **external adapter author** who was building a seventh family on the shared glue — that import no longer resolves, and the fix is to own the eighty lines, which is now the recommended shape anyway:

```ts
// before — the shared glue, reached through a subpath
import { adapterUnitOfWork, transactionRegistry } from "@kronos-ts/core/transaction"

// after — your own copy, over the public phase API and nothing else
import { Phase, type UnitOfWork } from "@kronos-ts/core"
const registry = new WeakMap<UnitOfWork, Slot>()
uow.on(Phase.COMMIT, () => hooks.commit(tx))
uow.onError(() => { if (!committed) return hooks.rollback(tx) })
```

That import list is the argument. The glue never had privileged access to the handle — `uow.on(Phase.COMMIT, …)` and `uow.onError(…)` are the whole of what it touches, and the base `UnitOfWork` has no transaction concept for it to reach into — which makes it a HELPER over public shapes, and by this surface's own first rule helpers are not core. It was shared to stop six copies diverging; what it actually did was force one copy to carry every family's needs at once. Split, each package states its binding honestly: **eager** for drizzle, knex, kysely, prisma and typeorm (a `PRE_INVOCATION` hook forces the transaction open before the action, because their token store and DLQ read through the observing accessor and must not be left writing outside it — so those copies have no lazy mode to get wrong), **lazy** for postgres (claimed at mint, begun only when a writer asks, so its read paths pay no begin/commit and claim no connection — and it alone carries `claimed`, the discrimination lazy binding needs).

The ordering the shared version pinned is still pinned, now against the copies that have to honour it: the eager tests live in `@kronos-ts/drizzle`, the lazy and `claimed` tests in `@kronos-ts/postgres`. And the proof the eviction was sound is in core's own suite — the correlation test that used to import the glue to show an adapter's transaction stays reachable through a composed handle now writes that adapter inline in six lines, against the public phase API, and still passes.
