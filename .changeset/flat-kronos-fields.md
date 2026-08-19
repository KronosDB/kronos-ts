---
"@kronos-ts/core": minor
"@kronos-ts/test": minor
"@kronos-ts/rabbitmq": minor
"@kronos-ts/kronosdb": minor
"@kronos-ts/axon-server": minor
---

kronos is two buses and three plainly-typed lists; enhancement is a
user-composed function.

**`kronos({ commandBus, queryBus, commandHandlers, queryHandlers, states,
processors })` is the entire options surface.** `module()`, `AppModule`, `ModuleStores`, the `modules` array,
`unitOfWorkFactory` and `enhance` are all DELETED. The word "module" is gone
from kronos vocabulary.

```ts
// before — a module wrapper whose only job was to hold stores
kronos({
  commandBus, queryBus, unitOfWorkFactory: uow,
  modules: [module("billing", { eventStore }, ...billingSlice)],
})

// after — four named lists, persistence rides on the items that need it
kronos({
  commandBus: correlatingCommandBus(simpleCommandBus(uow)),
  queryBus: correlatingQueryBus(simpleQueryBus(uow)),
  states: [{ ...Bill, eventStore }],
  commandHandlers: [{ ...openBill, eventStore }],
  queryHandlers: [{ ...getBill, eventStore }],
  processors: [{ ...billProjection, eventStore, tokenStore }],
})
```

The word "registration" is gone with the ceremony it named — nothing is
registered on anything. So is the union that held the mixed list: there is no
`Registration`/`Handler` union, no `kind` read anywhere in assembly, and no
array-vs-record predicate. The caller says what each thing is by choosing the
field, and `kronos` walks four plainly-typed lists.

**Sited properties are BARE — there is no `stores` record.** `HandlerStores`
is DELETED. A record earns its name when something is behind it — one
connection, one transaction, one lifecycle — and nothing was: it was four
unrelated objects in a bag. Each now rides under its own name.

```ts
// before — a bag with nothing behind it
const stores = { eventStore, snapshotStore, tokenStore, tagResolver }
commandHandlers: billing.map((h) => ({ ...h, stores })),

// after — the same spread, minus a level
commandHandlers: billing.map((h) => ({ ...h, eventStore })),
```

An item may carry `eventStore`, `snapshotStore?`, `tagResolver?` and an optional
`name` for diagnostics; a processor entry additionally carries `tokenStore?`,
`deadLetterQueue?` and `unitOfWork?` — the last two of which a TRACKING
processor must actually be given (see below). Items sharing one `eventStore`
OBJECT share one repository set and one stream — identity is the grouping key.
States, command handlers and processors without an event store are a boot error
naming the item; query handlers may legitimately have none, because a read model
served from a projection table needs no log.

**Processor persistence arrives at composition; the builder keeps only policy.**
`trackingProcessor` / `subscribingProcessor` LOSE `.tokenStore(...)`,
`.deadLetterQueue(...)` and `.unitOfWork(...)`, and the built definition no
longer carries those values. Where a cursor is kept, where poison pills are
parked and what transaction a batch runs in are DEPLOYMENT facts — a slice that
names them cannot be redeployed without editing it. Everything the builder still
has is policy: `.batchSize`, `.pollingIntervalMs`, `.errorHandler`,
`.enqueuePolicy`, `.sequencingPolicy`, `.deadLetterListener`,
`.resetClearsDeadLetters`, `.dlqRetryInterval`, `.initialSegmentCount`,
`.onReset`.

```ts
// before — two doors: some persistence on the builder, some on the entry
processors: [{ ...trackingProcessor("bills").eventHandlers(...).tokenStore(ts).build(), stores }]

// after — one door, and both topologies are one .map()
processors: projections.map((p) => ({ ...p, eventStore, tokenStore }))              // uniform
processors: projections.map((p) => ({ ...p, eventStore, tokenStore: tsFor(p.name),  // per-module
                                      deadLetterQueue: dlq(db, p.name) }))
```

A tracking processor entry with no `tokenStore` is now a BOOT ERROR naming the
processor. It used to fall back to a fresh in-memory cursor store, which booted
fine and then replayed the whole log on every restart — a defect that only ever
showed up in production as duplicate projections. Subscribing processors keep no
cursor and need none. `deadLetterQueue` is deliberately per-ENTRY, not per log:
the queue is keyed by sequence identifier alone, so two processors sharing one
object would interleave each other's letters.

`registerCommandHandlersNatively` / `registerQueryHandlersNatively` are now
`subscribeCommandHandlers` / `subscribeQueryHandlers` — named for what they do.

`app.stateManagers` is now keyed by the event store object rather than a module
name. `[state, options]` tuples live in `states` and are otherwise unchanged
apart from the state carrying its own stores.

**The buses own the unit of work.** `simpleQueryBus(unitOfWork)` now captures a
factory exactly as `simpleCommandBus(unitOfWork)` always has, so
`queryGateway(bus)` needs nothing but the bus and `kronos` needs no factory at
all. Processors name their own `unitOfWork` — there is no app-level one to
inherit. It is a property on the processor ENTRY, alongside its stores, because
it is a binding rather than a store and belongs with the other deployment facts.

A TRACKING processor entry must carry BOTH `tokenStore` and `unitOfWork`, or
`kronos` refuses to boot and names the processor. The token store guard already
existed; the unit-of-work guard is new and rests on a sharper argument. A
tracking processor commits a cursor alongside whatever its handlers wrote, and a
silently-supplied bare factory makes those two unrelated effects: a crash landing
between them either replays an event the projection already applied or skips one
it never did. That boots clean and surfaces much later as a read model nobody
can explain — so it is a boot error instead. Subscribing processors keep no
cursor and need neither.

**Enhancement is host-side data transformation.** `enhance` is gone from
`kronos`, and the `handlerEnhancer` plumbing is gone from the processors and the
handling modules. `tracingEnhancer`/`meteringEnhancer` are replaced by
handler transformers named for what you get:

```ts
// before — one enhancer, applied to everything, invisibly
kronos({ enhance: (h, i) => tracing(metering(h, i), i), modules })  // gone

// after — a map, at the call site, over the handlers you choose
const handlers = billing.map(meteringHandler(recorder)).map(tracingHandler(spanFactory))
```

`HandlerEnhancer`, `HandlerMetadata` and `multiHandlerEnhancerDefinition` are
deleted. `openTelemetry()` now returns `{ spanFactory, tracingHandler }`, and
`openTelemetryMetrics()` returns a transformer.

**Tooling.** TypeScript 7 across the workspace.


**`state()` takes correlated-tuple data, not a callback DSL.** The `on()` pairing
helper and `EvolverRegistration` are DELETED, replaced by the same mapped-tuple
technique the append batch API already used:

```ts
// before — a builder callback whose only job was to pair the two
state({ evolve: (on) => [on(BillOpened, (s) => …), on(LineBilled, (s, m) => …)] })

// after — the pairing IS the data
state({ evolve: [
  [BillOpened, (s) => ({ ...s, open: true })],
  [LineBilled, (s, m) => ({ ...s, total: s.total + m.payload.amount })],
]})
```

Per-element inference is preserved: a wrong payload access is reported at the
offending tuple element, not on the array as a whole.
