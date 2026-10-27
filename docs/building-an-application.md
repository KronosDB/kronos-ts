# Building an application

A walkthrough of a real composition, from a domain slice to a running process.

**Read this first.** Two of the things below are *not* the framework:

| | What it is |
| --- | --- |
| `state`, `commandHandler`, `queryHandler`, `eventHandler`, `eventProcessor`, `kronos`, `send`, `query` | **Library.** `@kronos-ts/core`. |
| `slice(...)`, `module(...)`, the `controller.ts` / `slice.ts` / `index.ts` folder shape | **House style.** Ordinary functions you write in your own repo. Documented here because it is the reference style and it works; nothing in core knows about it. |

There is no module concept in the framework. `kronos` takes four flat lists. If
you like a different grouping function, write a different one — the only thing
that has to come out the other end is four arrays.

## 1. A slice is a plain value

A slice is a zero-argument export. It knows its own domain and nothing about
deployment: no event store, no buses, no cursor.

```ts
const ns = withNamespace("billing")

const OpenAccount = ns.command("OpenAccount", { payload: z.object({ accountId: z.string() }) })
const Charge = ns.command("Charge", {
  payload: z.object({ accountId: z.string(), amount: z.number() }),
})
const AccountOpened = ns.event("AccountOpened", {
  payload: z.object({ accountId: z.string() }),
  tags: { accountId: (p) => p.accountId },
})
const Charged = ns.event("Charged", {
  payload: z.object({ accountId: z.string(), amount: z.number() }),
  tags: { accountId: (p) => p.accountId },
})
const GetBalance = ns.query("GetBalance", { payload: z.object({ accountId: z.string() }) })

const Account = state({
  id: { accountId: z.string() },
  tags: (id) => ({ accountId: id.accountId }),
  evolve: [
    () => ({ open: false, balance: 0 }),
    [AccountOpened, (s) => ({ ...s, open: true })],
    [Charged, (s, { payload }) => ({ ...s, balance: s.balance - payload.amount })],
  ],
})

const charge = commandHandler(Charge, async ({ payload }, ctx) => {
  const account = await ctx.load(Account, { accountId: payload.accountId })
  if (!account.open) throw new Error("no such account")
  ctx.append(Charged, payload)
})
```

Everything above is module-private `const`. Only the slice value is exported —
and note that `Account` appears in NO list. `ctx.load` is handed the state value
at the call site, so a state is data the handler closes over, never something
the app registers.

### `slice()` — your constructor

All it does is normalise the three arrays so callers never write `?? []`:

```ts
export function slice(def: Partial<Slice>): Slice {
  return {
    commandHandlers: def.commandHandlers ?? [],
    queryHandlers: def.queryHandlers ?? [],
    eventHandlers: def.eventHandlers ?? [],
  }
}
```

### The processor is a partially applied function

Here is the one interesting choice. An event handler entry in a slice carries a
`processor` that is a **function**, not a value. The slice closes out the
semantics it owns — the delivery's name, its lane function, whether it parks
poison pills — and leaves the *infrastructure* as the parameter list:

```ts
type ProcessorFor = (
  eventStore: EventStore,
  tokenStore: TokenStore,
  unitOfWork: () => UnitOfWork,
  deadLetterQueue: SequencedDeadLetterQueue,
) => EventProcessor
```

A projection wants global order and no queue, so it just does not declare the
trailing parameter — TypeScript assignability lets a shorter function stand in
for a longer type, so declining an argument needs no syntax at all:

```ts
export const billingSlice = slice({
  commandHandlers: [openAccount, charge],
  queryHandlers: [getBalance],
  eventHandlers: [
    {
      handler: projectCharged,
      processor: (eventStore, tokenStore, uow) => eventProcessor("billing-balances", eventStore, tokenStore, uow),
    },
  ],
})
```

An automation that must preserve per-account order and park poison pills takes
all four:

```ts
export const settlementSlice = slice({
  eventHandlers: [
    {
      handler: settleCharge,
      processor: (eventStore, tokenStore, uow, deadLetterQueue) =>
        deadLettering(
          eventProcessor("billing-settlement", eventStore, tokenStore, uow),
          deadLetterQueue,
          (e) => e.tags.find((t) => t.key === "accountId")?.value ?? e.identifier,
        ),
    },
  ],
})
```

The parameter list *is* the semantic declaration. A reader sees at a glance that
the projection takes no queue and therefore runs in global order, and that the
automation parks per account. Nothing checks a flag at boot.

## 2. A module is your grouping function

Storage in, lists out. It creates the resources it owns, names them as consts,
and contributes through plain `flatMap`/`map` chains.

```ts
export function billingModule(eventStore: EventStore): ModuleLists {
  const pg = postgresPool(postgresAdapter({ connectionString: process.env.DATABASE_URL! }))
  const tokenStore = postgresTokenStore(pg)
  const deadLetterQueue = postgresDeadLetterQueue(pg)
  const uow = postgresUnitOfWork(unitOfWork, pg)
  const slices = [billingSlice, settlementSlice]

  // The one persistence step a slice takes: its query builder over the
  // task's transaction. `drizzleHandler` is THIS APP's dozen-line wrapper
  // (see `@kronos-ts/postgres`'s README, "On `ctx.db`, as a handler
  // wrapper"); handlers then read `ctx.db` with the Drizzle types the app
  // installed.
  const wrap = (h) => postgresHandler(drizzleHandler(h), pg)

  return {
    commandHandlers: slices.flatMap((s) => s.commandHandlers).map((h) => ({ ...h, handler: wrap(h.handler), eventStore })),
    queryHandlers: slices.flatMap((s) => s.queryHandlers).map((h) => ({ ...h, handler: wrap(h.handler), eventStore })),
    eventHandlers: slices.flatMap((s) => s.eventHandlers).map((e) => ({
      ...e.handler,
      handler: wrap(e.handler.handler),
      processor: e.processor(eventStore, tokenStore, uow, deadLetterQueue),
    })),
  }
}
```

Note the transaction identity: `tokenStore`, `deadLetterQueue` and the unit-of-work
factory all come off the **same** `pg` pool, and `ctx.db` is Drizzle over that
task's transaction. That is not a convention — it is the rule that makes a
projection write and its cursor update one transaction. Kronos ships one
persistence family, postgres; a query builder is a client you construct over
its transaction, never the owner of one.

The module attaches storage but **not buses**. Which bus stack a process runs is
a property of the process, not of the module.

## 3. The composition root

Three moves: resources, bus stacks, three flatMaps.

```ts
const eventStore = inMemoryEventStore()

const commandBus = interceptingCommandBus(localCommandBus(unitOfWork), correlation)
const queryBus = interceptingQueryBus(localQueryBus(unitOfWork), correlation)

const modules = [billingModule(eventStore)]

export const app = kronos({
  commandHandlers: modules.flatMap((m) => m.commandHandlers).map((h) => ({ ...h, commandBus, queryBus })),
  queryHandlers: modules.flatMap((m) => m.queryHandlers).map((h) => ({ ...h, queryBus })),
  eventHandlers: modules.flatMap((m) => m.eventHandlers).map((h) => ({ ...h, commandBus, queryBus })),
})
```

**The command bus and processors take the transaction family; the query bus
takes the plain `unitOfWork`.** A command's decision and its append have to
commit together, and a processor's projection write and its cursor update have
to as well — that is what `postgresUnitOfWork` is for. A query is different: it is always its own task (see
[how it works](how-it-works.md#a-query-is-always-its-own-task)), so it needs no
transaction to join, and building `localQueryBus` from the persistence
family's factory would only mint transactions a read never uses:

```ts
const uow = postgresUnitOfWork(unitOfWork, pg)  // the command bus and processors want this
const commandBus = interceptingCommandBus(localCommandBus(uow), correlation)
const queryBus = interceptingQueryBus(localQueryBus(unitOfWork), correlation)   // a read needs no transaction
```

### Making metadata propagate

Nothing rides from one message to the next unless you compose it. One wrapper,
around every handler, does it — no change to the task at all:

```ts
export const app = kronos({
  commandHandlers: modules.flatMap((m) => m.commandHandlers)
    .map((h) => ({ ...h, handler: correlatingHandler(h.handler) }))
    .map((h) => ({ ...h, commandBus, queryBus })),
  // …the same wrap on queryHandlers and eventHandlers
})
```

`correlatingHandler(h.handler)` gives back a handler that carries `messageOrigin`
— the default cargo: the chain is inherited or seeded, the cause is the parent,
unconditionally — onto everything it gives birth to. The cargo lives in the
invocation's own closure, not on the unit of work, so wrapping the handlers
changes nothing about what they wire against: `commandBus: localCommandBus(unitOfWork)`
still typechecks, wrapped or not. See
[how it works](how-it-works.md#correlation-the-functions-you-wrap-in).

### Bus stacks are nesting, not configuration

Each wrapper is a function from a bus to a bus, so the stack is written in the
order it applies, outermost last:

```ts
const local = localCommandBus(unitOfWork)                    // in-process handler map
const wired = rabbitMqCommandBus(local, rabbit)               // fork: local, else the wire
const traced = otlpCommandBus(wired, exporter)                // span per dispatch
const commandBus = interceptingCommandBus(traced, correlation)    // correlation over BOTH branches
```

`interceptingCommandBus` goes **outside** the transport, always. Inside, only the
local branch reaches it and everything routed over the wire leaves with no
correlation.

Swapping infrastructure is swapping resources in this file:

```ts
// before
const eventStore = inMemoryEventStore()

// after
const pool = postgresPool(process.env.DATABASE_URL!)
await pool.start()
const eventStore = postgresEventStore(pool)
```

Nothing above the composition root changes. That is the whole payoff of entries
pointing at objects instead of naming them.

### Two logs, two deliveries — no configuration

Topology is which literals share which objects. Two modules on separate logs is
literally two objects:

```ts
const billingLog = postgresEventStore(billingPool)
const catalogLog = postgresEventStore(catalogPool)

const modules = [billingModule(billingLog), catalogModule(catalogLog)]
```

`kronos` groups by object identity, so those are two repository sets and two
streams. Nobody declared a group.

## 4. Edges

An edge is where a message is **born**. That is the only place per-request
metadata can enter, because it cannot be reconstructed downstream.

```ts
export async function openAccountEndpoint(actor: string, accountId: string) {
  const metadata: Metadata = { actor, requestId: crypto.randomUUID() }
  return send(commandBus, OpenAccount, { accountId }, metadata)
}
```

`send(bus, descriptor, payload, metadata?)` and
`query(bus, descriptor, payload, metadata?)` are the two verbs. There is nothing
called a gateway: a gateway was an object with one method closing over a bus, and
these are the same operation with the bus as the first argument. Partially apply
it yourself if you want the bus fixed.

**Stamp the actor on both verbs.** It is easy to remember on `send` and easy to
forget on `query`, and a read that cannot say who asked is a read you cannot
audit or authorise:

```ts
// before — the read is anonymous
await query(queryBus, GetBalance, { accountId })

// after
await query(queryBus, GetBalance, { accountId }, { actor, requestId })
```

From there it rides forward only if you say so. `ctx.send`, `ctx.query`,
`ctx.append` and `ctx.schedule` give a message exactly the metadata they were
handed — nothing is carried behind your back. What makes metadata JUMP from the
message a handler is handling onto everything that handling births is
`correlatingHandler(next, from?)`, and `from` — a function of the handled
message — defaults to `messageOrigin`:

```ts
correlatingHandler(h.handler)   // messageOrigin: correlationId/causationId, traceparent when present

// the same, plus this host's own per-request facts
correlatingHandler(h.handler, (m) => ({
  ...messageOrigin(m),
  actor: String(m.metadata.actor ?? ""),
}))
```

One wrapper, one function, and `actor` now reaches appended events and
downstream commands across any transport — because it is on the message.

`subscriptionQuery(bus, descriptor, payload, metadata?)` is the third verb, for
an initial result plus the stream of updates handlers push with `ctx.emitUpdate`.

### The edge is yours

The edge verbs are *unclosed on purpose* — `send` takes its bus, its descriptor,
its payload and its metadata, and nothing is conjured. That is not an
inconvenience to work around; it is the seam where a host says, once, what
entering this system means. Close it out in one file:

```ts
export const dispatch = (d, payload, actor) => send(commandBus, d, validate(d, payload), { actor })
export const ask      = (d, payload, actor) => query(queryBus,  d, validate(d, payload), { actor })
```

```ts
// before — every controller repeats the bus, the parse and the metadata,
// and the one that forgets `actor` on the read is the one you find in an audit
await query(queryBus, GetBalance, GetBalance.payload.parse(req.body))

// after
await ask(GetBalance, req.body, actor)
```

Controllers never see the unclosed verb, so validation and per-request metadata
are one visible decision in one file.

Note what `validate(d, payload)` returns: **the parsed value**, which is what
gets sent. A schema's coercions and defaults are part of what it says, so the
message carries what the schema produced, not what arrived over the wire. And
because the edge can `await`, a schema with an asynchronous refinement works
here — `dispatch` and `ask` are already async.

This is the same mechanism `validatingHandler(next, descriptor)` composes at the
entry, applied one boundary out. Composing both is not redundant: the edge is
where a *request* becomes a message, and the entry is where a message from
*anywhere* — a transport, a replay, another service — becomes a handling.

## 5. The slice folder convention

Wire edges live *beside* domain code, never in it.

```
billing/charge/
  controller.ts   ← the wire edge: HTTP/oRPC/pubsub route → send(...) / query(...)
  slice.ts        ← the domain: messages, state, handlers, top-down
  index.ts        ← ties them together
```

`slice.ts` never imports a transport. It does not know whether it is reached over
HTTP, a queue or a test fixture — which is exactly why the same composition-root
function drops into `testFixture` unchanged. `controller.ts` is where the actor comes off the request
and becomes metadata. When the wire changes — oRPC today, a pubsub subscriber
next year — one file changes.

`index.ts` exports the slice value and registers the controller. Keeping them in
separate files rather than separate trees means the wire edge for a behaviour is
next to the behaviour, not in a distant `routes/` directory that has to be kept
in sync by hand.

## 6. Recipe: a projection that writes once per batch

**House style, not the framework.** Nothing below is exported by kronos-ts. It is
one way to build it out of what is: a batch is one unit of work, the unit of work
is on `ctx`, and `onPrepareCommit` runs before the processor stores its token.
Rename it, cut it down, or write it differently.

The handler reads and saves a row as if it were the table:

```ts
type Ctx = EventHandlerContext & Repositories<{ balances: typeof balances }>

export const balanceHandlers = [
  eventHandler(Deposited, async ({ payload }, ctx: Ctx) => {
    const b = await ctx.balances.get(payload.accountId)
    await ctx.balances.save({ ...b, total: b.total + payload.amount })
  }),
  eventHandler(Withdrawn, async ({ payload, timestamp }, ctx: Ctx) => {
    const b = await ctx.balances.get(payload.accountId)
    const total = b.total - payload.amount
    await ctx.balances.save({ ...b, total, overdrawnSince: total < 0 ? (b.overdrawnSince ?? timestamp) : null })
  }),
]
```

and the batch decides when the table is read and written:

```ts
const byAccount = (e: EventMessage) => e.tags.find((t) => t.key === "accountId")?.value ?? e.identifier

export const balancesProcessor = pipe(
  eventProcessor("balances", eventStore, postgresTokenStore(pg), postgresUnitOfWork(unitOfWork, pg)),
  (p) => batched(p, 1000),          // up to 1000 ready events per unit of work
  (p) => concurrent(p, byAccount),  // the batch's accounts side by side, so their gets arrive together
)

const projection = (h) => pipe(h, (x) => repositoryHandler(x, { balances }), (x) => postgresHandler(x, pg))
```

The repository, next to your `drizzleHandler`:

```ts
// app/repository.ts
import { getTableColumns, inArray, is, sql, SQL } from "drizzle-orm"
import { drizzle, type PostgresJsDatabase } from "drizzle-orm/postgres-js"
import type { PgColumn, PgTable } from "drizzle-orm/pg-core"
import type { Sql } from "postgres"
import type { UnitOfWork } from "@kronos-ts/core"
import type { PostgresCapability } from "@kronos-ts/postgres"

type Row<T extends PgTable> = T["$inferSelect"]
type Key<T extends PgTable> = {
  [K in keyof T["_"]["columns"]]: T["_"]["columns"][K]["_"]["isPrimaryKey"] extends true ? T["_"]["columns"][K]["_"]["data"] : never
}[keyof T["_"]["columns"]]

export type Repository<T extends PgTable> = {
  /** This batch's row, else the stored one, else a new one from the table's column defaults. */
  get(id: Key<T>): Promise<Row<T>>
  /** Replaces this batch's row; written with the token when the batch commits. */
  save(row: Row<T>): Promise<void>
}
export type Repositories<T extends Record<string, PgTable>> = { readonly [K in keyof T]: Repository<T[K]> }

type Kept = { rows: Map<unknown, any>; changed: Set<unknown>; asked: Map<unknown, Array<[(row: any) => void, (error: unknown) => void]>> }

const batches = new WeakMap<UnitOfWork, Map<PgTable, Kept>>()

export const repositoryHandler =
  <T extends Record<string, PgTable>, M, C extends Repositories<T>>(next: (message: M, ctx: C) => Promise<void>, tables: T) =>
  async (message: M, ctx: Omit<C, keyof T> & PostgresCapability & { unitOfWork: UnitOfWork }) => {
    const db = drizzle(ctx.sql().unwrap<Sql>())
    let kept = batches.get(ctx.unitOfWork)
    if (!kept) {
      const batch = new Map(Object.values(tables).map((t) => [t, { rows: new Map(), changed: new Set(), asked: new Map() }]))
      batches.set(ctx.unitOfWork, (kept = batch))
      // Before the token, in the same transaction. A failed batch never gets here.
      ctx.unitOfWork.onPrepareCommit(async () => {
        for (const [table, { rows, changed }] of batch) {
          if (changed.size === 0) continue
          const { key, set } = shape(table)
          await db.insert(table).values([...changed].map((id) => rows.get(id))).onConflictDoUpdate({ target: key, set })
        }
      })
    }
    const repositories = Object.fromEntries(Object.entries(tables).map(([name, t]) => [name, repository(db, t, kept.get(t)!)]))
    return next(message, { ...ctx, ...repositories } as unknown as C)
  }

function repository(db: PostgresJsDatabase, table: PgTable, kept: Kept): Repository<PgTable> {
  const { key, keyName, fresh } = shape(table)
  return {
    async get(id) {
      if (kept.rows.has(id)) return kept.rows.get(id)
      return new Promise((answer, fail) => {
        const first = kept.asked.size === 0
        kept.asked.set(id, [...(kept.asked.get(id) ?? []), [answer, fail]])
        // Every get asked in this turn of the event loop, in one select.
        if (first) setImmediate(async () => {
          const asked = new Map(kept.asked)
          kept.asked.clear()
          try {
            const found = await db.select().from(table).where(inArray(key, [...asked.keys()]))
            for (const row of found) if (!kept.rows.has(row[keyName])) kept.rows.set(row[keyName], row)
            for (const [id, waiting] of asked) {
              if (!kept.rows.has(id)) kept.rows.set(id, { ...fresh, [keyName]: id })
              for (const [done] of waiting) done(kept.rows.get(id))
            }
          } catch (error) {
            for (const waiting of asked.values()) for (const [, failed] of waiting) failed(error)
          }
        })
      })
    },
    async save(row) {
      kept.rows.set(row[keyName], row)
      kept.changed.add(row[keyName])
    },
  }
}

/** The key, the conflict update, and what a new row starts as — read off the table. */
function shape(table: PgTable) {
  const columns = Object.entries(getTableColumns(table)) as Array<[string, PgColumn]>
  const [keyName, key] = columns.find(([, c]) => c.primary)!
  const others = columns.filter(([, c]) => c !== key)
  const fresh = Object.fromEntries(others.map(([name, c]) => {
    if (c.default !== undefined && !is(c.default, SQL)) return [name, c.default]
    if (!c.notNull) return [name, null]
    throw new Error(`a new row has nothing to start at: "${c.name}" is NOT NULL without a default`)
  }))
  const set = Object.fromEntries(others.map(([name, c]) => [name, sql`excluded.${sql.identifier(c.name)}`]))
  return { key, keyName, fresh, set }
}
```

Left out for length: one-column primary keys only, and a batch over 65,535
parameters needs its insert split (the replay benchmark's
`integrationtests/src/benchmarks/processor-replay/repository.ts` does both).

How it holds together:

- **Rows and token commit together.** The write is in `onPrepareCommit`, which the
  processor's own token write follows. A failed batch rolls both back, and its
  rows die with its unit of work, so nothing is folded twice.
- **At the head it is as fresh as before.** A batch is only what is ready — one or
  two events — so it writes at once.
- **Reads are one statement per batch** because `concurrent` starts every bucket
  together: their first `get`s land in the same turn. Without `concurrent` the
  handler still works, one read per row per batch.
- **The bucket must cover every row the handler touches.** Buckets run side by
  side; two buckets writing one row would race.
- **Dead letters** park cleanly if the handler throws before `save`.
  `deadLettering(p, queue, byAccount)` takes the same function as `concurrent`.

Variations, all yours to choose:

- **Deltas.** A fold that never needs the old row (a counter, last-write-wins)
  can skip `get` and write `total = total + excluded.total`. No read at all.
- **A fold queued for the end of the batch** — the handler hands over
  `(row) => row` and never waits for it. All of a batch's reads still become one,
  without `concurrent`, for handlers whose rows cross buckets. The handler cannot
  see the result, and a throwing fold fails the batch rather than parking.

---

Next: [testing](testing.md), or the package guides under
[`docs/packages/`](packages/).
