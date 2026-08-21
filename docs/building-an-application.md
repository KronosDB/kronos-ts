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
      processor: (eventStore, tokenStore, uow) =>
        eventProcessor({ name: "billing-balances", eventStore, tokenStore, unitOfWork: uow }),
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
        eventProcessor({
          name: "billing-settlement",
          eventStore, tokenStore, unitOfWork: uow,
          sequence: sequentialPerTag("accountId"),
          deadLetterQueue,
        }),
    },
  ],
})
```

The parameter list *is* the semantic declaration. A reader sees at a glance that
the projection takes no queue and therefore runs in global order, and that the
automation lanes per account. Nothing checks a flag at boot.

## 2. A module is your grouping function

Storage in, lists out. It creates the resources it owns, names them as consts,
and contributes through plain `flatMap`/`map` chains.

```ts
export function billingModule(eventStore: EventStore): ModuleLists {
  const db = drizzle(postgres(process.env.DATABASE_URL!))
  const tokenStore = drizzleTokenStore(db)
  const deadLetterQueue = drizzleDeadLetterQueue(db)
  const uow = drizzleUnitOfWork(unitOfWork, db)
  const slices = [billingSlice, settlementSlice]

  return {
    commandHandlers: slices.flatMap((s) => s.commandHandlers).map((h) => ({ ...h, eventStore })),
    queryHandlers: slices.flatMap((s) => s.queryHandlers).map((h) => ({ ...h, eventStore })),
    eventHandlers: slices.flatMap((s) => s.eventHandlers).map((e) => ({
      ...e.handler,
      processor: e.processor(eventStore, tokenStore, uow, deadLetterQueue),
    })),
  }
}
```

Note the transaction identity: `tokenStore`, `deadLetterQueue` and the unit-of-work
factory all come off the **same** `db` handle, which is also the handle the
projections write their read models through. That is not a convention — it is the
rule that makes a projection write and its cursor update one transaction. See
[the persistence packages](packages/drizzle.md).

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

### Making metadata propagate

Nothing rides from one message to the next unless you compose it. Two lines
turn the root above into one where it does — a unit of work that can carry a
map, and a wrapper that fills it per invocation:

```ts
const correlationFrom = (parent: Message): Metadata => ({
  correlationId: String(parent.metadata.correlationId ?? parent.identifier),
  causationId: String(parent.identifier),
})

const uow = () => correlating(unitOfWork)                     // ← a task that carries
const commandBus = interceptingCommandBus(localCommandBus(uow), correlation)
const queryBus = interceptingQueryBus(localQueryBus(uow), correlation)

export const app = kronos({
  commandHandlers: modules.flatMap((m) => m.commandHandlers)
    .map((h) => ({ ...h, handler: correlatingHandler(h.handler, correlationFrom) }))
    .map((h) => ({ ...h, commandBus, queryBus })),
  // …the same wrap on queryHandlers and eventHandlers
})
```

Wrap the handlers and forget the `correlating` — the compiler will not let you.
A wrapped handler asks for a correlating unit of work, and a bus built from the
bare `unitOfWork` cannot give it one. See
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
const eventStore = postgresEventStore(pool, { tagResolver: descriptorBasedTagResolver() })
```

Nothing above the composition root changes. That is the whole payoff of entries
pointing at objects instead of naming them.

### Two logs, two deliveries — no configuration

Topology is which literals share which objects. Two modules on separate logs is
literally two objects:

```ts
const billingLog = postgresEventStore(billingPool, opts)
const catalogLog = postgresEventStore(catalogPool, opts)

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
`correlatingHandler(next, from)`, and `from` is the function that names the
cargo:

```ts
// the pair, written where it is used — two lines, yours
const correlationFrom = (parent: Message): Metadata => ({
  correlationId: String(parent.metadata.correlationId ?? parent.identifier),
  causationId: String(parent.identifier),
})

correlatingHandler(h.handler, correlationFrom)

// the same, plus this host's own per-request facts
correlatingHandler(h.handler, (m) => ({
  ...correlationFrom(m),
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

---

Next: [testing](testing.md), or the package guides under
[`docs/packages/`](packages/).
