# How it works

Seven concepts. Each section says what the thing is, and shows the code that is
it. Everything here is `@kronos-ts/core`.

## Messages and descriptors

A descriptor declares a message type: a qualified name, a Zod payload schema, an
optional result schema, and — for events — how tags come off the payload.

```ts
const ns = withNamespace("university")

const SubscribeStudent = ns.command("SubscribeStudent", {
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
})

const StudentSubscribed = ns.event("StudentSubscribed", {
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: { courseId: (p) => p.courseId, studentId: (p) => p.studentId },
})

const GetCourseView = ns.query("GetCourseView", {
  payload: z.object({ courseId: z.string() }),
})
```

`withNamespace(ns)` returns `{ command, event, query }`. The unprefixed
`command({ name: qn(ns, name), … })`, `event({ … })` and `query({ … })` do the
same with the qualified name written out.

`tags` is a **record of extractors**, and this shape is load-bearing. The
record's own keys *are* the tag keys, so the framework knows which tags an event
type carries without running anything — which is what lets a state derive its
DCB query per event type (below). A function form exists for tag sets a per-key
extractor cannot express — a variable number of tags, or a key that varies with
the payload — and then you must declare the keys yourself:

```ts
const ItemsRelabelled = event({
  name: qn("catalog", "ItemsRelabelled"),
  payload: z.object({ items: z.array(z.string()) }),
  tags: (p) => p.items.map((id) => tag("itemId", id)),
  tagKeys: ["itemId"],   // not derivable from a function — say it
})
```

Passing `tagKeys` alongside a `tags` record throws at definition time: they
cannot disagree, so only one of them may exist. An event with no `tags` at all
has a *known* key set — the empty one. An event with a `tags` function and no
`tagKeys` has an *unknown* one, and a state that folds it fails at boot rather
than guessing.

`query` is one exported binding doing two jobs — `query({ name, payload })`
declares a query type, `query(bus, descriptor, payload)` dispatches one. Arity
tells them apart: the declaration takes a single definition object, the dispatch
takes a bus first.

## The unit of work

`unitOfWork()` is the one primitive in core. It is scoped to a **task's
processing**, not to a message: a command bus opens one per command, an event
processor opens one per *batch*. So it holds no message and no message metadata.

It drives a fixed phase protocol:

```ts
export const Phase = {
  PRE_INVOCATION: -10000,   // transaction begin
  INVOCATION: 0,            // the handler runs
  POST_INVOCATION: 10000,
  PREPARE_COMMIT: 20000,    // event flush, token store write
  COMMIT: 30000,            // driver transaction commit
  AFTER_COMMIT: 40000,      // subscription updates, notifications
} as const
```

```ts
await unitOfWork().execute(async (uow) => {
  uow.onPrepareCommit(() => { /* flush the event buffer */ })
  uow.onCommit(async () => { /* commit the driver transaction */ })
  uow.onAfterCommit(() => { /* publish subscription updates */ })
  uow.onError((error, phase) => { /* rollback, compensate */ })
  uow.contributeCorrelationData({ traceparent })
})
```

`execute` drives the protocol; one unit of work executes exactly once. It also
carries `events` (the buffer `ctx.append` writes into, plus one `SourcingInfo`
per `ctx.load` — those two together become the DCB append condition),
`stateCache` (so a repeated `load` of the same state does not re-source, and an
`append` evolves the cached state), `correlationData()`, and `replaying`.

**There is no transaction surface here.** No `uow.transaction()`, no
`ctx.transaction`. A transaction is a fact about a driver and only the adapter
that owns the driver can type it, so each persistence package keeps its
transaction in package-private state keyed by the unit of work and exports a
typed accessor pair. Adapter authors reach the shared glue at
`@kronos-ts/core/transaction`; see [writing your own](writing-your-own.md).

The unit of work is **handed down, never ambient**. It reaches infrastructure as
a trailing parameter (`tokenStore.storeToken(name, token, uow)`), and it reaches
handlers only through the `ctx` built around it. Nothing reads it from a global,
which is why a capability used outside a handler is a compile error. A `ctx`
that outlives its unit of work throws `NoActiveUnitOfWork` off the `closed`
flag; a mutating capability called from a lifecycle hook throws `WrongUoWPhase`.

## Buses

A bus is a record of functions. That is the whole definition.

```ts
interface CommandBus {
  dispatch(message: CommandMessage): Promise<unknown>
  subscribe(commandName: string, handler: (m, uow) => Promise<unknown>): void
}
```

`dispatch` takes no unit of work, deliberately: every command — primary, or
dispatched from inside another handler via `ctx.send` — is handled in its **own
fresh** unit of work, so there is nothing to hand in. Commands compose by
independent commit, not by sharing a transaction. `QueryBus.query(message, uow?)`
*does* take one, because a consulting read legitimately nests inside the
caller's unit of work and its transaction.

`simpleCommandBus(unitOfWork)` and `simpleQueryBus(unitOfWork)` are the local
segments — an in-process handler map, plus the factory that opens a unit of work
per dispatch.

A transport is not a bus implementation you select. It is a function that takes
**your** local segment and returns a bus of the same shape:

```ts
rabbitMqCommandBus(rabbit, local)   // fork: local handler? go local. else the wire.
kronosDbCommandBus(kdb, local)      // inbound from the server runs through YOUR local bus
```

Core never learns the word "remote". Routing lives with the wire format that
determines it.

### Interception

One seam, one function:

```ts
type Intercept<M extends Message = Message> = (message: M) => M
```

Not a list, not a registry. Plurality composes in function space, where the
order is written down and readable:

```ts
const tenancy: Intercept<CommandMessage> = (m) => ({
  ...m,
  metadata: { ...m.metadata, tenantId: "acme" },
})

const commandBus = interceptingCommandBus(
  simpleCommandBus(unitOfWork),
  (m) => tenancy(lineage(m)),
)
```

It takes the whole message, not its metadata, because `causationId` is the
message's *identifier* and that does not appear in metadata.

The wrap goes on the **outside** of a transport —
`interceptingCommandBus(rabbitMqCommandBus(rabbit, local), lineage)` — so it
covers both branches of the transport's local/remote fork. Wrapping on the
inside is the classic lineage defect: commands that leave over the wire carry no
correlation at all.

### Lineage

```ts
export const lineage = <M extends Message>(message: M): M => ({
  ...message,
  metadata: {
    ...message.metadata,
    correlationId: String(message.metadata.correlationId ?? message.identifier),
    causationId: String(message.metadata.causationId ?? message.identifier),
  },
})
```

Both fields **seed**; neither clobbers. That `??` on `causationId` is the whole
point. `lineage` seeds *roots* — messages born at an edge with no cause. Every
subsequent hop is stamped by `ctx`: `ctx.send`, `ctx.query` and `ctx.append` all
take the handled message's own metadata as their base and merge the unit of
work's correlation data over it, so a command dispatched from a handler carries
the handler's message as its cause. An unconditional
`causationId: message.identifier` at the bus edge would overwrite that and
collapse the causal graph into self-loops.

Applying `lineage` twice is a no-op on both fields, which is what lets a
transport bus wrap a local segment that is itself intercepting.

## Handlers and the three contexts

A handler is a function of `(message, ctx)`. Which `ctx` you get is the safety
model — the capabilities you must not have are *absent from the type*, not
merely discouraged.

```ts
interface EventHandlerContext {
  load · schedule · scheduleAfter · cancelSchedule · send · query · emitUpdate
  isReplay() · contributeCorrelationData() · unitOfWork
}
interface HandlerContext extends EventHandlerContext { append }
interface QueryHandlerContext {
  load · query · contributeCorrelationData() · unitOfWork
}
```

**A query context cannot `send`.** Dispatching a command from a query breaks
command/query separation, so there is no `send` on it — and no `append`, because
a query must not write. `query` *is* there: a read composing another module's
read stays a read.

**An event context cannot `append`.** An automation is a stateful reactor: it
loads decision state and expresses intent as a *command*. Since `dispatch` always
opens a fresh unit of work, the command handler is its own atomic
decide-and-append boundary with the full DCB treatment. Appending from inside a
processor's unit of work would bypass that boundary, so the type makes it
unrepresentable.

```ts
const subscribe = commandHandler(SubscribeStudent, async ({ payload }, ctx) => {
  const s = await ctx.load(Subscription, payload)
  if (!s.registered) throw new Error("student is not registered")
  if (s.taken >= s.capacity) throw new Error("course is full")
  ctx.append(StudentSubscribed, payload)
})

const closeWhenFull = eventHandler(StudentSubscribed, async ({ payload }, ctx) => {
  const s = await ctx.load(Subscription, payload)
  if (s.taken >= s.capacity) await ctx.send(CloseEnrollment, { courseId: payload.courseId })
})

const getCourseView = queryHandler(GetCourseView, async ({ payload }) =>
  views.get(payload.courseId))
```

The context is built fresh per invocation as a closure over that invocation's
unit of work, buses and stores. `ctx.append` **buffers**; the command path's
PREPARE_COMMIT flush turns the buffer plus the sourcing infos into one
conditional event-store write — that write *is* the DCB consistency check.
`ctx.append` also evolves the cached state, so a handler that appends and then
loads sees its own writes.

`ctx.unitOfWork` is how a handler reaches a transaction:
`activeDrizzleTransaction(ctx.unitOfWork) ?? db` to write inside whatever is
already open, or `await drizzleTransaction(ctx.unitOfWork)` to open one.

## States and the derived DCB query

A state is a decision model: an id schema, an initial value, the tags it is
scoped by, and a list of `[EventDescriptor, fold]` pairs.

```ts
const Subscription = state({
  id: { courseId: z.string(), studentId: z.string() },
  initial: () => ({ capacity: 0, taken: 0, registered: false }),
  tags: (id) => ({ courseId: id.courseId, studentId: id.studentId }),
  evolve: [
    [CourseCreated,     (s, { payload }) => ({ ...s, capacity: payload.capacity })],
    [StudentRegistered, (s) => ({ ...s, registered: true })],
    [StudentSubscribed, (s) => ({ ...s, taken: s.taken + 1 })],
  ],
})
```

`evolve` is correlated-tuple **data**, not a builder. `S` is fixed by `initial`
before the array is checked, and each pair is checked against *its own*
descriptor rather than a union, so a wrong `payload` access is reported at that
pair.

You never write the query. It is derived, **per event type**, from tags × evolve:
for each folded type, the state's tag record is intersected with the tag keys
that type declares; the distinct intersections become the query's items (items
are ORed, tags within an item are ANDed); and a type joins every item whose tag
set it declares in full.

For the state above that derivation produces, verbatim:

```json
[
  { "tags": { "courseId": "cs-101" },
    "types": ["university.CourseCreated", "university.StudentSubscribed"] },
  { "tags": { "studentId": "stu-1" },
    "types": ["university.StudentRegistered", "university.StudentSubscribed"] }
]
```

That is the multi-stream DCB scope — an OR across two streams — from one plain
tag record, with no OR written by hand. **One record is the answer, including
for multi-stream states.** The array form of `tags` is an *override* that
replaces per-type derivation with a blunt OR (every record paired with every
folded type, intersection not consulted); reach for it only when you genuinely
need a scope the intersection cannot express.

Notice that `StudentSubscribed` rides the `courseId` item as well as its own.
That is the step that makes a capacity check correct: the state sees *other*
students' subscriptions to the same course. Pinning it to `courseId AND
studentId` would silently drop them — an under-sourced fold, and an append
condition too narrow to catch the very conflict it exists to catch.

Two failures are boot errors rather than silent no-ops. A folded event whose tag
keys are unknown (a `tags` function with no `tagKeys`) fails, because the query
cannot be scoped to it. A folded event sharing *no* tag key with the state fails,
because that fold can never fire — it is a modelling mistake.

`name` on a state is durable **snapshot identity** and nothing else. It is
optional until something would actually write a snapshot; `kronos` refuses to
boot a state configured with a snapshot policy or store that has no name.

## Event processors

A processor is a **value** — a record you can hold, compare, and hand to as many
event handlers as belong to it.

```ts
const projection = eventProcessor({ name: "course-views", eventStore, tokenStore, unitOfWork })
```

`name` is the durable identity: the tracking token is stored under it, so the
name survives restarts. `tokenStore` and `unitOfWork` are constitutive and never
defaulted — a missing token store boots fine and then replays the whole log on
every restart, and a non-transactional unit of work commits the projection write
and the token update as two unrelated effects. Both surface much later as a read
model nobody can explain.

### Sequencing is a function

```ts
type Sequence = (event: EventMessage) => string
```

Total: every event has a lane. Events sharing a lane are processed in order.

- **Absent** — global stream order, one lane. What a projection wants.
- **`sequentialPerTag("accountId")`** — one lane per account; different accounts
  proceed concurrently. The helper is one line, and its fallback is visible: an
  event without that tag lanes under its own event name.
- **`(e) => e.identifier`** — every event in a lane of its own, i.e. no ordering
  constraint at all. "No constraint" is not a missing answer; it is a lane
  function you can write down.

### Dead-letter queues park lanes

```ts
const balances = eventProcessor({
  name: "balances",
  eventStore, tokenStore,
  unitOfWork: drizzleUnitOfWork(db, unitOfWork),
  sequence: sequentialPerTag("accountId"),
  deadLetterQueue: drizzleDeadLetterQueue(db),
})
```

A queue parks the failed event **and everything behind it in the same lane** —
that is what preserves per-entity order across a poison pill. So "which lane" is
not optional once there is a queue, and `eventProcessor` throws at construction
when given a `deadLetterQueue` without a `sequence`. The honest global-order
answer for a projection is *no queue at all*: propagate and retry.

Absent a queue, a handler failure propagates and the batch retries.
`batchSize` (default 1) is how many events share one unit of work.

## Assembly

`kronos` takes four lists. There is no module, container, registry or enhancer.

```ts
const app = kronos({
  states: [{ ...Subscription, eventStore }],
  commandHandlers: [{ ...subscribe, eventStore, commandBus, queryBus }],
  queryHandlers: [{ ...getCourseView, queryBus }],
  eventHandlers: [
    { ...projectSubscribed, commandBus, queryBus, processor: projection },
    { ...closeWhenFull,     commandBus, queryBus, processor: automation },
  ],
})
```

An entry **points at** shared objects; a record **contains** its own parts.
`kronos` follows those references and never counts. There is no `stores: { … }`
bag, because nothing was behind the bag — it was four unrelated objects, so each
rides under its own name: `eventStore`, `snapshotStore`, `tagResolver`, plus
`commandBus`/`queryBus`/`processor` where they apply, and an optional `name` used
only in boot errors.

Two grouping rules, and they are different on purpose:

- **Stores group by object identity.** Two entries share one repository set and
  one stream because they name the *same* `eventStore` object. The snapshot store
  and tag resolver of a group come from the first entry seen for that log.
- **Processors group by NAME.** A token persists under a processor's name, so two
  entries naming `"balances"` *are* one delivery even when separate modules built
  their own equal values. Equal configs merge. Conflicting ones are a boot error
  naming both entries and listing the fields that disagree — the alternative is
  two processors fighting over one cursor row. Objects and functions in that
  comparison are compared by identity, because that is what they mean here: two
  `sequence` functions lane identically only if they are the same function.

Boot errors name the offending entry (by its `name`, else its descriptor):
a state or command handler with no `eventStore`; an event handler with no
`processor`; a state given a snapshot policy but no `name`; conflicting processor
configuration. A query handler with no event store is fine — a read model served
from a projection table needs no log to answer.

Start is two-phase: every handler is subscribed before *any* processor runs, so
an automation replaying from a cold store can never dispatch to a command whose
handler is not yet subscribed.

```ts
interface App {
  readonly processors: ReadonlyMap<string, RunningProcessor>
  stop(): Promise<void>
}
```

`app.processors` is keyed by that same durable name. A `RunningProcessor` carries
`running` / `position` / `replaying`, `status()`, `start()` / `stop()`,
`resetTokens(from?)` and `reprocessDeadLetters(filter?)` — what a control plane
drives and what a test awaits.

---

Next: [building an application](building-an-application.md) for the composition
root and the slice convention, or [testing](testing.md).
