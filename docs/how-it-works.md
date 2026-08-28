# How it works

Seven concepts. Each section says what the thing is, and shows the code that is
it. Everything here is `@kronos-ts/core`.

## Messages and descriptors

A descriptor declares a message type: a qualified name, a payload schema, an
optional result schema, and — for events — how tags come off the payload.

The payload schema is any [Standard Schema](https://standardschema.dev) — zod,
valibot, arktype, or one you write by hand. Core depends on none of them; the
examples here use zod because it is what most people reach for, and the
inference is the same whichever you pick.

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

### The four faces of a descriptor

A descriptor is not a type declaration that then gets out of the way. It is the
value you keep passing, and it has exactly four faces — every one of them a
function of `(descriptor, …)`:

| Face | What it looks like | What it is |
| --- | --- | --- |
| **DECLARE** | `const SubscribeStudent = ns.command("SubscribeStudent", { payload })` | the descriptor is born |
| **CONSTRUCT** | `send(bus, SubscribeStudent, payload, metadata)` · `ctx.append(StudentSubscribed, payload)` | hand it the parts, get a message |
| **ACCEPT** | `commandHandler(SubscribeStudent, ({ payload }) => …)` | take its message apart |
| **MATCH** | `is(message, StudentSubscribed)` | ask whether a message is one of these |

Read that column again: the descriptor is an **argument** at every face except
the first. That is why validation needs no registry and no `.register()` call —
a schema registry exists to answer "which schema goes with this type name", and
at every site where the question could arise the answer is already in your hand.
`validate(descriptor, payload)` is the whole mechanism, and
`validatingHandler(next, descriptor)` is it composed at the entry. See
[the validation section](#validation-the-gate) and
[the edge idiom](building-an-application.md#the-edge-is-yours).

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
})
```

`execute` drives the protocol; one unit of work executes exactly once. It also
carries `events` (the buffer `ctx.append` writes into, plus one `SourcingInfo`
per `ctx.load` — those two together become the DCB append condition),
`stateCache` (so a repeated `load` of the same state does not re-source, and an
`append` evolves the cached state), and `replaying`.

**There is no correlation surface here either.** A unit of work is pure task
lifecycle; carrying metadata from one message to the next is a capability you
*compose* — `correlating(unitOfWork())` — and the section below is the whole of
it.

**There is no transaction surface here.** No `uow.transaction()`, no
`ctx.transaction`. A transaction is a fact about a driver and only the adapter
that owns the driver can type it, so each persistence package keeps its
transaction in package-private state keyed by the unit of work and exports a
typed accessor pair. The registry/factory/accessor glue behind that pair is
package-private too: it only ever needed the PUBLIC phase API (`uow.on(Phase.
COMMIT, …)`, `uow.onError(…)`), which made it a helper, and each persistence
package owns its own copy — tuned to whether that family binds its transaction
eagerly (drizzle, knex, kysely, prisma, typeorm) or lazily (postgres).

The unit of work is **handed down, never ambient**. It reaches infrastructure as
a trailing parameter (`tokenStore.storeToken(name, token, uow)`), and it reaches
handlers only through the `ctx` built around it. Nothing reads it from a global,
which is why a capability used outside a handler is a compile error. A `ctx`
that outlives its unit of work throws `NoActiveUnitOfWork` off the `closed`
flag; a mutating capability called from a lifecycle hook throws `WrongUoWPhase`.

## Buses

A bus is a record of functions. That is the whole definition.

```ts
type CommandBus = {
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

`localCommandBus(unitOfWork)` and `localQueryBus(unitOfWork)` are the local
segments — an in-process handler map, plus the factory that opens a unit of work
per dispatch.

A transport is not a bus implementation you select. It is a function that takes
**your** local segment and returns a bus of the same shape:

```ts
rabbitMqCommandBus(local, rabbit)   // fork: local handler? go local. else the wire.
kronosDbCommandBus(local, kdb)      // inbound from the server runs through YOUR local bus
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
  localCommandBus(unitOfWork),
  (m) => tenancy(correlation(m)),
)
```

It takes the whole message, not its metadata, because `causationId` is the
message's *identifier* and that does not appear in metadata.

The wrap goes on the **outside** of a transport —
`interceptingCommandBus(rabbitMqCommandBus(local, rabbit), correlation)` — so it
covers both branches of the transport's local/remote fork. Wrapping on the
inside is the classic correlation defect: commands that leave over the wire carry no
correlation at all.

### Correlation

```ts
export const correlation = <M extends Message>(message: M): M => ({
  ...message,
  metadata: {
    ...message.metadata,
    correlationId: String(message.metadata.correlationId ?? message.identifier),
    causationId: String(message.metadata.causationId ?? message.identifier),
  },
})
```

Both fields **seed**; neither clobbers. That `??` on `causationId` is the whole
point. `correlation` seeds *roots* — messages born at an edge with no cause.
Every subsequent hop is stamped by a wrapped HANDLER, not by the bus (next
section). An unconditional `causationId: message.identifier` at the bus edge
would overwrite that and collapse the causal graph into self-loops.

Applying `correlation` twice is a no-op on both fields, which is what lets a
transport bus wrap a local segment that is itself intercepting.

## Correlation: the functions you wrap in

**Correlation is the carrying mechanism** — metadata jumping from the message a
handler is handling onto every message that handling gives birth to, and from
there onto everything *those* births cause. The correlationId/causationId pair
is the *default cargo* of that mechanism, not the mechanism itself.

Core carries nothing. `ctx.send`, `ctx.query`, `ctx.append`, `ctx.schedule` and
`ctx.scheduleAfter` each take a trailing `metadata?`, and a birth's metadata is
*exactly* that argument. Two functions turn that into propagation:

```ts
// 1. a task that can carry a map
const uow = () => correlating(unitOfWork(clock))

// 2. a handler that fills it and overlays it onto every birth
correlatingHandler(h.handler, correlationFrom)
```

`correlationFrom` here is not an import — it is YOUR two lines, and writing them is the whole lesson:

```ts
const correlationFrom = (parent: Message): Metadata => ({
  correlationId: String(parent.metadata.correlationId ?? parent.identifier),
  causationId: String(parent.identifier),
})
```

The asymmetry is the rule. `correlationId` is the **chain**: inherited when the
parent has one, seeded from the parent otherwise. `causationId` is the
**parent**, unconditionally — never the parent's own causationId, which would
name the grandparent and collapse the chain. So an automation's dispatched
command is caused by the *event it reacted to*, not by the command that appended
that event.

`from` is a plain `(message) => Metadata` and it is **required** — the mechanism
has no opinion about what is worth carrying, and a default would decide that for
every host. More cargo is more function:

```ts
correlatingHandler(h.handler, (m) => ({
  ...correlationFrom(m),
  actor: String(m.metadata.actor ?? ""),
}))
```

### The demand is conditional

Wrapping a handler gives back one that asks for `ctx.unitOfWork:
CorrelatingUnitOfWork` — the demand is on the wrapper's *output*, so the handler
itself never names a task. Buses and processors are parametric in what their
factory mints, and the entry types tie the two together — so wiring a wrapped
handler against a bus built from a bare `() => unitOfWork()` is a **compile
error**:

```ts
kronos({
  commandHandlers: [{
    ...h,
    handler: correlatingHandler(h.handler, correlationFrom),
    commandBus: localCommandBus(unitOfWork),   // ← error: mints bare units of work
  }],
})
```

Wrap your handlers and the compiler makes you wrap your unit of work. Wrap
neither and the word never appears in your types: every generic defaults to the
bare `UnitOfWork`, and an uncorrelated app reads exactly as it always did. That
conditionality is the whole design — an *unconditional* demand propagates
contravariantly through every transport, which is why an earlier attempt to
hardcode correlation into `ctx` and the bus signatures had to be reverted.

## Validation: the gate

The fourth mechanism, and the same shape as the other three: a function you
compose, not a thing you configure. It is two exports.

```ts
validate(descriptor, payload)              // the primitive, anywhere
validatingHandler(next, descriptor)        // the mechanism, at the entry
```

**There is no registry, and nothing to register.** Look back at the four faces: a
descriptor is an *argument* at every site where validation could be a question,
and it carries its own payload schema. A schema registry exists to answer "which
schema goes with this type name" — a question that only arises somewhere holding
a name and not a descriptor, which used to be the serializer. So the serializer
went back to doing one thing (encoding) and the check moved to where the
descriptor already is.

```ts
kronos({
  commandHandlers: handlers
    .map((h) => ({ ...h, handler: validatingHandler(h.handler, h.descriptor) }))
    .map((h) => ({ ...h, commandBus, queryBus, eventStore })),
})
```

Wrapped, a handling is gated in both directions. **Inbound**, the message's
payload is checked against the entry's own descriptor — a message off a wire is
a claim, not a fact. **Outbound**, every birth verb the context has is wrapped,
and each checks its payload against the descriptor *it* was called with. That
second half is the one that matters most:

```ts
// the log never accepts a lie
ctx.append(Charged, { accountId })     // ← missing `amount`: throws HERE,
                                       //   not in a replay years from now
```

**The parsed value replaces the input**, on both paths. Standard validation is a
*parse* — coercions, defaults and transforms are part of what a schema says — so
the handler is handed the message the schema produced, and each verb gives birth
to the produced value.

`ctx.send` and `ctx.query` already answer promises, so a schema that validates
asynchronously is simply awaited. `ctx.append` returns `void` and `ctx.schedule`
(where the log provides it)
builds its message in the caller's turn, so an async schema *there* throws,
naming the message type and the verb. Use `validate` yourself at the edge, where
you can await, and the birth verbs never see the question.

Validation asks the context for nothing, so unlike `correlatingHandler` it adds
no demand: a validated handler wires against exactly the buses the unvalidated
one did, and the wrappers compose in any order.

## Handlers and the three contexts

A handler is a function of `(message, ctx)`. Which `ctx` you get is the safety
model — the capabilities you must not have are *absent from the type*, not
merely discouraged.

```ts
type EventHandlerContext<E extends EventStore = EventStore, U extends UnitOfWork = UnitOfWork> =
  { load · source · send · query · emitUpdate · isReplay() · unitOfWork: U }
  & SnapshotReads<E>      // the fused read — only when the log caches folds
  & ScheduleVerbs<E>      // schedule · scheduleAfter · cancelSchedule — only when the log holds deadlines
type CommandHandlerContext<E, U> = EventHandlerContext<E, U> & { append }
type QueryHandlerContext<E, U> = { load · source · query · unitOfWork: U } & SnapshotReads<E>
```

`E` comes first because `E` is what a handler writes: it annotates the log it
*uses* (`ctx: CommandHandlerContext<SnapshotCapableEventStore>`). `U` stays
defaulted — the task is something done *to* a handling, demanded by a wrapper on
its output and minted by a bus — and a handler names it only when it reaches for
the task directly.

**A context is assembled by intersection.** The first line is what every handling
gets; the other two are what the entry's *log* contributes, and against a log
that was never wrapped they resolve to `unknown` and vanish. So the fused
`ctx.source(query, { snapshot })` and the three scheduling verbs are structurally
*absent* rather than present-and-failing — the diagnostic is "property does not
exist", at the call site.

`U` is whatever the seam's unit-of-work factory mints and `E` is the entry's log,
both threaded through so a handler can *demand* what it needs. Both default —
`UnitOfWork` and `EventStore` — so a handler that wants nothing special writes
nothing special.

### Name your app's context once

A demand names only what it uses, and it says so by **intersecting a face** or
naming a tier's type — never by restating parameters it has no opinion about.
But an app that depends on several capabilities would then repeat the same
intersection in every handler, and every handler would have to be edited the
day the deployment learns a new trick. So name it once, in your own vocabulary:

```ts
// yours, in your app — not something the framework ships
type UniversityCommandContext =
  CommandHandlerContext<SnapshotCapableEventStore & ScheduleCapableEventStore> & EmitCapability

const enrollStudent = commandHandler(EnrollStudent, async ({ payload }, ctx: UniversityCommandContext) => {
  const course = await ctx.load(Course, { courseId: payload.courseId })   // needs the snapshot tier
  ctx.append(StudentEnrolled, payload)
  await ctx.scheduleAfter(EnrollmentLapses, payload, 86_400_000)          // needs the schedule tier
  ctx.emitUpdate(WatchCourse, (q) => q.courseId === payload.courseId, …)  // needs the subscription tier
})
```

One word per handler, one line to change when the app's floor moves — and the
line still reads as a floor, so every entry is checked against it exactly as
before. The adapter packages name theirs the same way
(`PostgresCommandContext`, `DrizzleEventContext`), which is where the naming
convention comes from: the thing it belongs to, then the message kind.

Keep it a *floor*, not a description of your infrastructure: name the
capability tiers you use, never a concrete store or bus type. `state()` and the
entries do the rest.

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

A state is a decision model: an id schema, the tags it is scoped by, and one
`evolve` tuple — the initial state at position zero, then a `[EventDescriptor, fold]`
pair per event type it folds.

```ts
const Subscription = state({
  id: { courseId: z.string(), studentId: z.string() },
  tags: (id) => ({ courseId: id.courseId, studentId: id.studentId }),
  evolve: [
    (id) => ({ courseId: id.courseId, capacity: 0, taken: 0, registered: false }),
    [CourseCreated,     (s, { payload }) => ({ ...s, capacity: payload.capacity })],
    [StudentRegistered, (s) => ({ ...s, registered: true })],
    [StudentSubscribed, (s) => ({ ...s, taken: s.taken + 1 })],
  ],
})
```

`evolve` is correlated-tuple **data**, not a builder. Element zero is *always*
the initial state: the fold is `cases.reduce(...)` starting from `evolve[0](id)`, and it
is the evolver of nothing — which is why it lives in the same list rather than in
a field beside it. `S` is fixed by it before the cases are checked, and each
pair is checked against *its own* descriptor rather than a union, so a wrong
`payload` access is reported at that pair.

**The initial state is handed the id.** Being the evolver of nothing is not the same as
being the knower of nothing: no event has been folded yet, so the identity is the
one thing a zeroth state can honestly know, and a fold that carries its own key
no longer has to wait for an event to tell it what it already is. The `id` is the
same inferred record `tags` takes. An initial state that does not care *declines* the
argument by writing none — `() => ({ … })` stays assignable to `(id) => S` by
TypeScript's arity rule, which is why most folds never mention one.

A state that snapshots says so on the value — `state({ …, snapshot: { key:
"course-v1", when: afterEvents(50) } })`. `key` is a string *you* wrote: it is
where entries are filed, and changing it is how you throw them away. Where the
snapshot lands is a site fact: the entry's `eventStore`, which must have been
wrapped in its family's `…SnapshottingEventStore`. Both halves have to be
present — and **the compiler makes sure they are**: writing the config types the
state as snapshotting, and `ctx.load` refuses it against a log that cannot serve
one. See [Snapshotting](#snapshotting-a-cache-over-the-fold).

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

**Nothing names a state.** There is no `name` field: a state that caches its fold
says *where* under `snapshot.key`, and diagnostics name a state by its process
identity and the events it folds.

## The raw layer: `ctx.source`

`state()` writes the query for you. `ctx.source(query)` is the layer underneath:
**you write the query, you run the fold** — and the append condition still holds.

```ts
const events = await ctx.source({
  tags: { courseId: "cs-101" },
  types: [CourseCreated, StudentSubscribed],
})
```

The query is the same plain data a state derives for itself: within an item
`types` is an any-of (descriptors, qualified names or strings) and `tags` an
all-of, and an array of items is an OR.

```ts
await ctx.source([{ tags: { courseId } }, { tags: { studentId } }])   // OR
```

What comes back is the matching events, in stream order, ready to be the input
of a fold. `is()` makes that fold a typed switch — this is the idiom:

```ts
const subscribe = commandHandler(SubscribeStudent, async ({ payload }, ctx) => {
  const events = await ctx.source({
    tags: { courseId: payload.courseId },
    types: [CourseCreated, StudentSubscribed],
  })

  const course = events.reduce(
    (s, e) => {
      if (is(e, CourseCreated))     return { ...s, capacity: e.payload.capacity }
      if (is(e, StudentSubscribed)) return { ...s, taken: s.taken + 1 }
      return s
    },
    { capacity: 0, taken: 0 },
  )

  if (course.taken >= course.capacity) throw new Error("course is full")
  ctx.append(StudentSubscribed, payload)          // conditioned on that very read
})
```

**The last line is the point.** `ctx.source` records what it read onto the task —
the query, and the position it read up to — exactly the way `ctx.load` does, and
the PREPARE_COMMIT flush turns those entries into the append condition. So the
hand-rolled fold above has the *identical* DCB optimistic-concurrency guarantee
the `state()` fold has: if another task appends a matching event between this
read and this write, this write fails. A raw fold is a first-class decision, not
an escape hatch that gives one up.

**Declaring `types` narrows the conflict window.** Omitting it is legal and means
"every event carrying these tags" — a wider window, and more spurious conflicts.
That narrowing is one of the things the state derivation was doing on your
behalf.

It reads the entry's store, so a store composed with `upcastingEventStore` hands
this layer upcasted events too, and it is on all three contexts — on a query
context there is no `append` to condition, but the read is the same read.

**It also takes a snapshot key — when the entry's log can serve one.**
`ctx.source(query, { snapshot: "course:cs-101" })` returns
`{ snapshot, events, position }` instead of the bare array — the cached fold
filed under that string, plus only the events after it. That overload is
*contributed* by a snapshot-capable log and is structurally **absent** otherwise,
so on a bare log `ctx.source` takes exactly one argument and always did. That is
the whole snapshotting capability at this layer; see below.

## Snapshotting: a cache over the fold

A fold over ten thousand events gives the same answer every time. Snapshotting
keeps that answer so the next reader does not have to compute it again — and
every rule of the mechanism falls out of that one word, **cache**.

- **Latest only.** A cache has a current entry, not a history. `store` replaces;
  `load` answers with the one entry or with nothing.
- **Never migrated.** An entry you cannot use is *discarded*, not converted.
  Upcasting exists because events are kept forever; a cache entry that has
  stopped meaning something is thrown away and recomputed.
- **Never load-bearing.** Every read path falls back to full sourcing on a miss,
  an unusable entry, or an outright failure to reach the cache.

### The raw idiom

Start here, because this *is* the capability. Two primitives — a source that
takes a key, and a write that takes the same key — **and both come off the one
object the entry already carries.** The rest of this section is sugar over them.

```ts
const key = `course:${courseId}`
const { snapshot, events, position } = await ctx.source(query, { snapshot: key })
const state = events.reduce(fold, (snapshot?.state as CourseState) ?? initial)
if (events.length > 100) await eventStore.storeSnapshot(key, { state, position })
```

Four lines, and every decision in them is yours. **You** wrote the key. **You**
ran the fold. **You** judged whether the cached value was worth starting from —
the framework hands it over without an opinion, because it does not know what
your fold folds into. **You** decided a snapshot was due; the `if` is the policy,
and there is no other kind. And `position` is the consistency marker the read
reached, which is exactly what the entry you write should record as its own.

A bare `ctx.source(query)` is untouched: no key, no fusion, and the return is
still the events array.

The write primitive is **a member of the log** — a slice already holds its
`eventStore` among its resources, so writing is a one-line call at the moment the
slice decides a fold was expensive enough to keep. There is deliberately no `ctx`
capability for it, because there does not need to be one; and there is no second
resource to wire, because the capability rides on the store it belongs to.

### The key is yours

A snapshot is filed under **a string you wrote**. Nothing is derived from your
code, nothing is hashed, and nothing about the framework's opinion of your fold
enters into it.

Which makes invalidation one sentence: **changed the fold's meaning? change the
key.** Rename `"course-v1"` to `"course-v2"` and every old entry becomes
unreachable in the same instant — no migration, no backfill, no version column.

```ts
snapshot: { key: "course-v1", when: afterEvents(100) }   // ← one character in a diff
```

It is user-space, greppable and reviewable; it happens exactly when you decide it
should rather than when a heuristic guesses; and **there is no automatic
rotation, which is the point.** Deciding when two folds are the same fold is a
judgement about meaning, and meaning is not derivable. A refactor that preserves
meaning does not need a new key, and nothing will invent one behind your back for
a refactor that does not.

The framework's job is to hold the key you gave it and hand back what is filed
under it.

### The strategy is said on the condition

This is what lets one address serve four very different storage families. A read
does not fetch a snapshot — it **asks** for one, on the sourcing condition:

```ts
type SourcingCondition = { query; start?; snapshot?: SnapshotKey }
type SnapshotKey       = { key: string }        // ONE user-composed string
type SourcingResult    = { events; marker; snapshot?: Snapshot }
type Snapshot          = { state: unknown; position: bigint }
```

Whoever serves the read decides what to do with it, and every answer is correct:

- a log that was never **wrapped** ignores the key and sources in full — a slower
  load, never a wrong one;
- a log wrapped in the in-memory, KronosDB or Axon Server wrapper resolves the
  key client-side: a lookup, then a narrowed source — **two calls, inside one
  function**;
- a log wrapped in the postgres wrapper fuses both into **one round trip**,
  because it holds the connection.

```ts
// one line per family, and the base store in each has never heard of snapshots
const eventStore = inMemorySnapshottingEventStore(inMemoryEventStore())
const eventStore = kronosDbSnapshottingEventStore(kronosDbEventStore(kdb, ctx), kdb, ctx)
const eventStore = axonServerSnapshottingEventStore(axonServerEventStore(axon, ctx), axon, ctx)
const eventStore = postgresSnapshottingEventStore(
  postgresEventStore(pool, { tagResolver }), pool, { serializer },
)
```

**"Fused in one round trip" is not a feature; it is just owning the query.** A
wrapper that has to talk over a wire cannot beat two calls. The postgres wrapper
holds the connection, so it can say the whole thing once — a CTE over
`kronos_snapshots`: the lookup, the start position derived from it, the event
query and the head. When KronosDB or Axon Server grow a fused RPC, it lands
inside their wrapper and **no host changes a line**, because the capability was
never a promise about round trips.

### The compiler makes you wire it

This is the part that changed the feel of the feature. A snapshot policy used to
be a wish: declare one, forget the store, and you got a silent full replay plus a
cache nobody read. Now the state's type says it caches, and `ctx.load` refuses it
against a log that cannot:

```ts
const Course = state({ /* … */, snapshot: { key: "course-v1", when: afterEvents(100) } })

// ✗ — "this state declares a snapshot policy, but this handler's eventStore
//      cannot serve one" / "wrap this entry's eventStore in the snapshotting wrapper
//      for its persistence family — <family>SnapshottingEventStore(store, …)"
commandHandler(OpenCourse, async (m, ctx: CommandHandlerContext) => {
  await ctx.load(Course, { courseId })
})

// ✓ — say what you need, and the ENTRY must supply it
commandHandler(OpenCourse, async (m, ctx: CommandHandlerContext<SnapshotCapableEventStore>) => {
  await ctx.load(Course, { courseId })
})
```

The demand travels the same way correlation's does: annotate the context, and the
entry that places the handler must carry a log which satisfies it — so a wiring
mistake stops at the composition root instead of at 3 a.m. One alias says it for
every read surface (`IfSnapshotCapable`, in `event-sourcing/load.ts`), and
**nothing runs**: the whole demand is erased, and the only runtime trace is one
defensive `throw` for callers who had no compiler.

**Fusing does not narrow the append condition.** It narrows which *events* come
back; it does not narrow what was *read*. Both reads record the same query and
the same marker on the task, so a fold seeded from a snapshot has exactly the DCB
guarantee a fold that replayed everything has.

### The capability

**One member is added, and it is the write:**

```ts
type SnapshotCapability = {
  storeSnapshot(key: string, snapshot: Snapshot, uow?: UnitOfWork): Promise<void>
}
type SnapshotCapableEventStore = EventStore & SnapshotCapability
```

There is no `loadSnapshot`, because reading is not a second call — a capable log
honours `condition.snapshot` inside `source()` and leads its result with the
cached fold, which is exactly what lets a store that owns its query serve the
whole thing at once. No list, no history, no delete: a cache has a *current*
entry, and replacing it **is** how you invalidate it.

The wrappers are **additive** — `<E extends EventStore>(next: E, …) => E & SnapshotCapability`
— so wrapping an upcasting store still gives an upcasting store, and stacking in
either order keeps everything. A wrapper typed `(EventStore) => SnapshotCapableEventStore`
would have *laundered* whatever it was not itself adding, and a demand built on
laundered types rejects configurations that work.

The key is one opaque string, and every implementation stores it as one column.
Nothing parses it, which is exactly why an operator can read it: a
`kronos_snapshots` row says `course-v1:{"courseId":"cs-101"}` and that is the
whole story of which fold it belongs to.

### The leading snapshot is not an event

When something serves the strategy, the result carries the cached fold as its own
field — never as a synthetic first element of `events`. An event is a fact with a
name, a version, tags and a payload, and every reader of `events` is entitled to
treat it as one; a snapshot is a folded *state*.

```ts
// before — fetch from a separate seam, then decide where to start
const snap  = await snapshotStore.load(key)
let   state = snap ? snap.state as S : initial(id)
const { events, marker } = await store.source(sourcingCondition(query, snap && snap.position + 1n))

// after — ask the LOG, and start from whatever came back leading the result
const { events, marker, snapshot } = await store.source(sourcingCondition(query, undefined, { key }))
let   state = snapshot ? snapshot.state as S : initial(id)
```

### `state()`'s snapshot config is sugar over exactly those primitives

Everything above is four lines a handler can write. `state({ snapshot })` is
those four lines with the key composition and the policy written for you:

```ts
const Course = state({
  id: { courseId: z.string() },
  tags: (id) => ({ courseId: id.courseId }),
  evolve: [ /* … */ ],
  snapshot: { key: "course-v1", when: afterEvents(100) },   // .or(…) composes
})
```

`key` is **required**. A state that snapshots must say where, because "where" is
a decision about meaning that only you can make.

The load stamps the key on the condition, seeds from a fitting snapshot, and
writes through the site's store when the policy fires. **The composed key is
`` `${key}:${canonical id}` ``** — the declared string, a colon, and the id
record flattened canonically: object keys **sorted** (so `{ courseId, studentId }`
and `{ studentId, courseId }` name the same entry), a bigint keeping its `n`
suffix (so `1n` and `"1"` stay distinct), and a plain string id left as itself,
unquoted. One declared key therefore serves every instance of the state without
them colliding, and renaming it moves every id at once.

The **capability** is a site fact, and it rides on the log:

```ts
const eventStore = postgresSnapshottingEventStore(
  postgresEventStore(pg, { tagResolver }), pg, { serializer },
)
kronos({ commandHandlers: handlers.map((h) => ({ ...h, eventStore })) })
```

One store object per entry — there is no `snapshotStore` field and nothing a host
can wire half of. With no policy on the state nothing happens; with a policy and
an unwrapped log, the build fails.

### The read is the mechanism's; the write is the fold's

The repository writes the entry, and that is not core knowing an optimization.
The **read fusion** belongs to the store because fusing a lookup with a query
is a property of the store you are reading from. The **write** belongs to the
fold because the thing being cached is the fold's own output at the fold's own
position, and nobody else in the system is holding both — a decorator on the read
path sees events go past, not the state they add up to.

It is fire-and-forget and its failure is swallowed, because a cache write that
could fail a load would make the cache load-bearing.

### Structural fitness is a safety net, not a second key

The key is the gate that handles **meaning**, and nothing here second-guesses it.
This is the layer underneath, for the ways a cached value can come back wrong
without anybody having changed a key:

- **storage corruption** — a truncated BYTEA, a half-written row;
- **serializer drift** — bytes written by one encoder and read by another;
- **shape drift you did not notice** — you added a field to the initial state and
  did not think of it as a change of meaning, so the key stayed, and every old
  entry now lacks a field the fold reads.

The specimen is free and always current. `evolve[0]` is the initial state, a live
example of the shape the fold works in, and it cannot drift from the fold because
it *is* the fold's first line. **No code is inspected** — this reads a *value*,
the same way the fold does. The cached value is checked against `initial(id)`
structurally: every key the specimen has must exist, with a recursively matching
`typeof`; extra keys are fine; objects recurse; arrays check each element against
the specimen's first; an empty array or a `null`/`undefined` leaf teaches nothing
and is skipped.

What it cannot catch, said plainly: a change that keeps the structure and changes
the meaning. Cents becoming dollars is `number` before and `number` after. **That
is what the key is for.**

Unfit means **discard silently, replay in full**, and the policy writes a fresh
entry of the current shape on the way out.

At the raw layer there is no such check, because there is nothing to check
against: fitness is your fold's judgement, and an `if` is how you spell it.

### A cache is never load-bearing

A miss, an unfit entry, or an outright throw from the cache all fall
back to full sourcing, silently. A snapshot service that is down makes loads
slower, and that is the only thing it is allowed to do.

### Composing with upcasting

Both wrap the log. The documented order is **upcasting outermost**:

```ts
upcastingEventStore(
  postgresSnapshottingEventStore(postgresEventStore(pg, { tagResolver }), pg, { serializer }),
  upcast,
)
```

Read it inside out: the snapshot layer decides *which* events are read, the
upcast layer decides what each of them *means*. The reverse order gives the same
answers today — the snapshot layer only ever narrows the range, and a folded
state was never going to be `(event) => event`'s business — so the preference is
for the spelling that stays true if that ever changes.

**Both orders typecheck with every capability intact**, because both wrappers
preserve what flows through them: `upcastingEventStore` is a generic identity
(`<E extends EventStore>(next: E, upcast) => E`) and the snapshotting wrappers
are additive (`… => E & SnapshotCapability`). A wrapper that collapsed to the
base seam would *launder* — the runtime object still delegates everything, but
the type threw the capability away, so `ctx.load` would reject a state the
configuration serves perfectly. The order here is a semantic preference, never a
typing constraint.

What an upcaster does not reach, in either order, is the cached state itself.
That is the discard rule, not an oversight: a fold whose events changed meaning is
a fold whose cache no longer fits, and the answer is a new key — not a second
upcasting surface for states.

## Scheduling: the second tier on the log

An event that has not happened yet is still an event, and where it lands when its
time comes is **the log**. That sentence is the whole design.

There used to be an `EventScheduler` seam and an `eventScheduler` field on every
entry, and the three implementations gave the game away: the in-memory one had to
be handed an `eventSink`, the postgres one an `eventStore` in its config, and the
KronosDB one only existed because the server appends the event itself. Three
different spellings of *"and this is the log I fire into"* is the shape of a
capability that belongs **on** the log.

So scheduling became a capability tier, added by wrapping — exactly like
snapshotting, demanded through exactly the same construction:

```ts
type ScheduleCapability = {
  schedule(event: EventMessage, at: Date, uow?: UnitOfWork): Promise<ScheduleToken>
  cancelSchedule(token: ScheduleToken, uow?: UnitOfWork): Promise<CancelResult>
}
type ScheduleCapableEventStore = EventStore & ScheduleCapability
```

Three wrappers, one per family that has one, each **additive**:

```ts
const eventStore = inMemorySchedulingEventStore(inMemoryEventStore())
const eventStore = postgresSchedulingEventStore(
  postgresEventStore(pool, { tagResolver }), pool, { unitOfWork: uow, tagResolver },
)
const eventStore = kronosDbSchedulingEventStore(kronosDbEventStore(kdb, ctx), kdb, { serializer })
```

Axon Server gets none, and the absence is deliberate: its generated protobuf
carries a `DcbEventScheduler` service, but this package never built a client for
it. There was nothing to absorb, and writing one here would be new functionality
wearing a refactor's clothes.

**KronosDB is the odd family out, and the tier makes that visible.** Its schedules
already ride the KronosDB log server-side, so its wrapper holds no table, no timer
and no poller — and it does *not* join the caller's transaction, because the
server owns the schedule the instant it is told. A handling that arms one and then
throws has armed it. That was already true of the standalone scheduler; it is now
written on the same object as the log, where a reader comparing the three families
can see it.

### The compiler makes you wire it

The mirror of the snapshotting demand, one capability over. Against a bare log the
three verbs are not on the context at all:

```ts
// ✗ — "Property 'schedule' does not exist on type 'CommandHandlerContext'."
eventHandler(OrderPlaced, async (m, ctx: EventHandlerContext) => {
  await ctx.scheduleAfter(PaymentTimedOut, { orderId: m.payload.orderId }, 900_000)
})

// ✓ — say what you need, and the ENTRY must supply it
eventHandler(OrderPlaced, async (m, ctx: EventHandlerContext<ScheduleCapableEventStore>) => {
  await ctx.scheduleAfter(PaymentTimedOut, { orderId: m.payload.orderId }, 900_000)
})
```

One alias says it — `IfScheduleCapable`, in `event-scheduling/schedule.ts` — and
one face derives from it, `ScheduleVerbs<E>`, which the contexts intersect.
Snapshotting needed *two* faces because a `state()` can declare that it caches, so
there was something to refuse as well as something to offer. Nothing declares that
it schedules, so this side of the mirror is one conditional.

What this replaces is a `throw new Error("No event scheduler configured")` — a
missing field, discovered by the first deadline anybody armed in production.
**Nothing runs**: the demand is erased, and the only trace is one defensive assert
for callers who had no compiler.

### Cancelling is news, not an exception

```ts
type CancelResult =
  | { kind: "cancelled" }         // it was pending; it will not fire
  | { kind: "already-appended" }  // it fired first — compensate if you must
  | { kind: "not-found" }         // already cancelled, never existed, wrong deployment
```

Three outcomes a caller branches on, and every family answers in the same three
words — so the compensating branch is written once rather than per backend. Keep
the `ScheduleToken` if you mean to cancel: append an event that carries it. That
is the deadline / process-manager pattern, and it is why the token is a value you
hold rather than something the framework remembers for you.

### Both tiers, any order

The store-tier category has two members now, and they compose:

```ts
const eventStore = upcastingEventStore(
  postgresSchedulingEventStore(
    postgresSnapshottingEventStore(postgresEventStore(pool, { tagResolver }), pool, { serializer }),
    pool, { unitOfWork: uow, tagResolver },
  ),
  upcast,
)
```

Every order keeps every capability, because every wrapper obeys the same two
rules: same-seam wrappers are generic identity, capability adders are additive.
With one capability that was easy to satisfy by accident; with two it is not — a
wrapper that collapsed to *its own* capability would keep the one it adds and
silently drop the other. The type probes pin every order.

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
  unitOfWork: drizzleUnitOfWork(unitOfWork, db),
  sequence: sequentialPerTag("accountId"),
  deadLetterQueue: drizzleDeadLetterQueue(db),
})
```

A queue parks the failed event **and everything behind it in the same lane** —
that is what preserves per-entity order across a poison pill. So "which lane" is
not optional once there is a queue, and a `deadLetterQueue` without a `sequence`
**does not compile**:

```
Property 'sequence' is missing in type '{ name: string; eventStore: …; deadLetterQueue: … }'
  but required in type '{ readonly sequence: {
    readonly ERROR: "this processor has a deadLetterQueue but no sequence, and parking is a lane operation";
    readonly FIX: "add `sequence: sequentialPerTag(\"<tagKey>\")`, or drop the queue and let failures propagate and retry";
  }; }'.
```

The keys *are* the message, which is why the refusal is spelled inline rather than
behind a named type: give TypeScript a name and it prints the name. This used to
be a `throw` at construction — honest, and late, because a composition root runs at
boot. The throw survives as one defensive assert for JavaScript callers. The honest
global-order answer for a projection is *no queue at all*: propagate and retry.

**Never mix persistence families within one processor — and the stores enforce
that themselves.** A token store or dead-letter queue handed a unit of work that
carries no transaction of its own throws, naming the factory to build the
processor's `unitOfWork` with. It does not fall back to its plain handle, which
is what used to make this the worst-shaped failure there is: the token committed
**outside** the batch, every test passed, and a later crash between the
projection write and the token write left a read model permanently wrong with
nothing to read as the cause.

A handler's accessor still falls back, and the asymmetry is the point: whether
the seam a handler runs in is transactional is a deployment decision, so
`ctx.db()` works either way. A token store has no such freedom — being in the
projection's transaction is the entire reason it exists — so absence is an error
rather than a default.

Absent a queue, a handler failure propagates and the batch retries.
`batchSize` (default 1) is how many events share one unit of work.

## Transactions: one owner, lenses for the rest

Only one persistence family can own a task's transaction — transaction identity
*is* the client handle. That leaves a
real question when your application code uses an ORM and your log is
`postgresEventStore` on the same database: whose transaction does a handler run
in?

The answer depends on what the processor's handlers do, because the event
store's append joins the **postgres** family's transaction and no other:

```ts
// ✗ ORM family owns — appends run in their OWN transaction, apart from your ORM writes.
//   Projection-write + token-write stay atomic (that pair is what the ORM family is for),
//   but a command handler's ctx.append is NOT atomic with its ctx.db() writes.
const uow = drizzleUnitOfWork(() => unitOfWork(), db)

// ✓ postgres owns, and the ORM rides the SAME connection as a lens —
//   table write + append commit or roll back together.
const uow = postgresUnitOfWork(() => unitOfWork(), pg)
const tx  = await postgresTransaction(ctx.unitOfWork)
const db  = drizzle(tx.unwrap<PoolClient>(), { schema })
```

So the rule, per processor: **ORM family end-to-end** when the handlers only
project — their atomicity need is projection + token, which the family gives.
**Postgres family with the ORM as a lens** when handlers must write tables *and*
append in one transaction.

There is no drizzle event store, deliberately: the log's SQL is engine code —
append conditions, advisory locks, watermark queries — not application data. So
the sharing runs in one direction only: the framework exposes its transaction
(`unwrap()` returns the live driver connection) and your ORM binds to it. The
mechanics — per adapter, per ORM, including the pool-owning ones — are in
`@kronos-ts/postgres`'s README under "Handing the transaction to an ORM".

## Assembly

`kronos` takes three lists. There is no module, container, registry or enhancer.

```ts
const app = kronos({
  commandHandlers: [{ ...subscribe, eventStore, commandBus, queryBus }],
  queryHandlers: [{ ...getCourseView, queryBus }],
  eventHandlers: [
    { ...projectSubscribed, commandBus, queryBus, processor: projection },
    { ...closeWhenFull,     commandBus, queryBus, processor: automation },
  ],
})
```

`Subscription` is in no list. `ctx.load(Subscription, id)` is handed the value at
the call site and the log off the entry's site, so the fold is complete without
anybody having declared it: `kronos` registers **behaviour**, and a state is
data.

An entry **points at** shared objects; a record **contains** its own parts.
`kronos` follows those references and never counts. There is no `stores: { … }`
bag, because nothing was behind the bag — it was unrelated objects, so each rides
under its own name: `eventStore`, `tagResolver`, plus
`commandBus`/`queryBus`/`processor` where they apply, and an optional `name` used
only in boot errors. **There is no `snapshotStore` beside `eventStore`**: a site
that caches folds is a site whose log was wrapped, so it is still one object
under one name.

Two grouping rules, and they are different on purpose:

- **Stores group by object identity — lazily, in a cache nobody registers with.**
  Two entries naming the *same* `eventStore` object share the folds built over
  it, because `ctx.load` builds a state's repository on first use and remembers
  it against the store it came from. Losing that cache would cost a rebuild and
  nothing else, which is what makes it a cache and not a registry. The tag
  resolver rides per entry — an entry that wants its own says so.
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
type App = {
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
