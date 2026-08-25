# kronos-ts — SURFACE v2 (the frozen target)

The rules this surface is held to — every export must survive all of them:
- Core is only what cannot be a helper: task lifecycle, bus/store shapes,
  reference-following assembly, the DCB model. Everything else is a package
  of functions over public shapes that anybody could have written.
- Libraries export functions of ALL their real arguments; hosts write the
  arrows. No default-conjured delegates, no curried config-first factories.
- One function per seam; plurality is composed in function space by the host.
- Entries point at shared objects; records contain their own parts; the
  framework follows references and never counts. No module concept.
- Fields for order-free parameters of one algorithm; pipes only where order
  is real. A commuting pipeline stage is a lie.
- Parameter type expresses lifetime. Per-request data enters where the
  message is born (edge); per-handling via ctx; message-derivable via
  Intercept.
- One operation = a function type. A record of functions = shared state.
- THE THREE LAWS OF THE UNREPRESENTABLE. They are one idea said three ways, and
  every capability tier, demand and wrapper in this surface is an instance of
  them:
  1. CAPABILITIES LIVE IN TYPES. A thing that can do more is a thing whose TYPE
     says more, and the only way to acquire one is a function that ADDS it —
     `<T extends Base>(next: T, …) => T & Capability`. There is no flag, no
     option and no field a host can set to claim a capability the value does not
     have.
  2. DEMANDS ARE FLOORS. A consumer says the least it needs and never names an
     implementation: `CommandHandlerContext<SnapshotCapableEventStore>`,
     never `PostgresEventStore`. A floor admits everything above it — so a
     demand narrows what may be WIRED without narrowing who may supply it, and
     adding a family never touches a consumer.
  3. PIPES PRESERVE. Same-seam wrappers are generic identity
     (`<T extends Base>(next: T, …) => T`); capability adders are additive
     intersections. A wrapper that collapses to the base LAUNDERS: the runtime
     object still delegates everything, but the type threw a capability away, so
     a demand rejects a configuration that works perfectly — which is worse than
     having no demand, because it cannot be fixed from the call site.
- And the sentence the three laws exist to serve: ANYTHING WIREABLE THAT WOULD
  DIE AT RUNTIME FOR A REASON THE COMPILER COULD HAVE STATED IS A BUG IN OUR
  TYPES, NOT IN THE USER'S CONFIG.
- Types ARE function signatures. The `interface` keyword appears nowhere: a
  shape is a `type` alias of function-signature fields, a single operation is
  a bare arrow, and `extends` is an intersection. Classes are Errors only —
  `instanceof` is the one thing a type alias cannot do. Everything with
  behaviour is a closure over its state, so the state is unreachable and the
  value is the whole contract.
- No "factory" naming, no one-field return records, no sentinels in
  signatures, no variadic behavior parameters, no ALS, no registration.

## Package layout
```
packages/core          ← merge of common + messaging + eventsourcing + modelling + app
packages/test
packages/rabbitmq · kronosdb · axon-server            (transports)
packages/postgres · drizzle · knex · kysely · prisma · typeorm   (persistence)
packages/otlp          ← replaces opentelemetry; zero @opentelemetry deps
```
The `extensions/` directory is gone; the concept is gone.

Inside `packages/core/src`, the folders are named for what things MEAN, not for
what they technically are. There is no `buses/`, no `handlers/`, no `stores/` —
those are the same three shapes repeated once per message kind, and filing by
shape scatters each kind's life across the tree. `kronos` takes THREE lists, and
each maps 1:1 onto an activity folder; `event-sourcing/` is the fourth folder and
takes no list, because what lives there is not behaviour to register but the log
and the state VALUES the handlers close over:
```
messaging/          what a message IS, before any kind picks it up. Imports from
                    NOTHING else.
                    messages.ts       ONE file: qualified names · metadata · the
                                      three message kinds · descriptors ·
                                      withNamespace. One subject, one address.
                    tag.ts · identifier.ts · serialized-error.ts   each stands alone
                    standard-schema.ts  the vendored StandardSchemaV1 contract
                    serialization/    serializer seam · json. ENCODING ONLY —
                                      no validating decorator, no registry.
                                      Room for a binary serializer.
unit-of-work/       the task lifecycle everything else runs inside
command-handling/   ← kronos({ commandHandlers })   bus · local-bus · send (BOTH
                    births: the edge verb and ctx.send) · handler · context ·
                    subscribe
query-handling/     ← kronos({ queryHandlers })     bus · local-bus · query (both
                    births) · handler · context · subscribe · subscription-query
                    · subscription-filter · emit-update
event-sourcing/     ← NO list. The log AND the folds over it, because state IS
                    sourcing: event-store · storage-engine · conditions ·
                    consistency-marker · dcb-query · tag-resolver · in-memory ·
                    state · load (BOTH reads — the folded `ctx.load` and the raw
                    `ctx.source`, which share the one piece of bookkeeping that
                    turns a read into an append condition) · append · repository
                    (which also holds the lazy per-site fold cache `ctx.load`
                    goes through) · snapshot (the fold's caching VOCABULARY:
                    what one cached fold is, when one is due, how an id joins a
                    key) · structural-fitness · in-memory-snapshotting-event-store
event-processing/   ← kronos({ eventHandlers })     handler · context ·
                    processor · running-processor · source · tracking-token ·
                    token-store · segment · sequence · dead-lettering ·
                    dead-letter-queue · dead-letter-reprocessor
event-scheduling/   events that have not happened yet, and the SECOND STORE TIER:
                    scheduler (the capability CONTRACT — ScheduleCapability ·
                    ScheduleCapableEventStore · ScheduleToken · CancelResult) ·
                    schedule (the DEMAND — IfScheduleCapable · ScheduleVerbs —
                    and the ctx verbs behind it) ·
                    in-memory-scheduling-event-store
interception/ · correlation/ · upcasting/ · validation/ · kronos.ts · index.ts
```
FOUR MECHANISMS, one shape. `Intercept` is `(message) => message` at the BUS
boundary; `Upcast` is `(event) => event` at the LOG boundary; `correlation/` is
what a handling carries onward; `validation/` is what may CROSS a handling
boundary, inbound and out. None of them is part of `messaging/`, because a
mechanism is not vocabulary: `messaging/` says what a message IS, the others are
things you do to one.

THE LIST IS HONEST BECAUSE OF WHAT IS NOT ON IT. `snapshotting/` was the fifth,
and it is gone — not renamed, dissolved. A mechanism here is a WRAP-IN THAT
LIVES IN CORE and serves every backend the same way, and snapshotting cannot be
one: fusing a cache lookup into a read is a property of the STORE you are
reading from, and the store families live in their own packages. So it is a
CAPABILITY TIER, added by a per-family wrapper, and what stayed in core is only
the vocabulary the fold and the wrappers share — which belongs in
`event-sourcing/`, beside the fold that asks for it.

THERE ARE STILL FOUR MECHANISMS. What grew is the OTHER category: THE STORE-TIER
CATEGORY NOW HAS TWO MEMBERS — SNAPSHOTTING AND SCHEDULING — and naming it a
category rather than a special case is the point. Both are per-family wrappers
on the log, both are ADDITIVE, both are demanded through one `If…Capable` anchor
and offered through one conditional face on the context, and both compose in any
order with each other and with `upcastingEventStore`. Scheduling joined by the
same argument that moved snapshotting: an `EventScheduler` seam had three
implementations and every one of them had to be told which log to fire into —
the in-memory one took an `eventSink`, the postgres one an `eventStore` in its
config, and the KronosDB one existed only because the server appends the event
itself. Three spellings of "and this is the log" is the shape of a capability
that belongs ON the log. A third tier would be a third intersection member on
the context and nothing else would move.

A tag lives in `messaging/`, not `event-sourcing/`, because `event({ tags })` is
where one is written — declaring an event is message vocabulary, and the
sourcing side imports it rather than the other way round. The rule that settles
every such collision: `messaging/` is the bottom of the dependency order and
imports from no activity folder, so anything it needs is vocabulary and moves
down to it.

## @kronos-ts/core

```ts
// primitives
qn(ns, name): QualifiedName · type Metadata · emptyMetadata() · type Tag · tag(k, v)
generateIdentifier(): string · type SerializedError
type StandardSchemaV1 · type InferOutput<S>    // the VENDORED schema contract, types only

// SCHEMAS ARE STANDARD SCHEMA. Every payload/result/id constraint is
// `StandardSchemaV1`, so zod, valibot, arktype and anything else carrying a
// `~standard` property all work, and CORE DEPENDS ON NONE OF THEM — zod is out
// of `@kronos-ts/core`'s dependencies entirely. Inference is unchanged: a zod
// schema still gives a handler the exact object type it always did.

// ── task lifecycle: THE primitive ──────────────────────────────────────────
unitOfWork(clock?: () => number): UnitOfWork   // message-agnostic (AF5): a task's processing
                                               // clock ABSENT = system time (the null behaviour)
// THERE IS NO `Clock` TYPE. The arrow IS the contract — the same rule that
// leaves a unit-of-work factory spelled `() => UnitOfWork` and never named. A
// clock reads an INSTANT, epoch ms, the same unit `message.timestamp` carries;
// every seam that takes one writes `clock?: () => number` inline.
type UnitOfWork = {
  execute<R>(action: (uow: UnitOfWork) => Promise<R>): Promise<R>
  on(phase, action) · onPrepareCommit · onCommit · onAfterCommit · onError · whenComplete
  now(): number                                // THE task's instant — every message it births stamps from here
  readonly events: UoWEventBuffer · readonly stateCache: UoWStateCache
  readonly phase · readonly closed
}                    // PURE TASK LIFECYCLE. No transaction surface — adapters own
                     // theirs. No correlation either — see the correlation section.
// ONE CLOCK PER TASK. ctx.append / ctx.send / ctx.query / ctx.schedule all stamp
// `timestamp` from uow.now(); scheduleAfter measures its delay from it too. A test
// that freezes the clock freezes every timestamp under the task, uniformly.

// ── messages & descriptors ─────────────────────────────────────────────────
type CommandMessage · QueryMessage · EventMessage · SequencedEventMessage
// `timestamp?: number` on Message — UNSET MEANS NOT THROUGH A TASK YET. There is
// no `Unstamped<M>` and no `stamped()`: the concept was a type for a moment, not
// a thing a host holds. `EventMessage` narrows it back to REQUIRED, because a
// fact you can read has an instant. WHEN the instant is settled is unchanged —
// the bus fills it from uow.now(), a transport from system time at the wire,
// ctx.append at birth.
command({ name, payload, result? }) · query({ name, payload, result? })
event({ name, payload, tags?, tagKeys?, version? })
is<D extends MessageDescriptor>(message: Message, descriptor: D): message is <the message type for D>
  // ONE guard, all three kinds: kinds equal AND qualified names equal AND — for
  // an EVENT, the only kind carrying a version on the message — versions equal.
  // Narrows the payload via InferOutput off the descriptor's own schema.
  // tags: { key: (p) => string }  — record of extractors; keys ARE the tag keys
  // tags: (p) => Tag[] needs explicit tagKeys when a state folds it
withNamespace(ns): { command, query, event }

// ── DCB queries: plain data, spec vocabulary ───────────────────────────────
type QueryItem  = { types?: ReadonlyArray<EventDescriptor | QualifiedName | string>; tags?: Record<string, string> }
type EventQuery = QueryItem | ReadonlyArray<QueryItem>      // items OR · tags ALL-of · types ANY-of

// ── buses: shapes + local segments ─────────────────────────────────────────
type CommandBus<U extends UnitOfWork = UnitOfWork> = {
  dispatch(m: CommandMessage): Promise<unknown>   // `timestamp` may still be unset
  subscribe(name, handler: (m: CommandMessage, uow: U) => Promise<unknown>): void
}
type QueryBus<U extends UnitOfWork = UnitOfWork> = { query(m, uow?); subscribe(name, (m, uow: U) => …) }
//   TWO members. Live updates are THE THIRD CAPABILITY TIER — the first on a
//   bus — added the way snapshotting and scheduling are added to a log:
type SubscriptionCapability = { subscriptionQuery · subscribeToUpdates · emitUpdate · completeSubscription · completeSubscriptionExceptionally }
type SubscriptionCapableQueryBus<U> = QueryBus<U> & SubscriptionCapability
//   IfSubscriptionCapable<Q, Capable, Bare> is THE anchor (query-handling/bus.ts);
//   SubscriptionEmit<Q> derives ctx.emitUpdate from it — structurally ABSENT on a
//   context whose entry wired a bus that never claimed the tier.
type EmitCapability = { emitUpdate }   // ← WHAT A HANDLER WRITES, and all it writes:
//   `ctx: EventHandlerContext & EmitCapability`. A DEMAND NAMES ONLY WHAT IT USES —
//   type parameters are POSITIONAL, so annotating the bus would restate the log the
//   handler had no opinion about. Parameters are the SUPPLY side (the entry threads
//   its bus and log in); intersections are the DEMAND side. Same as the persistence
//   faces (`DrizzleCapability`), and the refusal is identical either way. localQueryBus
//   offers the tier natively; kronosdb/axon-server/rabbitmq offer it
//   server- or broker-mediated; a custom bus writes TWO functions or claims it.
//   The subscriptionQuery EDGE VERB demands SubscriptionCapableQueryBus.
localCommandBus<U extends UnitOfWork = UnitOfWork>(unitOfWork: () => U): CommandBus<U>
localQueryBus  <U extends UnitOfWork = UnitOfWork>(unitOfWork: () => U): QueryBus<U>
// `U` is WHAT THE FACTORY MINTS, threaded to the handler's `ctx.unitOfWork`. It
// defaults everywhere, so uncorrelated, unadapted code never writes it. It is
// COVARIANT: a `CommandBus<CorrelatingUnitOfWork>` satisfies a plain
// `CommandBus` slot, and a plain bus does NOT satisfy a correlating one. That
// asymmetry IS the conditional compile error — see the correlation section.
// EDGE DISPATCH IS WHERE THE INSTANT IS SETTLED: the verb builds the message with
// no timestamp, the bus mints the unit of work and fills it from uow.now(). A
// nested query takes the instant of the task it joins. A transport, having no
// task, uses system time at the wire — and hands a locally-shortcut message on
// untouched, so the task that handles it supplies the instant.

// ── interception: one seam, one function ───────────────────────────────────
type Intercept<M extends Message = Message> = (message: M) => M
interceptingCommandBus<U>(bus: CommandBus<U>, intercept: Intercept<CommandMessage>): CommandBus<U>
interceptingQueryBus  <U>(bus: QueryBus<U>,   intercept: Intercept<QueryMessage>):   QueryBus<U>

// ── upcasting: the SAME seam, one boundary over ────────────────────────────
// Interception at the LOG boundary. Old events are REINTERPRETED on the way
// out; the log is never rewritten.
type Upcast = (event: EventMessage) => EventMessage     // TOTAL — identity when unconcerned
upcastingEventStore(next: EventStore, upcast: Upcast): EventStore   // READ paths only
// NO shipped constructor. Writing the match IS the lesson, and `is()` makes it a
// typed switch: declare the outdated version as its own descriptor, and the
// compiler knows what the payload looked like back then. The target version is
// read off the CURRENT descriptor, never restated.
// Plurality composes in function space, like everything else:
//   upcastingEventStore(store, (e) => v3(v2(v1(e))))
// Commands and queries need nothing new: a message crossing versions at a BUS is
// what `Intercept` already is. Only events have a second boundary, because only
// events are kept.

// ── snapshotting: NOT A MECHANISM. A CAPABILITY TIER ON THE LOG ───────────
// THE BASE `EventStore` MENTIONS SNAPSHOTS NOWHERE, and it is COMPLETE without
// them: a log you can source, append, stream and subscribe to is everything the
// DCB model needs, and most well-designed projects never need one line more.
// If snapshotting exists at all it exists ON THE EVENT STORE, added by WRAPPING
// one — which is why there is no seam, no `snapshotStore` field, and nothing a
// host can wire half of.
type SnapshotCapability = {          // what a wrapper ADDS; the read is already
  storeSnapshot(key: string, snapshot: Snapshot, uow?: UnitOfWork): Promise<void>
}                                    // in `source()`, via `condition.snapshot`
type SnapshotCapableEventStore = EventStore & SnapshotCapability
type Snapshot = { state: unknown; position: bigint }        // TWO fields. No version.
snapshotIdentifier(id: unknown): string                 // the id flattening state() uses
matchesInitialStructure(specimen, candidate): boolean   // the safety net
afterEvents(n) · whenSourcingTimeExceeds(ms) · noSnapshotPolicy()   // the POLICY, .or()-composable
//
// FOUR WRAPPERS, ONE PER FAMILY, and each is ADDITIVE — `E` in, `E & SnapshotCapability`
// out, so nothing the inner store carried is thrown away:
//   inMemorySnapshottingEventStore(next)                                 // core
//   postgresSnapshottingEventStore(next, pg, { serializer })             // ONE round trip
//   kronosDbSnapshottingEventStore(next, kdb, context)                   // ONE fused call — see its section
//   axonServerSnapshottingEventStore(next, conn, context)                // two, client-side
// Postgres fuses the lookup into its own query because it holds the CONNECTION;
// the others fuse client-side because a wire is in the way. That is a difference
// in what a wrapper can REACH, not in what the capability MEANS, and a server
// that grows a fused RPC changes one function body and no host's code.
//
// THE COMPILE-TIME DEMAND — and it is the headline. `state({ snapshot })` types
// the state as snapshotting, and `ctx.load` REFUSES one against an entry whose
// log cannot serve it. The wiring mistake that used to be a silent full replay
// is a build error naming the fix:
//
//   // A state that caches its fold…
//   const Course = state({ …, snapshot: { key: "course-v1", when: afterEvents(100) } })
//
//   // …cannot be loaded through a log that was never wrapped.
//   commandHandler(Open, async (m, ctx: CommandHandlerContext) => {
//     await ctx.load(Course, id)     // ✗ "this state declares a snapshot policy,
//   })                               //    but this handler's eventStore cannot serve one"
//
//   // Say what you need, and the entry must supply it.
//   commandHandler(Open, async (m, ctx: CommandHandlerContext<SnapshotCapableEventStore>) => {
//     await ctx.load(Course, id)     // ✓ and the entry's `eventStore` must now be wrapped
//   })
//
// ONE ALIAS SAYS IT, and every read surface derives from that one alias:
//   IfSnapshotCapable<E, Capable, Bare>          // THE anchor, in event-sourcing/load.ts
//   SnapshotReads<E>   = IfSnapshotCapable<E, { source: FusedSourceFunction }, unknown>
//   SnapshotDemand<E>  = IfSnapshotCapable<E, unknown, { snapshot?: <branded refusal> }>
// Contexts are ASSEMBLED BY INTERSECTION — `CommandHandlerContext<E, U>` is the base
// shape & `SnapshotReads<E>` — so against a bare log the fused
// `ctx.source(query, { snapshot })` overload is structurally ABSENT rather than
// present-and-complaining. Anything later anchors HERE; add a face, not a
// predicate. NOTHING RUNS: the whole demand is erased, and the only runtime
// trace is one defensive `throw` in `repository.ts` for JavaScript callers.
//
// THE RAW LAYER IS THE CAPABILITY; state() IS SUGAR OVER IT. Two primitives, off
// ONE object, and everything else here is those two with the key composition and
// the policy written for you:
//
//   const key = `course:${courseId}`
//   const { snapshot, events, position } = await ctx.source(query, { snapshot: key })
//   const state = events.reduce(fold, snapshot?.state as S ?? initial)
//   if (events.length > 100) await eventStore.storeSnapshot(key, { state, position })
//
// You wrote the key, you ran the fold, you judged the cached value, and the `if`
// IS the policy. `position` is the consistency marker the read reached — what
// the entry you write records as its own. THE WRITE IS A MEMBER OF THE LOG the
// slice already holds, so there is deliberately NO `ctx` write capability.
//
// THE KEY IS YOURS. A snapshot is filed under a STRING YOU WROTE — nothing is
// derived from your code and nothing is hashed. Which makes invalidation one
// sentence: CHANGED THE FOLD'S MEANING? CHANGE THE KEY. Rename "course-v1" to
// "course-v2" and every old entry is unreachable in the same instant, with no
// migration, backfill or version column. User-space, greppable, reviewable —
// and there is NO automatic rotation, which is the point: deciding when two
// folds are the same fold is a judgement about MEANING, and meaning is not
// derivable.
//
// THE STRATEGY IS SAID ON THE CONDITION, which is what lets ONE address serve
// every family:
//   type SourcingCondition = { query; start?; snapshot?: SnapshotKey }
//   type SnapshotKey       = { key: string }        // ONE user-composed string
//   type SourcingResult    = { events; marker; snapshot?: Snapshot }
// A bare `ctx.source(query)` sets none and reads the whole history, exactly as
// it always did. A log that was never wrapped IGNORES the key and sources in
// full, which is still correct — just not accelerated, and now not reachable by
// accident either.
//
// FUSING DOES NOT NARROW THE APPEND CONDITION. It narrows which EVENTS come
// back, not what was READ: both reads record the same query and the same marker
// on the task, so a fold seeded from a snapshot has exactly the DCB guarantee a
// full replay has.
//
// THE LEADING SNAPSHOT IS NOT AN EVENT. `SourcingResult.snapshot` is its own
// field, never a synthetic first element of `events`: an event is a fact with a
// name and a version and tags, and every reader of `events` is entitled to
// treat it as one. The repository starts from `snapshot.state` instead of
// `initial(id)` when it is there AND still fits.
//
// THE READ IS THE STORE'S; THE WRITE IS THE FOLD'S. `eventSourcedRepository`
// calls `storeSnapshot({ state, position })` when the policy fires — not because
// core knows an optimization, but because the thing being cached is the fold's
// own output at the fold's own position, and nobody else is holding both. A
// wrapper on the read path sees events go past, not the state they add up to.
// Fire-and-forget, failure swallowed: a cache write that could fail a load would
// make the cache load-bearing.
//
// SNAPSHOTS HAVE NO VERSIONS. Versioning is aggregate-era thinking: it assumes
// an ARTIFACT with a lineage to track across deploys. There is none here — a
// snapshot is a cache of ONE WAY OF FOLDING ONE QUERY, and a fold is a
// FUNCTION, not data. So there is no version column, no schema and no
// migration: there is a key you chose, and you change it when you mean to.
//
// STRUCTURAL FITNESS IS A SAFETY NET, NOT A SECOND KEY. The key is the gate
// that handles MEANING and nothing here second-guesses it. This guards the
// layer BELOW the fold: storage corruption, serializer drift, and shape drift
// you did not think of as a change of meaning. The specimen is `initial(id)` —
// free, always current, and a VALUE: no code is inspected. What it cannot catch
// is a change that keeps the structure and changes the meaning (cents to
// dollars is `number` either way) — THAT IS WHAT THE KEY IS FOR. Unfit ⇒
// DISCARD SILENTLY, replay in full, and the policy writes a fresh entry. At the
// raw layer there is no such check: fitness is your fold's judgement.
//
// A CACHE IS NEVER LOAD-BEARING. Miss, unfit entry, or an outright throw from
// the cache: all three fall back to full sourcing, silently.
//
// Composed with upcasting, the documented order is UPCASTING OUTERMOST:
//   upcastingEventStore(postgresSnapshottingEventStore(store, pg, { serializer }), upcast)
// The snapshot layer decides WHICH events are read; the upcast layer decides
// what each of them means. Both wrappers are capability-preserving, so BOTH
// orders keep BOTH capabilities in the type — the order is a semantic
// preference, never a typing constraint.

// ── scheduling: THE SECOND CAPABILITY TIER ON THE LOG ──────────────────────
// AN EVENT THAT HAS NOT HAPPENED YET IS STILL AN EVENT, and where it lands when
// its time comes is the LOG. So scheduling is not a seam beside the store, it is
// a tier ON it — added by wrapping, exactly like snapshotting, and demanded
// through the same construction.
type ScheduleCapability = {            // what a wrapper ADDS. TWO members, because
  schedule(event: EventMessage, at: Date, uow?: UnitOfWork): Promise<ScheduleToken>
  cancelSchedule(token: ScheduleToken, uow?: UnitOfWork): Promise<CancelResult>
}                                      // neither half was in EventStore's shape already
type ScheduleCapableEventStore = EventStore & ScheduleCapability
type ScheduleToken  = { id: string }   // opaque; PERSIST IT if you mean to cancel
type CancelResult   = { kind: "cancelled" | "already-appended" | "not-found" }
  // THREE OUTCOMES, AS NEWS RATHER THAN THROWS. Compensating for a deadline that
  // fired first is a branch, not an exception, and every family answers in the
  // same three words so it is written once.
//
// THREE WRAPPERS, ONE PER FAMILY THAT HAS ONE, each ADDITIVE — `E` in,
// `E & ScheduleCapability` out:
//   inMemorySchedulingEventStore(next, { clock? }?)                        // core; Map + setTimeout
//   postgresSchedulingEventStore(next, pg, { unitOfWork, tagResolver, … }) // table + polling worker
//   kronosDbSchedulingEventStore(next, kdb, { serializer })                // SERVER-SIDE; no timer here
// Each also adds what only IT can promise, under its own name so a composed
// store carries them without collision: `stopScheduling()` on the in-memory
// tier, `startScheduling()`/`stopScheduling()` on postgres, `listSchedules()` on
// KronosDB. Those are NOT in the capability — a capability is what every member
// of the tier can honestly answer.
//
// AXON SERVER HAS NO WRAPPER, and the absence is deliberate rather than an
// oversight: the generated protobuf carries a `DcbEventScheduler` service, but
// this package never built a client for it — no channel, no service definition,
// nothing in `connection.ts`. There was no scheduler to absorb, and inventing
// one here would be new functionality wearing a refactor's clothes.
//
// KRONOSDB IS THE ODD FAMILY OUT, and the tier makes it visible instead of
// hiding it: its schedules ALREADY RIDE THE KRONOSDB LOG, server-side, so the
// wrapper holds no table, no timer and no poller — and it does NOT join the
// caller's transaction, because the server owns the schedule the instant it is
// told. A handling that arms one and then throws HAS armed it. That was true of
// the standalone scheduler this absorbs; it is now written on the same object as
// the log, where a reader comparing the three families can see it.
//
// THE COMPILE-TIME DEMAND, and it is the mirror of snapshotting's:
//   IfScheduleCapable<E, Capable, Bare>       // THE anchor, in event-scheduling/schedule.ts
//   ScheduleVerbs<E> = IfScheduleCapable<E, { schedule; scheduleAfter; cancelSchedule }, unknown>
// Contexts intersect it, so against a bare log the three verbs are structurally
// ABSENT — "Property 'schedule' does not exist on type 'CommandHandlerContext'" at the
// call site, rather than `throw new Error("No event scheduler configured")` on
// the first deadline anybody armed in production.
//
//   eventHandler(OrderPlaced, async (m, ctx: EventHandlerContext) => {
//     await ctx.scheduleAfter(PaymentTimedOut, { orderId }, 900_000)   // ✗ property does not exist
//   })
//
//   eventHandler(OrderPlaced, async (m, ctx: EventHandlerContext<ScheduleCapableEventStore>) => {
//     await ctx.scheduleAfter(PaymentTimedOut, { orderId }, 900_000)   // ✓ and the entry's log must be wrapped
//   })
//
// ONE FACE, NOT TWO. Snapshotting needed a second (`SnapshotDemand`) because a
// STATE can declare that it caches, so there was something to refuse as well as
// something to offer. Nothing declares that it schedules — a handler calls the
// verb or does not — so this side of the mirror is one conditional and the
// construction is otherwise identical.
//
// NOTHING RUNS: the demand is erased, and the only runtime trace is one
// defensive assert for JavaScript callers, whose message names the CAPABILITY
// and the wrapper PATTERN (`<family>SchedulingEventStore(store, …)`) rather than
// any one family — core does not know which one this host chose.
//
// THE THREE VERBS ARE UNCHANGED where they exist: `ctx.schedule(d, p, at, md?)`,
// `ctx.scheduleAfter(d, p, delayMs, md?)` — the delay measured from `uow.now()`,
// so a frozen clock gives a predictable fire time — and
// `ctx.cancelSchedule(token)`. What changed is where the capability comes from.

// ── correlation: THE FUNCTIONS YOU WRAP IN ─────────────────────────────────
// Correlation is the CARRYING MECHANISM — metadata jumping from the message a
// handler is handling onto every message that handling births, and on down the
// chain. The correlationId/causationId pair is the DEFAULT CARGO of that
// mechanism, not the mechanism itself.
correlating(uow: UnitOfWork): CorrelatingUnitOfWork     // a task that carries a map
type CorrelatingUnitOfWork = ReturnType<typeof correlating>   // DERIVED, never hand-written
  // + correlationData(): Record<string, string>
  // + attachCorrelationData(partial: Record<string, string>): void
correlatingHandler(next, from: (message: Message) => Metadata): next   // the wrapper
correlation: Intercept        // the EDGE intercept: correlationId ?? identifier ·
                              // causationId ?? identifier (SEEDS ROOTS ONLY — a
                              // wrapped handler re-stamps causation per hop)

// `from` is REQUIRED and never defaulted OR shipped: the mechanism has no
// opinion about what is worth carrying, and even the id pair is the host's two
// lines (DOCUMENTED, not exported — it teaches the mechanism):
//   const correlationFrom = (parent: Message): Metadata => ({
//     correlationId: String(parent.metadata.correlationId ?? parent.identifier),
//     causationId: String(parent.identifier),          // causation = the parent, ALWAYS
//   })
// Compose more cargo by composing a FUNCTION:
//   correlatingHandler(h.handler, (m) => ({ ...correlationFrom(m), actor: String(m.metadata.actor) }))
//
// THE BOOTSTRAP IDIOM — two lines, and the compiler ties them together:
//   const uow = () => correlating(unitOfWork(clock))
//   kronos({ commandHandlers: handlers
//     .map((h) => ({ ...h, handler: correlatingHandler(h.handler, correlationFrom) }))
//     .map((h) => ({ ...h, commandBus: localCommandBus(uow), queryBus, eventStore })) })
//
// OPT-IN, WITH A CONDITIONAL DEMAND — MADE ON THE WRAPPER'S OUTPUT. The handler
// names no task: `correlatingHandler(h)` takes any `(m, ctx: C) => R` and gives
// back `(m, ctx: C & { unitOfWork: CorrelatingUnitOfWork }) => R`, so wiring it
// against a bus or processor built from a bare `() => unitOfWork()` is a COMPILE
// ERROR at the entry, and the handler file never knew. Carrying is done TO a
// handling; a handler writes `U` only if it reaches for the map itself. Wrap
// neither and the concept appears nowhere in your types. The demand exists only for the
// hosts that composed one — an unconditional demand would propagate
// contravariantly through every transport, which is why the previous attempt
// (correlation hardcoded into ctx and the bus signatures) was reverted.

// ── edge verbs: build the message, dispatch it. Nothing named "gateway". ───
send(bus: CommandBus, descriptor, payload, metadata?): Promise<Result>
query(bus: QueryBus, descriptor, payload, metadata?): Promise<Result>
subscriptionQuery(bus: QueryBus, descriptor, payload, metadata?): SubscriptionQueryResult
// THEY ARE UNCLOSED ON PURPOSE, and the host closes them out ONCE (documented,
// not exported — it is two lines and they are the host's two lines):
//   export const dispatch = (d, payload, actor) => send(commandBus, d, validate(d, payload), { actor })
//   export const ask      = (d, payload, actor) => query(queryBus, d, validate(d, payload), { actor })
// Controllers never see the unclosed verb, so validation and per-request
// metadata are one visible decision in one file. The edge can await, so an
// asynchronous schema is at home here.

// ── handlers: three contexts (the safety), one definition shape ────────────
commandHandler(descriptor, (message, ctx: CommandHandlerContext) => result)
queryHandler(descriptor,  (message, ctx: QueryHandlerContext) => result)
eventHandler(descriptor,  (message, ctx: EventHandlerContext) => void)
// A HOST NAMES ITS OWN CONTEXT ONCE and every handler writes that one word:
//   type UniversityCommandContext =
//     CommandHandlerContext<SnapshotCapableEventStore & ScheduleCapableEventStore> & EmitCapability
// The app's floor lives on ONE line — add a tier and no handler is edited.
// Named for the thing it belongs to, then the kind, exactly as the adapter
// packages name theirs (`PostgresCommandContext`, `DrizzleEventContext`).
// Keep it a FLOOR: name tiers, never a concrete store or bus.
type EventHandlerContext<E extends EventStore = EventStore, Q extends QueryBus = QueryBus, U extends UnitOfWork = UnitOfWork> =
  { load · source · send · query · isReplay · unitOfWork: U }
  & SnapshotReads<E> & ScheduleVerbs<E> & SubscriptionEmit<Q>   // ← ALL THREE TIERS, one intersection each
type CommandHandlerContext<E, Q, U> = EventHandlerContext<E, Q, U> & { append }  // the atomic decide-append boundary
type QueryHandlerContext<E, U> = { load · source · query · unitOfWork: U } & SnapshotReads<E>
  // A query handling gets NO scheduling verbs at any tier — a read gives birth to nothing.
  // PARAMETERS IN FREQUENCY ORDER OF WHAT A HANDLER WRITES: `E` first (a log that caches
  // folds, a log that holds deadlines), `Q` second (a bus that serves live subscribers —
  // rare), `U` last and almost never written: the task is something done TO a handling
  // (a wrapper demands it on its output, a bus mints it), and a handler names it only when
  // it reaches for the task directly. Each is the ENTRY's object, threaded from the
  // composition root through the subscribe glue, defaulted so plain code never writes any
  // of them. Buses never carry a store; entries carry both.

// ── the two reads: state() derives, ctx.source lets you write ──────────────
ctx.source(query: EventQuery): Promise<ReadonlyArray<EventMessage>>
ctx.source(query: EventQuery, opts: { snapshot: string }): Promise<SnapshottedSource>
  // ↑ the second one EXISTS ONLY when `E` is snapshot-capable — contributed by
  // `SnapshotReads<E>`, absent otherwise, so a bare log's `ctx.source` takes ONE argument.
type SnapshottedSource = { snapshot: Snapshot | undefined; events; position: bigint }
  // THE RAW LAYER — you write the query, you run the fold, the append condition still holds.
  // The read is recorded on the task exactly as `ctx.load`'s is, so a hand-rolled `is()` +
  // `reduce` gets the identical DCB guarantee a decision state gets. Declaring `types`
  // narrows the conflict window; omitting it is legal and widens it — that narrowing is one
  // of the things `state()`'s derivation was doing for you.
  //
  // THE PLAIN CALL IS UNCHANGED — an events array, exactly as before. WITH A SNAPSHOT KEY
  // it is the FUSED read: the latest entry filed under that EXPLICIT string, plus only the
  // events after its position (full history + `undefined` on a miss). `position` is the
  // consistency marker the read reached — what a snapshot you write from this fold records
  // as its own. Sourcing is recorded IDENTICALLY either way, so the append condition holds
  // unchanged: fusing narrows which EVENTS come back, not what was READ. Served through the
  // same `SourcingCondition.snapshot` path a wrapped store serves — see snapshotting above.
  // THIS PLUS `eventStore.storeSnapshot(key, …)` IS THE WHOLE CAPABILITY, and BOTH halves
  // come off the ONE object the entry already carries; `state({ snapshot })` is those four
  // lines with the key composition and the policy written for you.

// EVERY BIRTH VERB TAKES A TRAILING `metadata?`, mirroring the edge verbs:
//   ctx.send(descriptor, payload, metadata?)      ctx.append(descriptor, payload, metadata?)
//   ctx.query(descriptor, payload, metadata?)     ctx.append([[D, p, metadata?], …])
//   ctx.schedule(d, p, at, metadata?)             ctx.scheduleAfter(d, p, delayMs, metadata?)
// A birth's metadata is EXACTLY that argument. ctx carries NOTHING over from the
// message being handled — carrying is a policy, and a policy belongs to a host.
// `correlatingHandler` is the mechanism, and this parameter is where it injects.

// ── event delivery: tracked only; the processor is a value ────────────────
type Sequence = (event: EventMessage) => string    // TOTAL — (e) => e.identifier = own lane = no constraint
sequentialPerTag(key: string): Sequence            // one-line transparent helper
eventProcessor<U extends UnitOfWork = UnitOfWork, L extends EventProcessorLane<U> = …>({
  name: string,
  eventStore: EventStore, tokenStore: TokenStore<U>, unitOfWork: () => U,   // constitutive
  sequence?: Sequence,                             // absent = global stream order (projection-safe)
  deadLetterQueue?: SequencedDeadLetterQueue<U>,   // absent = propagate & retry
  batchSize?: number,
}): EventProcessor<U>          // `U` threads to the event handler's ctx.unitOfWork
// THE PROCESSOR IS WHERE THE COMPILER MEETS THE FACTORY AND THE STORES, and it
// now enforces TWO things it used to discover at boot:
//   1. A DEAD-LETTER QUEUE IMPLIES A LANE. `deadLetterQueue` without `sequence`
//      does not compile — the demand adds a required `sequence` whose type is an
//      anonymous ERROR/FIX record, so the compiler prints the keys at the wiring
//      site. The construction-time throw survives as ONE defensive assert, for
//      JavaScript callers.
//   2. NO MIXING PERSISTENCE FAMILIES. `tokenStore` and `deadLetterQueue` are
//      keyed on `U`, and a family's stores demand that family's brand — see the
//      persistence section. `U` is inferred from `unitOfWork` (its one COVARIANT
//      mention), and the stores are checked against the answer.
type EventProcessorSite<U> · EventProcessorLane<U> · EventProcessorConfig<U> · SequenceDemand<C>

// ── assembly: three lists. Nothing else. ──────────────────────────────────
kronos<U extends UnitOfWork = UnitOfWork>({
  commandHandlers?: ReadonlyArray<CommandHandlerEntry<U>>,   // handler ctx, commandBus, queryBus all keyed on U
  queryHandlers?:   ReadonlyArray<QueryHandlerEntry<U>>,
  eventHandlers?:   ReadonlyArray<EventHandlerEntry<U>>,     // …and the processor too
}): App
// THREE, because `kronos` registers BEHAVIOUR and a state is DATA. There is no
// `states` list, no `StateEntry` and no `StateOptions`: `ctx.load(Course, id)`
// names the state at the call site and takes the log off the entry's site, and
// the repository for that pair is built on first use and remembered in a weak
// per-store cache. Losing the cache costs a rebuild; losing a registry used to
// cost everything.
// The entry types are what tie a handler's demand to the infrastructure it is
// wired against: one `U` per app, inferred, defaulting to the bare UnitOfWork.
// A correlatingHandler-wrapped handler + a bare bus = a compile error naming
// the field that disagrees.
// THERE IS NO `eventScheduler` FIELD. There is no scheduler seam left to put on
// one: scheduling is a tier on the entry's `eventStore`, so which log an
// automation arms a deadline in is the same deployment fact as which log it
// reads, said once. A site that can schedule is a site whose log was wrapped.
type App = { processors: ReadonlyMap<string, RunningProcessor>; stop(): Promise<void> }
// grouping: processors by NAME (the durable identity — tokens persist by name
// across restarts). Same name + equal config ⇒ one delivery; same name +
// conflicting config ⇒ boot error. Stores are not grouped at all any more —
// entries naming one `eventStore` OBJECT share its folds because the lazy cache
// is keyed on that object, which nobody had to declare.
// boot errors name the entry: missing eventStore, missing processor, processor field conflicts

// ── decision models ────────────────────────────────────────────────────────
state({ id, tags: (id) => TagRecord | readonly [TagRecord, ...],
        evolve: [(id) => S, [EventDescriptor, fold], ...], snapshot?, lifecycle? })
  // FIELD ORDER IS READING ORDER: tags are a function of id; evolve carries its own initial state
  // ELEMENT ZERO IS THE INITIAL STATE — the evolver of nothing; it may read the identity it
  // is folded for. The fold is `cases.reduce(...)` starting from evolve[0](id), so the
  // initial lives in the same list as the evolvers of something, and `S` is read off it.
  // IT IS HANDED THE ID, the same inferred record `tags` takes: nothing has happened yet, so
  // the identity is the one thing a zeroth state can honestly know, and a fold that carries
  // its own key stops having to learn it from an event. An initial that does not care
  // DECLINES the argument — `() => ({…})` stays assignable to `(id) => S` by arity.
  // Destructured once (`const [initial, ...cases] = evolve`); no Array.isArray anywhere.
  // It is also the SPECIMEN the snapshot fitness check is made against — see snapshotting.
  // DCB query DERIVED per event type from tags × evolve.slice(1)
  // `snapshot` is `{ key, when }` — WHERE entries are filed and WHEN one is written. `key`
  // is REQUIRED and EXPLICIT: a string YOU wrote, nothing derived and nothing hashed.
  // state() files entries under `${key}:${snapshotIdentifier(id)}`, so one declared key
  // serves every id without collision. INVALIDATION IS ONE SENTENCE: changed the fold's
  // meaning? change the key — greppable, reviewable, and never automatic. There is NO
  // `name` field on `state()`. The config rides on the state; the CAPABILITY is a SITE fact
  // riding on the entry's `eventStore`, and writing the config makes wiring it a COMPILE-TIME
  // obligation — `state()` returns `State<Id, S, true>`, and `ctx.load` refuses that against
  // a log which cannot serve it. Diagnostics name a state by its process `identity` plus the
  // events it folds.

// ── store seams + in-memory implementations ────────────────────────────────
type EventStore                       // COMPLETE for event sourcing; says nothing about caches or deadlines
type SnapshotCapableEventStore = EventStore & SnapshotCapability   // added by WRAPPING
type ScheduleCapableEventStore = EventStore & ScheduleCapability   // added by WRAPPING
type TokenStore<U = UnitOfWork>              // members take (processorName, …, uow?: U)
type SequencedDeadLetterQueue<U = UnitOfWork> // members take (processingGroup, …, uow?: U)
  // FUNCTION-TYPED FIELDS, NOT METHOD SHORTHAND, and that is load-bearing: TS checks
  // method parameters BIVARIANTLY, so a store demanding a branded task would have been
  // silently assignable to a bare slot and the family demand would be decoration.
type UnitOfWorkBrand<Name extends string, Fix extends string>   // the family SLOT — see persistence
type TagResolver = (event: EventMessage) => Tag[]
inMemoryEventStore() · inMemorySnapshottingEventStore(next) · inMemoryTokenStore()
inMemorySchedulingEventStore(next, { clock? }?)   // clock absent = system time

// ── serialization: the wire, for adapters that own one ─────────────────────
type Serializer · SerializedObject · SerializerDecorator
jsonSerializer()
  // ENCODING, AND NOTHING ELSE. No validating decorator and no registry for one
  // to read: a serializer holds a type NAME and a revision, which is the only
  // reason validating from inside it ever needed a lookup. See `validation/`.
  // Room to grow — a binary serializer lands beside this one.

// ── validation: the gate. TWO functions, and NO registry ───────────────────
validate(descriptor, payload): InferOutput<D["payload"]> | Promise<…>
validatingHandler(next, descriptor): (message, ctx) => Promise<Awaited<R>>
  // WHY NO REGISTRY: every site that validates already holds the descriptor as
  // an ARGUMENT — the edge verbs take one, the ctx birth verbs take one, an
  // entry pairs one with its handler — and a descriptor carries its own payload
  // schema. A registry answers "which schema goes with this type name"; nobody
  // needs to ask. Nothing registers, nothing is looked up, and losing the wiring
  // is a compile error rather than a silently unvalidated message type.
  //
  // `validate` is the PRIMITIVE, usable wherever a descriptor is in hand. What
  // comes back is the PARSED value — standard validation is a parse, so a
  // schema's coercions and defaults are part of what it says, and the returned
  // value replaces the input. A sync schema answers synchronously (no await
  // tax); an async one answers a promise the caller awaits. Failure throws,
  // naming the qualified name and joining the issues.
  //
  // `validatingHandler` is the MECHANISM, composed at the ENTRY — the one place
  // a descriptor and a handler already sit together:
  //   .map((h) => ({ ...h, handler: validatingHandler(h.handler, h.descriptor) }))
  // INBOUND it validates against the ENTRY's descriptor and hands `next` a
  // message whose payload is the parse. OUTBOUND it overlays the ctx birth verbs
  // exactly the way `correlatingHandler` does — wrapping only the verbs the
  // context has — and each verb validates against the descriptor IT was called
  // with (append's batch form: per tuple). `send`/`query` already answer
  // promises, so an async schema is awaited; `append`/`schedule` give birth in
  // the caller's turn, so an async schema there throws, naming the verb.
  // It demands NOTHING of `C`, so a wrapped handler wires against exactly the
  // buses the unwrapped one did, and it composes with `correlatingHandler` and
  // `otlpHandler` in any order.
```
Core contains ZERO tracing vocabulary, and no resilience vocabulary either: the
retry/health helpers are a package-private module in each of the three packages
that use them (axon-server · kronosdb · postgres). They only ever touched
`setTimeout` and a function, which makes them a helper, and a helper lives with
its users — the same rule that moved the transaction glue out.

## @kronos-ts/rabbitmq — dumb pipe: client-side routing, configurable
```ts
rabbitMqConnection(url, { serviceName, instanceId, topology?, retry? }): Promise<RabbitMqConnection>  // start()/close()
rabbitMqCommandBus(next: CommandBus, rabbit, { preferLocal?, timeoutMs? }?): CommandBus
rabbitMqQueryBus(next: QueryBus, rabbit, { preferLocal?, timeoutMs? }?): QueryBus
```

## @kronos-ts/kronosdb — smart hub: server-side routing
```ts
kronosDbConnection({ host, port, componentName }): Promise<KronosDbConnection>   // one channel; start()/close()
kronosDbEventStore(kdb, context: string): EventStore
kronosDbSnapshottingEventStore<E>(next: E, kdb, context): E & SnapshotCapability
kronosDbSchedulingEventStore<E>(next: E, kdb, { serializer }): E & ScheduleCapability & { listSchedules() }
kronosDbCommandBus(next: CommandBus, kdb, bus?: string): CommandBus   // inbound runs through YOUR local bus
kronosDbQueryBus(next: QueryBus, kdb, bus?: string): QueryBus
kronosDbControlPlane(kdb, processors): ControlPlane
```
BUSES ARE NAMES, CONTEXTS ARE LOGS (server ADR-0006, 0.9). The store wrappers
address a CONTEXT and the bus wrappers address a BUS, and the two are
independent dimensions — plain strings both, each defaulting (`config.context` /
`"default"`). Messaging RPCs carry `kronosdb-bus` per call with NO fallback to
`kronosdb-context`; a 1:1 topology is the host naming the bus after the
context, and N contexts sharing one command fabric is N stores and one bus
name. Since 0.9 the bus name is also the CLUSTER-WIDE identity (ADR-0007):
subscribe via any node, dispatch via any node, the server forwards.

## @kronos-ts/axon-server — same family as kronosdb
```ts
axonServerConnection(…) · axonServerEventStore(conn, ctx)
axonServerSnapshottingEventStore<E>(next: E, conn, ctx): E & SnapshotCapability
axonServerCommandBus(next, conn) · axonServerQueryBus(next, conn) · axonServerControlPlane(conn, processors)
```
KRONOSDB FUSES THE READ NATIVELY, IN ONE CALL. Since 0.8 its snapshots ride the
log (ADR-0005): the standalone `SnapshotStore` service is GONE, and `EventStore`
serves `AppendSnapshot` (the write) and `SnapshottedSource` (the fused read —
≤1 snapshot frame, always first, then event batches, then the SAME consistency
marker a plain `Source` ends with, so an append condition holds identically on
both paths). There is no client-side fallback. The marker is NEXT-EXCLUSIVE, so
the server resumes AT `snapshot.position`, never at `position + 1` — which is
what the deleted client-side fusion did, and it could drop an event that landed
between the fold and the snapshot write. `GetSnapshot` also exists server-side
and is deliberately NOT wired: reading a cached fold is not a second call.

AXON SERVER STILL FUSES CLIENT-SIDE — `getLast`, then a source after its
position, inside the one function. It serves `DcbSnapshotStore`
(Add/Delete/List/GetLast) — probed against `axoniq/axonserver:2025.2.5` AND
`2026.0.4`, both answer — but `SnapshottedDcbEventStore.Source`, the fused RPC
the public API describes, is UNIMPLEMENTED on both, so there is nothing to call.
When it lands it changes ONE function body and no host's code, because the
capability was never a promise about round trips. It is latest-only over a
service that is not: `add` sets `prune: true` and `getLast` is the only read,
which is the narrowing doing its job — the framework programs against the cache
it needs, not everything a backend offers.

## @kronos-ts/postgres — the FULL persistence family, no ORM required
```ts
postgresPool(connectionString | adapter): PostgresResource   // start()/close()
postgresEventStore(pg, { tagResolver }): EventStore   // no serializer; knows no snapshots
postgresSnapshottingEventStore<E>(next: E, pg, { serializer }): E & SnapshotCapability
postgresUnitOfWork<U>(next: () => U, pg): () => U     // THING-FIRST; lazy tx — its honest default
postgresTokenStore(pg): TokenStore                    // joins the SAME tx as your raw-sql writes
postgresDeadLetterQueue(pg): SequencedDeadLetterQueue
postgresTransaction(uow) · activePostgresTransaction(uow)
postgresHandler(handler, pg): handler                 // wraps the FUNCTION; ctx gains sql(): Sql | Tx
type PostgresCommandContext = CommandHandlerContext & { sql(): Sql | Tx }
postgresSchedulingEventStore<E>(next: E, pg, { unitOfWork, tagResolver, pollIntervalMs?, batchSize? }):
  E & ScheduleCapability & { startScheduling(); stopScheduling() }
type PostgresUnitOfWork                                   // the family brand — see below
```
POSTGRES FUSES THE READ IN ONE ROUND TRIP, and "fused" is not a feature — it is
just OWNING THE QUERY. `postgresSnapshottingEventStore.source` handles
`condition.snapshot` itself: a CTE over `kronos_snapshots` keyed on the single
`key` column, a start position derived from the entry it found, the event query
from there and the head, in ONE statement. A wrapper that had to talk over a
wire cannot beat that, because two calls is the best a wire allows. ONE WRAPPER
OWNS BOTH HALVES — the upsert and the fused read — so there is ONE serializer on
ONE object, and the class of mistake where the writing side and the reading side
got different ones no longer exists.
ADAPTER UNIT-OF-WORK DECORATORS ARE THING-FIRST AND CAPABILITY-PRESERVING:
`<pkg>UnitOfWork(next, client)` — the factory being decorated comes first, the
client is configuration. Each returns `() => U` for whatever `U` it was handed
and decorates the SAME handle rather than rebuilding a record, so a composed
capability survives both the type and the runtime:
```ts
const uow = drizzleUnitOfWork(() => correlating(unitOfWork(clock)), db)
//    ^ () => CorrelatingUnitOfWork, with its transaction keyed on that very object
```

PRINCIPLE: persistence families are keyed by TRANSACTION IDENTITY — the token
store/DLQ must write through the same client handle the handlers write through.
Every persistence package (postgres, drizzle, knex, kysely, prisma, typeorm)
implements the same seven-function family for its client type.

NEVER MIX FAMILIES WITHIN ONE PROCESSOR — AND NOW YOU CANNOT. That sentence used
to be prose, which means it was a sentence nobody could read at 2am while wiring
a processor out of two packages that both export a `tokenStore`. It is a type
now, and it is the clearest case in this surface of the law it serves: the
failure it prevents is not a throw, it is SILENCE. A drizzle token store handed a
postgres unit of work does not fail — it asks for ITS transaction, is told there
is none, and falls back to its plain handle. The token update commits OUTSIDE the
batch. Every test passes. Then a crash lands between the projection write and the
token write and a read model is permanently wrong in a way nobody can
reconstruct.

Each package brands what its unit-of-work decorator MINTS, and demands that brand
back in its stores' uow-accepting signatures:
```ts
// core owns the SLOT and knows no occupants:
type UnitOfWorkBrand<Name extends string, Fix extends string>   // phantom on an ambient unique symbol

// each package writes ONE line, and it is the only place the family is named:
type DrizzleUnitOfWork = UnitOfWorkBrand<"drizzle", "build this processor's unitOfWork with drizzleUnitOfWork(next, db)">

const uow = drizzleUnitOfWork(() => correlating(unitOfWork(clock)), db)
//    ^ () => CorrelatingUnitOfWork & DrizzleUnitOfWork — the brand and the capability coexist

eventProcessor({ name, eventStore, tokenStore: drizzleTokenStore(db), unitOfWork: uow })      // ✓
eventProcessor({ name, eventStore, tokenStore: drizzleTokenStore(db), unitOfWork: pgUow })    // ✗ compile error
eventProcessor({ name, eventStore, tokenStore: inMemoryTokenStore(), unitOfWork: uow })       // ✓ bare demands nothing
```
ERASED, AND NEVER CONSTRUCTED. The brand is a phantom on a unique symbol core
declares AMBIENTLY; no JavaScript writes it and none can read it, and each
decorator returns exactly what it always returned and asserts the branded type.
Emitted output is byte for byte unchanged.

SOURCE-COMPATIBLE FOR SAME-FAMILY USERS: a host already following the rule
changes nothing. The brand only refuses arrangements that were already broken.

ONE FAMILY OWNS; OTHERS MAY RIDE AS LENSES. The brand refuses two families each
holding a transaction, not two libraries on one connection: the postgres
transaction's `unwrap()` returns the live driver handle — the SAME connection
the event store appends on — and an ORM bound to it (`drizzle(tx.unwrap())`)
joins that one transaction. So a processor whose handlers must write tables AND
append atomically is a POSTGRES-family processor with the ORM as a lens; the
ORM families exist for processors that only project, where the atomic pair is
projection-write + token-write. Documented in `docs/how-it-works.md`
("Transactions: one owner, lenses for the rest").

THE DIAGNOSTIC BELONGS TO THE PACKAGE, and that is why `Fix` is a parameter. Core
is certain the two families differ and certain of nothing else; the package that
owns the store knows exactly which factory the host should have called. So the
brands differ FIRST on their `FIX` string, and the checker prints that sentence
at the wiring site:
```
Type 'UnitOfWork & PostgresUnitOfWork' is not assignable to type 'DrizzleUnitOfWork'.
  The types of '[unitOfWorkBrand].FIX' are incompatible between these types.
    Type '"build this processor's unitOfWork with postgresUnitOfWork(next, pg) — …"'
      is not assignable to type '"build this processor's unitOfWork with drizzleUnitOfWork(next, db) — …"'.
```
THE GENERAL RULE FOR EVERY DIAGNOSTIC HERE: an error may only be as specific as
the code that owns it is CERTAIN about. Core-owned refusals name the capability
and the wrapper PATTERN (`<family>SnapshottingEventStore(store, …)`); a package's
own refusals name its own functions. Specific-when-certain beats generic;
generic-when-uncertain beats wrong.

EACH PACKAGE OWNS ITS TRANSACTION GLUE, PRIVATELY. The registry (a WeakMap keyed
by unit of work), the factory builder and the open/observe accessors behind
`<pkg>Transaction` / `active<Pkg>Transaction` are a package-private module in
every one of the six — `src/transaction-glue.ts`, exported from no barrel. Core
has no `./transaction` subpath and no transaction vocabulary of any kind: that
glue only ever touched the PUBLIC phase API (`uow.on(Phase.COMMIT, …)`,
`uow.onError(…)`), which is what makes it a helper rather than a primitive, and
a helper lives with its users. Owning it is also what lets each family state its
own binding honestly — eager (drizzle · knex · kysely · prisma · typeorm: a
PRE_INVOCATION hook forces the transaction open, because the token store and DLQ
read through the OBSERVING accessor and must not be left outside it) or lazy
(postgres: claimed at mint, begun only when a writer asks, so read paths pay no
begin/commit and claim no connection).

## @kronos-ts/drizzle (knex · kysely · prisma · typeorm: identical family)
```ts
drizzleTokenStore(db): TokenStore<UnitOfWork & DrizzleUnitOfWork>
drizzleDeadLetterQueue(db): SequencedDeadLetterQueue<UnitOfWork & DrizzleUnitOfWork>   // group per call
drizzleUnitOfWork<U>(next: () => U, db): () => U & DrizzleUnitOfWork   // eager tx; delegate EXPLICIT
type DrizzleUnitOfWork = UnitOfWorkBrand<"drizzle", "build this processor's unitOfWork with drizzleUnitOfWork(next, db)">
drizzleTransaction(uow): Promise<Tx>            // opens; REJECTS on a non-drizzle uow
activeDrizzleTransaction(uow): Tx | undefined   // observes, never opens
drizzleHandler(handler, db): handler            // ONE generic wrapper: ctx gains db()
type DrizzleCommandContext = CommandHandlerContext & { db(): Db | Tx }             // + Event/Query variants
```
HANDLER WRAPPERS ARE FUNCTION-LEVEL — `(next, ...config) => (message, ctx) => result`,
with `<M, C, R>` inferred and no entry type anywhere. The host wraps by spreading
the entry itself, and anything a wrapper would have read off the entry it reads
off the MESSAGE instead:
```ts
const instrumented = <M extends Message, C extends DrizzleCapability & { unitOfWork: UnitOfWork }, R>(
  next: (m: M, c: C) => R,
) => drizzleHandler(otlpMetricsHandler(otlpHandler(next, exporter), exporter), db)

kronos({
  commandHandlers: slices.flatMap((s) => s.commandHandlers)
    .map((h) => ({ ...h, handler: instrumented(h.handler) }))
    .map((h) => ({ ...h, commandBus, queryBus, eventStore })),
})
```
The NAMES are unchanged — `<pkg>Handler` still, because a shared-package export
must carry its provenance. What changed is the LEVEL: the argument is the handler
function, not the entry. The erasure is DIRECTIONAL — `db()` in, base ctx out —
so a wrapper ordered wrong is a compile error. Wrappers that supply nothing
(tracing, metering) erase nothing and compose on either side. The per-kind names
(`drizzleCommandHandler` ×3), the entry-constraint types (`DrizzleHandlerEntry`,
`WithDrizzleSupplied`, `PostgresHandlerDefinition`, `Supplied`, `OtlpHandlerEntry`,
and the knex · kysely · prisma · typeorm equivalents) are gone, and no wrapper
reads the entry's `name` field any more — that field stays what it always was, a
diagnostics label kronos groups by.

## @kronos-ts/otlp — the protocol, not the ecosystem
```ts
otlpExporter({ endpoint, serviceName, flushIntervalMs? }): OtlpExporter   // resource: batches, close()
otlpCommandBus(bus, exporter): CommandBus       // span per dispatch; W3C traceparent into metadata
otlpQueryBus(bus, exporter): QueryBus
otlpHandler(handler, exporter, label?): handler        // parents (command/query msgs) / links (event msgs)
otlpMetricsHandler(handler, exporter, label?): handler // duration / throughput / failure counters
  // label?: (message: Message) => string — ABSENT = the message's qualified name.
  // Parent-vs-link and SERVER-vs-CONSUMER come from `message.kind`; there is no
  // kind argument and no entry to ask, so both wrappers are pre-appliable.
```
Zero `@opentelemetry/*` dependencies. No SDK, no global tracer, no patching.
otel-js interop = the consumer writes wrappers over the same public shapes.

## @kronos-ts/test — a test is a VALUE, run at a SITE
```ts
// ── the vocabulary: one constructor per message kind, doubling as the expectation
event(descriptor, payload, metadata?): EventValue        // a fact, an arrival, or an expectation
command(descriptor, payload, metadata?): CommandValue    // the act, or an expected dispatch
query(descriptor, payload, metadata?): QueryValue        // the act (a read is never a consequence)
  // payloads are compile-checked against the descriptor; `any()` widens, never disables
any(schema?: StandardSchemaV1): Any                      // the positional payload hole — renders as `*`
type Duration = number                                   // ms, the unit a clock reads

// ── assertion values for `then` (each CATEGORY judged only if mentioned)
result(value)                     // command → handler return · query → the answer · event act = fixture error
error(matcher: string | RegExp | ((e: unknown) => boolean))    // thrown only; asserting it DECLINES the throw
noEvents() · noCommands()         // the empty case of the list, said out loud
scheduled(event(...), after: Duration) · cancelled(event(...))

// ── the grammar: order is real, so the joints are a pipe
given(...events): ScenarioStart          // free-standing entry; given() empty ≡ scenario()
scenario(): ScenarioStart
  .wait(duration)  → same shape          // chainable at any joint, repeatable
  .when(action)    → ScenarioActed       // EXACTLY ONE command | query | event (singular)
  .wait(duration)  → ScenarioActed
  .then(...assertions) → Scenario        // terminal
type Scenario = { steps · then · description }      // "given X, wait 1000ms, when Y, then Z"
  // pure and immutable: each step returns a NEW value, so one prefix finishes many ways,
  // and one finished Scenario runs against as many fixtures as you like

// ── the site
testFixture(
  scope: (eventStore: FixtureEventStore) => lists,   // ONE store · THREE lists — no `states`
  opts?: { within?: Duration; clock?: () => number; realTime?: boolean },
): { run(scenario, opts?): Promise<{ result, events, commands }> }
FIXTURE_EPOCH                     // where the clock starts when nobody says otherwise
type PartialProcessor = (eventStore, tokenStore, unitOfWork, deadLetterQueue) => EventProcessor
type FixtureUnitOfWork = CorrelatingUnitOfWork    // what the site mints, named
ScenarioAssertionError            // message IS the diff
```
The fixture is a COMPOSITION ROOT, so it composes like one: its tasks are
`() => correlating(unitOfWork(clock))` and every handler the scope hands it is
wrapped with the id pair as cargo (its own two lines, the documented idiom) — the id pair is the
fixture's cargo choice, because a causal chain is what a scenario is about and
what a `then` can meaningfully assert. A scope wanting other cargo wraps its own
handlers first.

The fixture CREATES the resources and hands them to the scope, which is a
function of them — the same function a process deploys. It owns
```ts
recordingEventStore(
  controllableSchedulingEventStore(inMemorySnapshottingEventStore(inMemoryEventStore()), clock),
)
```
— the fixture composes what a host composes, BOTH STORE TIERS included, with the
recorder outermost so `appended` is what left the fixture (all three wrappers are
ADDITIVE, so every capability survives every layer above it) —
`inMemoryTokenStore()`, `inMemoryDeadLetterQueue()`,
`recordingCommandBus(localCommandBus(uow))` and
`recordingQueryBus(localQueryBus(uow))`, calls the scope FULL-HANDED (a shorter
parameter list declines the rest), completes any `PartialProcessor` on an
event-handler entry with its own resources, and wires the result kronos-style.

ONE RESOURCE, NOT THREE. The scope's single `eventStore` parameter is now the log,
the fold cache AND the schedule book; there is no scheduler to accept and no field
to put it in, and a scope that arms a deadline typechecks against the fixture for
the same reason a scope that caches a fold does. `foreign` detection collapsed with
it: bringing a foreign scheduler IS bringing a foreign store, so there is one check
where there were two.

Semantics, pinned:
- `given` events APPEND straight to the log; every processor FAST-FORWARDS its
  cursor past them WITHOUT invoking a handler. History is history — the
  automations that would have fired already fired, long ago.
- `when(command | query)` dispatches through the real bus. `when(event)` means the
  event ARRIVES: appended, and the processors DO react. That is the automation
  shape.
- After the act and after each `wait`: QUIESCE — drain dispatched commands, drive
  every processor to the head of the log IT reads, settle, repeat until nothing
  moves. Deterministic; no sleeps.
- `wait(d)` jumps the fixture clock, fires the schedules now due (in FIRE-TIME
  order, each event stamped with the instant it fires), then quiesces. If the
  scope brought resources the fixture does not own, `wait` throws a clear error
  unless `opts.realTime`, where it genuinely elapses (`setTimeout d`).
- `then`: `event()`/`noEvents()` ⇒ the EXACT ORDERED list of new events, nothing
  missing or extra; `command()`/`noCommands()` the same over dispatches (the act's
  own command excluded). `result()` is deep-strict with `any()` holes. `error()`
  matches the throw. `metadata` on a then-value is a SUBSET claim over the keys it
  names. Against a real-infrastructure scope the claims are re-judged until
  `opts.within` (default 5s); an all-fixture scope is judged once, because
  determinism makes waiting theatre.
- A fixture instance is ONE timeline: consecutive `run` calls continue it (sagas),
  and each reports only what it caused.

```ts
// recorders: thing-first decorators, same shape + a readable log + reset()
recordingEventStore(store): EventStore & { appended; reset() }
recordingCommandBus(bus):   CommandBus & { dispatched; reset() }
recordingQueryBus(bus):     QueryBus   & { queried;    reset() }
controllableSchedulingEventStore<E>(next: E, clock: () => number):
  E & ScheduleCapability & { schedules; due(); resetSchedules() }   // no timer, no sink
  // `due()` HANDS BACK the fired events rather than appending them, so the fixture
  // puts them in through the OUTERMOST store and the recorder sees a fired deadline
  // exactly as it sees a handler's append. `resetSchedules()`, not `reset()`: the
  // recorder above owns that name, and two different resets on one object under one
  // name was a collision waiting to be found at midnight.
```
Unit level needs no fixture at all: folds are reduces over evolve tuples;
handlers are functions called with an inline ctx record.

## Deleted to reach this surface (from the current worktree)
`kronosDbContext` · `distributedCommandBus`/`distributedQueryBus` + connector
interfaces + `SubscriptionRegistry` · `correlatingCommandBus`/`correlatingQueryBus`
+ the metadata/correlation-data seams they needed · variadic interceptor params ·
`amqpConnection` name · the `postgres()` bundle · `SpanFactory`/`MetricsRecorder`
seams + `tracingHandler`/`meteringHandler` in core · `@kronos-ts/opentelemetry`
package · `trackingProcessor`/`subscribingProcessor` builders + `SequencingPolicy`
objects · processors as a kronos list · on-commit delivery + any eventBus ·
`drizzleCommandHandler`×3 (one generic) · the ENTRY level of the handler wrappers
(same names, now taking `h.handler`) and the entry-constraint types they needed
(`DrizzleHandlerEntry`, `WithDrizzleSupplied`, `PostgresHandlerDefinition`, `Supplied`,
`KnexHandlerEntry`, `WithKnexSupplied`, `KyselyHandlerEntry`, `WithKyselySupplied`,
`PrismaHandlerEntry`, `WithPrismaSupplied`, `TypeormHandlerEntry`, `WithTypeormSupplied`,
`OtlpHandlerEntry`) · DLQ constructor table/group params ·
`AnyTagCriteria` · the five-package core split · the `extensions/` directory ·
`uow.correlationData()`/`uow.contributeCorrelationData()` and the correlation map
on the base unit of work · `ctx.contributeCorrelationData` · the processor's
correlation stamping · the IMPLICIT WHOLESALE CARRY (ctx verbs using the handled
message's metadata as their base) ·
the test package's triple-record fixture (`run({ given, when, then })`, `Slice`,
`EventFact`, `CommandAct`, `ErrorExpectation`, `ThenRecord`) and its
zero-argument `recordingEventStore()` ·
`type Unstamped<M>` and `stamped()` — `timestamp` is optional on `Message`
instead, and the stamping is an unexported internal ·
`type Clock` — the arrow IS the contract, so every site writes
`clock?: () => number` inline ·
THE `EventScheduler` SEAM ENTIRELY, with `inMemoryEventScheduler`,
`postgresEventScheduler`, `createKronosDbScheduler` (and its `KronosDbScheduler`
type, `ScheduleOptions`, `ScheduleAlreadyResolvedError`, `ScheduleNotFoundError`),
`controllableScheduler`, and the `eventScheduler` field on every entry,
`HandlerSite`, `HandlerContextDeps`, `CommandInvocationDeps`,
`ProcessorHandlerEntry` and `RunEventProcessorOptions` — scheduling is a
CAPABILITY TIER on the event store now, added by three per-family wrappers, and a
schedule fires into the log the handling that armed it already reads. The
per-context `ContextScheduleFunction`/`ContextScheduleAfterFunction` duplicates
went with it: the verb types live with the tier that contributes them. The
KronosDB caller-supplied idempotency token is gone from the shared contract too —
neither other family can honour one, and a capability is what every member can
promise; the service is still reachable at `connection.scheduler.scheduleAppend`
for a host that wants it. The three cancel outcomes that used to be thrown
KronosDB errors are the same `CancelResult` every other family answers with ·
the CONSTRUCTION-TIME THROW for `deadLetterQueue` without `sequence` as the ONLY
guard — it is a compile error now, and the throw survives as one defensive
assert for JavaScript callers ·
`validatingSerializer` · `SchemaRegistry` · its `.register()` ceremony · and the
three registry factories (`eventSchemaRegistry` / `commandSchemaRegistry` /
`querySchemaRegistry`) — validation moved OFF the serializer and became the
fourth mechanism, `validation/`. The serializer holds a type name and a
revision, which is the only reason it ever needed a registry to look a schema
up; every site that validates now holds the descriptor itself, so the question
the registry answered is not asked any more. `jsonSerializer()` encodes and
that is all it does ·
`upcastTo` — `is()` in `messaging/messages.ts` replaced it, and writing the
match by hand IS the documented idiom ·
THE `SnapshotStore` SEAM ENTIRELY, with `inMemorySnapshotStore`,
`postgresSnapshotStore`, `kronosDbSnapshotStore`, `axonServerSnapshotStore`, the
generic `snapshottingEventStore` decorator, the `snapshotStore` field on every
entry, and the whole `snapshotting/` folder — snapshotting is a CAPABILITY TIER
on the event store now, added by four per-family wrappers, and a snapshot is
`{ state, position }`. `state({ name })` is gone entirely too — a state that
caches its fold declares `snapshot: { key, when }` instead. A cache has a current
entry, so replacing it IS how you invalidate it and there is nothing to delete; a
cache entry carries no cargo, so there is no metadata to carry; and nobody read
the timestamp back, so it is a column an operator sorts by rather than a field
the framework holds. The `unknown` state id is gone with them, and so is the
`{ name, identifier }` PAIR — the seam takes ONE opaque key string, filed as one
column, composed by whoever asked for the read. `state()` composes
`${key}:${snapshotIdentifier(id)}` at the one site that knows the id's shape,
because six implementations inventing six encodings is six ways for an id to miss
its own entry ·
the DERIVED cache key — `snapshotCacheKey`, the id/tag/event/source-text hash and
every reflection caveat that came with it (`Function.prototype.toString`
normalization, bound evolvers, bytecode-only runtimes, closure collisions). A key
is a decision about MEANING, and meaning is not derivable: you write the string,
and you change it when the fold's meaning changes ·
the repository's DIRECT snapshot READ (`snapshots.load(...)` + a computed start
position) — the read is the STORE's now, asked for by a key on the sourcing
condition and served by a wrapped log. The repository still writes: the fold owns
its own cache, through `storeSnapshot` on the log it already holds ·
the `states` list on `kronos`, `StateEntry`, `StateOptions`, `StateManager` /
`stateManager()` / `StateManagerLike` and the `Sited<State…>` forms — a state is
a value `ctx.load` is handed, so nothing about it is registered, and the
repository per (log, state) pair is built lazily in a weak cache instead ·
`state({ initial })` as a FIELD — the initial state is `evolve[0]` now (still taking the
id, as `initial` did), and the snapshot policy moved onto the state value as
`state({ snapshot })` ·
`withRetry` / `healthCheck` / `ResilienceConfig` / `RetryEvent` IN CORE — each
consuming package owns a private copy ·
the whole first upcasting surface: `EventUpcaster` (a `canUpcast`/`upcast`
method pair — a class in disguise), `upcasterChain` (runtime dispatch over a
list, replaced by function composition), `IntermediateEventRepresentation`,
`singleEventUpcaster` and `upcastingSerializer` — upcasting moved from the WIRE
to the LOG, where an upcaster works in the domain form and an in-memory store
gets it too ·
zod as a DEPENDENCY of `@kronos-ts/core` and `@kronos-ts/test`.

## Renamed to reach this surface
`message/` → `messaging/`, and the five declaration files inside it
(`qualified-name` · `metadata` · `message` · `descriptor` · `namespace`) merged
into one `messages.ts` — they were one subject filed as five imports. The
serializer seam moved down into `messaging/serialization/`, and upcasting moved
OUT, up to `upcasting/`, beside the other mechanisms.

`upcastingSerializer` →
`upcastingEventStore` · `State.create` → `State.initial` (element zero of `evolve`
is the INITIAL STATE, the evolver of nothing — but it is still handed the id it
is being folded for, because being the evolver of nothing is not the same as
being the knower of nothing) · `event-sourcing/manager.ts` folded into `repository.ts`, which is
where the fold and its cache both live.

`CommandHandlerDefinition` → `CommandHandler` · `QueryHandlerDefinition` →
`QueryHandler` · `EventHandlerDefinition` → `EventHandler` · `StateModule` →
`State`. What `commandHandler(...)` returns is a command handler; "Definition"
was the word for a shape that had to be registered somewhere, and nothing is
registered any more. The type and value namespaces are separate, so `type
CommandHandler` and the `commandHandler` function coexist by design. Entry
types (`CommandHandlerEntry`, …) keep their names — an entry is a different
thing from the handler it points at.

`AmqpRabbitMqCommandTransport` · `AmqpRabbitMqQueryTransport` ·
`AmqpDistributedSubscriberRegistry` were the last behaviour classes; they are
`amqpRabbitMqCommandTransport(config, channels)`,
`amqpRabbitMqQueryTransport(config, channels)` and
`amqpDistributedSubscriberRegistry(config, channels)` — closures over the same
state, constructed with a call instead of `new`. The two transport names
survive as the TYPES of what those functions return.


## Consumer idiom (karma's — NOT the framework; documented as the reference style)
- `slice({...})`: host constructor normalizing the three arrays; slices are plain
  exported VALUES. Event handler entries carry `processor` as a PARTIALLY APPLIED
  function — the slice closes out its semantics (name, sequence, DLQ-or-not as its
  parameter list), the module calls it full-handed; shorter parameter lists decline
  trailing arguments by TS assignability.
- `module(eventStore)`: creates its db, names its resources as consts, contributes
  them via plain flatMap/map chains. ONE store parameter — a module whose states
  cache their folds takes a WRAPPED log, and the compiler says so.
- Slice FOLDER convention: wire edges live beside domain code, not in it —
  `slice/controller.ts` (oRPC today; pubsub or anything later), `slice/slice.ts`
  (state → handlers, top-down), `slice/index.ts` ties them. Wire stuff stays at
  the edge; the slice never imports a transport.
- The edge stamps per-request metadata on BOTH verbs: send AND query carry actor.
- The composition root names ONE cargo function and wraps every handler with it,
  so `actor` keeps riding once the edge has put it on the first message:
  `correlatingHandler(h.handler, (m) => ({ ...correlationFrom(m), actor: String(m.metadata.actor) }))`.
