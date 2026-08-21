---
"@kronos-ts/core": minor
"@kronos-ts/postgres": minor
"@kronos-ts/kronosdb": minor
"@kronos-ts/drizzle": minor
"@kronos-ts/knex": minor
"@kronos-ts/kysely": minor
"@kronos-ts/prisma": minor
"@kronos-ts/typeorm": minor
"@kronos-ts/test": minor
---

**Three wiring mistakes stop being runtime discoveries. Scheduling joins snapshotting as a capability tier on the log, persistence families refuse to be mixed, and a dead-letter queue without a lane no longer compiles.**

One rule ties them together, and it is now written into SURFACE's rules block: **anything wireable that would die at runtime for a reason the compiler could have stated is a bug in our types, not in the user's config.** Three laws serve it — capabilities live in types (adders return intersections), demands are floors (consumers constrain, never name implementations), and pipes preserve (same-seam wrappers are generic identity, adders are additive; a collapsing wrapper launders capabilities and breaks demands).

---

## 1. Scheduling is a capability tier on the event store

An event that has not happened yet is still an event, and where it lands when its time comes is **the log**.

The old `EventScheduler` seam proved it three times over. Every implementation had to be told which log to fire into, and each said it differently: the in-memory one took an `eventSink`, the postgres one an `eventStore` in its config, and the KronosDB one existed only because the server appends the event itself. Three spellings of *"and this is the log"* is the shape of a capability that belongs **on** the log — so there is no seam beside the store any more, no `eventScheduler` field a host can wire half of, and no `throw new Error("No event scheduler configured")` waiting for the first deadline anybody arms in production.

```ts
// before — two objects, and nothing checked they agreed
const scheduler = postgresEventScheduler(pg, { eventStore, unitOfWork: uow, tagResolver })
kronos({ commandHandlers: handlers.map((h) => ({ ...h, eventStore, eventScheduler: scheduler, … })) })

// after — one object, and the capability rides in its TYPE
const eventStore = postgresSchedulingEventStore(
  postgresEventStore(pg, { tagResolver }), pg, { unitOfWork: uow, tagResolver },
)
kronos({ commandHandlers: handlers.map((h) => ({ ...h, eventStore, … })) })
```

```ts
type ScheduleCapability = {
  schedule(event: EventMessage, at: Date, uow?: UnitOfWork): Promise<ScheduleToken>
  cancelSchedule(token: ScheduleToken, uow?: UnitOfWork): Promise<CancelResult>
}
type ScheduleCapableEventStore = EventStore & ScheduleCapability
```

Three wrappers, one per family that has one, each **additive** — `E` in, `E & ScheduleCapability` out:

```ts
inMemorySchedulingEventStore(next, { clock? }?)                        // Map + setTimeout
postgresSchedulingEventStore(next, pg, { unitOfWork, tagResolver, … }) // table + polling worker
kronosDbSchedulingEventStore(next, kdb, { serializer })                // server-side; no timer here
```

Each also adds what only it can promise, under its own prefixed name so a composed store carries them without collision: `stopScheduling()` in memory, `startScheduling()`/`stopScheduling()` on postgres, `listSchedules()` on KronosDB. None of them is in the capability, because a capability is what *every* member of the tier can honestly answer.

**Axon Server gets no wrapper, and the absence is deliberate.** Its generated protobuf carries a `DcbEventScheduler` service, but the package never built a client for it — no channel, no service definition, nothing in `connection.ts`. There was no scheduler to absorb, and writing one here would be new functionality wearing a refactor's clothes.

**KronosDB is the odd family out, and the tier makes that visible instead of hiding it.** Its schedules already ride the KronosDB log server-side, so the wrapper holds no table, no timer and no poller — and it does *not* join the caller's transaction, because the server owns the schedule the instant it is told. A handling that arms one and then throws has armed it. That was already true of the standalone scheduler; it is now written on the same object as the log, where a reader comparing the three families can see it.

### The ctx verbs are conditionally present

The exact mirror of the snapshotting demand, anchored at one alias:

```ts
IfScheduleCapable<E, Capable, Bare>   // THE anchor, in event-scheduling/schedule.ts
ScheduleVerbs<E> = IfScheduleCapable<E, { schedule; scheduleAfter; cancelSchedule }, unknown>
```

Contexts intersect it, so against a bare log the verbs are **structurally absent** rather than present-and-throwing:

```ts
// ✗
eventHandler(OrderPlaced, async (m, ctx: EventHandlerContext) => {
  await ctx.scheduleAfter(PaymentTimedOut, { orderId }, 900_000)
})

// ✓ — say what you need, and the ENTRY must supply it
eventHandler(OrderPlaced, async (m, ctx: EventHandlerContext<UnitOfWork, ScheduleCapableEventStore>) => {
  await ctx.scheduleAfter(PaymentTimedOut, { orderId }, 900_000)
})
```

The real diagnostic, verbatim:

```
error TS2339: Property 'schedule' does not exist on type 'HandlerContext'.
```

and at the entry, which is what carries the demand out of the slice:

```
error TS2322: Type '{ kind: "command-handler"; … }' is not assignable to type 'CommandHandlerEntry'.
  Types of property 'handler' are incompatible.
    Types of parameters 'context' and 'context' are incompatible.
      Type 'HandlerContext<UnitOfWork, EventStore>' is not assignable to type
      'HandlerContext<UnitOfWork, ScheduleCapableEventStore>'.
        Type 'HandlerContext<UnitOfWork, EventStore>' is missing the following properties
        from type 'ScheduleFunctions': schedule, scheduleAfter, cancelSchedule
```

Snapshotting needed *two* faces because a `state()` can declare that it caches, so there was something to refuse as well as something to offer. Nothing declares that it schedules, so this side of the mirror is one conditional. **Nothing runs**: the demand is erased, and the only trace is one defensive assert whose message names the capability and the wrapper *pattern* — `<family>SchedulingEventStore(store, …)` — never a specific family, because core does not know which one you chose.

### Cancelling answers news, not exceptions

```ts
type CancelResult =
  | { kind: "cancelled" } | { kind: "already-appended" } | { kind: "not-found" }
```

Every family answers in the same three words, so the compensating branch is written once. KronosDB's `ScheduleAlreadyResolvedError` and `ScheduleNotFoundError` are gone: they were two of these outcomes spelled as throws, from a time when that scheduler had its own vocabulary.

### BREAKING

- **`eventScheduler` is removed from every entry** — `HandlerSite`, `CommandHandlerEntry`, `QueryHandlerEntry`, `EventHandlerEntry`, `HandlerContextDeps`, `CommandInvocationDeps`, `ProcessorHandlerEntry`, `RunEventProcessorOptions`. Wrap the entry's `eventStore` instead.
- **The `EventScheduler` seam is dissolved.** `ScheduleCapability` is the shape now, with `cancel` renamed `cancelSchedule` (a bare `cancel` on an event store says nothing about what is being cancelled). `ScheduleToken` and `CancelResult` are unchanged and still public — a token is a value you hold.
- **The standalone scheduler exports are absorbed:** `inMemoryEventScheduler` → `inMemorySchedulingEventStore(next, …)`; `postgresEventScheduler` → `postgresSchedulingEventStore(next, pg, …)` (its `eventStore` config field is gone — the wrapped store *is* the log); `createKronosDbScheduler` → `kronosDbSchedulingEventStore(next, kdb, { serializer })`. `PostgresEventScheduler`/`PostgresEventSchedulerConfig` → `PostgresSchedulingControl`/`PostgresSchedulingConfig`; `KronosDbScheduler`/`ScheduleOptions` → `KronosDbSchedulingControl`/`KronosDbSchedulingOptions`.
- **KronosDB's caller-supplied idempotency token is gone from the shared contract.** Neither other family can honour one, and a capability is what every member can promise. `connection.scheduler.scheduleAppend` is still there for a host that wants it.
- **`ContextScheduleFunction` and `ContextScheduleAfterFunction` are removed** from `command-handling/context.ts` — they duplicated the types in `event-scheduling/schedule.ts`, and the verb types now live with the tier that contributes them.
- **`@kronos-ts/test`: `controllableScheduler(clock)` → `controllableSchedulingEventStore(next, clock)`**, returning `E & ScheduleCapability & { schedules; due(); resetSchedules() }`. `reset()` became `resetSchedules()` because this tier composes *under* `recordingEventStore`, which already owns that name — two different resets on one object under one name was a collision waiting to be found at midnight. `ControllableScheduler` → `ScheduleRecording`.

The fixture composes what a host composes, both tiers included:

```ts
// before
recordingEventStore(inMemorySnapshottingEventStore(inMemoryEventStore()))   // + a separate controllableScheduler

// after
recordingEventStore(
  controllableSchedulingEventStore(inMemorySnapshottingEventStore(inMemoryEventStore()), clock),
)
```

A scope's single `eventStore` parameter is now the log, the fold cache *and* the schedule book. `wait()` still jumps the clock, asks what is due and appends the fired events **in through the outermost store**, so a fired deadline is recorded exactly as a handler's append is; `scheduled()` and `cancelled()` assertions are unchanged. The fixture's "foreign resources" check collapsed from two to one: bringing a foreign scheduler *is* bringing a foreign store.

---

## 2. Persistence families refuse to be mixed

`Never mix families within one processor` was a sentence in a document — which means it was a sentence nobody could read at 2am while wiring a processor out of two packages that both export a `tokenStore`.

**The failure it prevents is not a throw. It is silence.** A drizzle token store handed a postgres unit of work does not fail: it asks for *its* transaction, is told there is none, and falls back to its plain handle. The token update commits **outside** the batch's transaction. Every test passes. Then a crash lands between the projection write and the token write, and a read model is permanently wrong in a way nobody can reconstruct.

Core owns the slot and knows no occupants; each package writes one line:

```ts
// core
type PersistenceFamily<Name extends string, Fix extends string>   // phantom on an ambient unique symbol

// @kronos-ts/drizzle — the only place the family is named
type DrizzleFamily = PersistenceFamily<
  "drizzle", "build this processor's unitOfWork with drizzleUnitOfWork(next, db)"
>
```

```ts
// before
drizzleUnitOfWork<U>(next: () => U, db): () => U
drizzleTokenStore(db): TokenStore
drizzleDeadLetterQueue(db): SequencedDeadLetterQueue

// after
drizzleUnitOfWork<U>(next: () => U, db): () => U & DrizzleFamily
drizzleTokenStore(db): TokenStore<UnitOfWork & DrizzleFamily>
drizzleDeadLetterQueue(db): SequencedDeadLetterQueue<UnitOfWork & DrizzleFamily>
```

**The processor is where the compiler meets the factory and the stores.** `U` is inferred from `unitOfWork` — its one covariant mention — and the stores are checked against that answer:

```ts
eventProcessor({ name, eventStore, tokenStore: drizzleTokenStore(db), unitOfWork: drizzleUow })  // ✓
eventProcessor({ name, eventStore, tokenStore: inMemoryTokenStore(), unitOfWork: drizzleUow })   // ✓ bare demands nothing
eventProcessor({ name, eventStore, tokenStore: drizzleTokenStore(db), unitOfWork: postgresUow }) // ✗
```

The real diagnostic, verbatim — note that the sentence is **drizzle's own**:

```
error TS2322: Type '() => UnitOfWork & PostgresFamily' is not assignable to type
'() => UnitOfWork & DrizzleFamily'.
  Type 'UnitOfWork & PostgresFamily' is not assignable to type 'UnitOfWork & DrizzleFamily'.
    Type 'UnitOfWork & PostgresFamily' is not assignable to type 'DrizzleFamily'.
      The types of '[persistenceFamily].FIX' are incompatible between these types.
        Type '"build this processor's unitOfWork with postgresUnitOfWork(next, pg) — this
        family's stores write through its transaction"' is not assignable to type '"build
        this processor's unitOfWork with drizzleUnitOfWork(next, db) — this family's stores
        write through its transaction"'.
```

and against a bare task:

```
error TS2322: Type '(clock?: (() => number) | undefined) => UnitOfWork' is not assignable to
type '() => UnitOfWork & DrizzleFamily'.
  Type 'UnitOfWork' is not assignable to type 'UnitOfWork & DrizzleFamily'.
    Property '[persistenceFamily]' is missing in type 'UnitOfWork' but required in type 'DrizzleFamily'.
```

That is a general rule for diagnostics, now stated in SURFACE: **an error may only be as specific as the code that owns it is certain about.** Core-owned refusals name the capability and the wrapper *pattern*; a package's own refusals name its own functions. (The landed `SnapshotDemand` FIX string was hardcoding a postgres example inside core; it now reads `wrap this entry's eventStore in the snapshotting wrapper for its persistence family — <family>SnapshottingEventStore(store, …)`, as does the matching runtime throw in `repository.ts`.)

**Erased, and never constructed.** The brand is a phantom on a unique symbol core declares *ambiently* — no JavaScript declares it, none reads it, and each decorator returns exactly what it always returned and asserts the branded type. Emitted output is byte for byte unchanged, and the brand and a composed capability coexist:

```ts
const uow = drizzleUnitOfWork(() => correlating(unitOfWork(clock)), db)
//    ^ () => CorrelatingUnitOfWork & DrizzleFamily — reads both ways round
```

### BREAKING

- **`TokenStore` and `SequencedDeadLetterQueue` are generic in `U`** (defaulting to `UnitOfWork`), and their members are **function-typed fields rather than method shorthand**. That is load-bearing, not stylistic: TypeScript checks method parameters *bivariantly*, so a store demanding a branded task would have been silently assignable to a bare slot and the whole demand would have been decoration. It is also what the surface rules already asked for — a shape is a type alias of function-signature fields.
- **All six persistence packages brand their uow decorator's product** and demand the brand in their token store and dead-letter queue. **Source-compatible for same-family users**: a host already following the rule changes nothing, because the brand only refuses arrangements that were already broken. Bare stores (`inMemoryTokenStore`, `inMemoryDeadLetterQueue`) demand nothing and fit any family — contravariance says so without a special case.

---

## 3. A dead-letter queue without a lane does not compile

Parking is a **lane** operation — the queue holds a failed event and everything behind it in the same lane — so "which lane" stops being optional the moment there is a queue. That was a `throw` at construction: honest, and late, because a composition root runs at boot.

```
error TS2345: Argument of type '{ name: string; eventStore: EventStore; tokenStore: …;
unitOfWork: …; deadLetterQueue: SequencedDeadLetterQueue<UnitOfWork>; }' is not assignable …
  Property 'sequence' is missing in type '{ … }' but required in type '{ readonly sequence: {
  readonly ERROR: "this processor has a deadLetterQueue but no sequence, and parking is a lane
  operation"; readonly FIX: "add `sequence: sequentialPerTag(\"<tagKey>\")`, or drop the queue
  and let failures propagate and retry"; }; }'.
```

The keys **are** the message — which is why the refusal is spelled inline rather than behind a named type: give TypeScript a name and it prints the name. The wording is general, because core is certain about the rule and about nothing else (`sequentialPerTag` is core's own export, so naming it is not core guessing at anybody's stack). The construction-time throw survives as one defensive assert for JavaScript callers, exactly as `repository.ts` keeps its one line.

`eventProcessor` grew a second inferred type parameter to make both demands coexist — the family check reads the task off `EventProcessorSite<U>`, the lane check reads the queue off the literal, and neither steals the other's inference. `EventProcessorSite<U>`, `EventProcessorLane<U>`, `EventProcessorConfig<U>` and `SequenceDemand<C>` are exported.

---

## The probes

Three load-bearing type-probe files join the root `tsconfig.json` `files` array, so every claim above is judged by `tsc --noEmit` and a `@ts-expect-error` that stops erroring turns the gate red:

- `packages/core/src/event-scheduling/__tests__/schedule-demand.types.ts` — the verb quadrants, entry contagion, and **both tiers three deep** in every stacking order with `upcastingEventStore`, including the identity wrapper in the middle.
- `packages/core/src/event-processing/__tests__/processor-demands.types.ts` — all four quadrants of (queue?) × (lane?).
- `integrationtests/src/__tests__/persistence-family.types.ts` — same-family, cross-family, bare-store and bare-task, the correlating composition, and the variance claim the whole mechanism rests on.

`integrationtests/src/__tests__/capability-preservation.types.ts` gains the second tier: with one capability the anti-laundering rule was easy to satisfy by accident, and with two it is not — a wrapper that collapsed to *its own* capability would keep the one it adds and silently drop the other.
