# @kronos-ts/knex

## 0.5.0

### Minor Changes

- 1aef927: Correlation is no longer knowledge the unit of work is born with — it is a capability you compose. Wrap your handlers, and the compiler makes you wrap your unit of work.

  Correlation is the CARRYING MECHANISM: metadata jumping from the message a handler is handling onto every message that handling gives birth to, and from there onto everything those births cause. The correlationId/causationId pair is just the cargo you typically want carried. The new `correlation/` folder is the concept's one address, and it is three functions plus one derived type:

  ```ts
  correlating(uow): CorrelatingUnitOfWork      // a task that carries a map
  correlatingHandler(next, from)               // fills it per invocation, overlays it on every birth
  correlation: Intercept                       // the EDGE intercept, seeding roots (unchanged)
  ```

  `from` is a plain `(message) => Metadata` and it is REQUIRED — never defaulted, and not shipped either. The mechanism has no opinion about what is worth carrying; even the id pair is the host's own two lines, documented rather than exported, because writing them is the whole lesson:

  ```ts
  const correlationFrom = (parent: Message): Metadata => ({
    correlationId: String(parent.metadata.correlationId ?? parent.identifier),
    causationId: String(parent.identifier),
  });
  ```

  More cargo is more function — `correlatingHandler(h.handler, (m) => ({ ...correlationFrom(m), actor: String(m.metadata.actor) }))`.

  **BREAKING — births no longer carry the handled message's metadata.** `ctx.send`, `ctx.query`, `ctx.append` and `ctx.schedule` used to take the handled message's whole metadata as their base, so anything on an incoming command rode forward for free. They no longer do: a birth's metadata is exactly the trailing `metadata?` argument it was given, and nothing else.

  ```ts
  // before — `actor` arrived on the appended event because the base was the command's metadata
  ctx.append(CourseCreated, { courseId });

  // after — compose the cargo, once, at the composition root
  commandHandlers: handlers.map((h) => ({
    ...h,
    handler: correlatingHandler(h.handler, (m) => ({
      ...correlationFrom(m),
      actor: String(m.metadata.actor ?? ""),
    })),
  }));
  ```

  The free carry read as a convenience and behaved as a policy: which of a message's keys are safe to propagate is a host decision, and a primitive that decides it silently propagates a tenant id into a message that crosses a tenant boundary. It is now one function, written down where a reader can find it.

  **BREAKING — a child's `causationId` is always the parent's identifier.** `correlationFrom` reads `causationId: parent.identifier`, unconditionally, never `parent.metadata.causationId ?? parent.identifier`. A child is caused by its parent, not by its grandparent. So an automation's dispatched command is caused by the event it reacted to, not by the command that appended that event. The `correlation` intercept is unchanged — it still `??`-seeds both fields, because it runs on messages born at an edge with no parent to ask. Between them: the edge seeds a root, every hop re-stamps.

  **BREAKING — the adapter unit-of-work decorators reverse their arguments.** `<pkg>UnitOfWork(client, make)` is now `<pkg>UnitOfWork(next, client)` in all six persistence packages — thing-first, like every other decorator on the surface. The thing being decorated is the factory; the client is configuration.

  ```ts
  // before
  const uow = drizzleUnitOfWork(db, unitOfWork);
  const uow = postgresUnitOfWork(pg, unitOfWork);

  // after
  const uow = drizzleUnitOfWork(unitOfWork, db);
  const uow = postgresUnitOfWork(() => correlating(unitOfWork(clock)), pg);
  ```

  They are also capability-preserving: each returns `() => U` for whatever `U` it was handed and decorates the SAME handle rather than rebuilding a record from it, so a composed capability survives the type AND the runtime — the adapter's transaction is keyed on the very object `ctx.unitOfWork` hands back.

  **The unit of work goes pure.** `UnitOfWork` loses `correlationData()` and `contributeCorrelationData()`, and the map with them; the contexts lose `ctx.contributeCorrelationData` (a handler that wants a mid-handling attach reaches `ctx.unitOfWork.attachCorrelationData` on a correlating task); the event processor no longer stamps a correlation rule onto each batch. Core mentions correlation nowhere outside `correlation/`. `requireInvocation` / `requireLive` are now `<U extends UnitOfWork>(uow: U): U` — a guard checks a unit of work, it does not launder one.

  **The demand is conditional, which is the whole point.** Buses, processors, contexts and the `kronos` entry types are now parametric in the unit of work their factory mints — `localCommandBus<U>(unitOfWork: () => U): CommandBus<U>`, threaded to `ctx.unitOfWork` and preserved across every transport. `U` defaults to the bare `UnitOfWork` everywhere, so uncorrelated code reads exactly as it did and never writes a type argument. What the threading buys is that a `correlatingHandler`-wrapped handler does NOT typecheck against a bus or processor built from a bare `() => unitOfWork()`:

  ```ts
  kronos({
    commandHandlers: [
      {
        ...h,
        handler: correlatingHandler(h.handler, correlationFrom),
        commandBus: localCommandBus(unitOfWork), // ← compile error: mints bare units of work
      },
    ],
  });
  ```

  An earlier attempt hardcoded a correlation capability into `ctx` and the bus signatures. It was reverted, and the lesson is in this shape: an unconditional demand propagates contravariantly through every transport, so every bus in the world has to know about correlation. A conditional one propagates exactly as far as somebody asked for it. A new type probe — registered in the root `tsconfig` `files` array beside the drizzle one, so a `@ts-expect-error` that stops erroring turns the build red — pins both directions: the wiring that must compile, and the four that must not.

  **The test fixture composes correlation**, because a fixture is a composition root and composes like a host: its tasks are `() => correlating(unitOfWork(clock))` and every handler the scope hands it is wrapped with `correlatingHandler(handler, correlationFrom)`. Scenario correlation semantics are unchanged; a scope wanting other cargo wraps its own handlers first.

- 1aef927: Core's folders now say what things MEAN, not what they technically are — and the adapter transaction glue, which only ever used the public phase API, has left core to live with the packages that use it.

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
    commandHandlers, // ← command-handling/
    queryHandlers, // ← query-handling/
    eventHandlers, // ← event-processing/
  }); //   event-sourcing/ — no list; `ctx.load` is handed the state
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
  import {
    adapterUnitOfWork,
    transactionRegistry,
  } from "@kronos-ts/core/transaction";

  // after — your own copy, over the public phase API and nothing else
  import { Phase, type UnitOfWork } from "@kronos-ts/core";
  const registry = new WeakMap<UnitOfWork, Slot>();
  uow.on(Phase.COMMIT, () => hooks.commit(tx));
  uow.onError(() => {
    if (!committed) return hooks.rollback(tx);
  });
  ```

  That import list is the argument. The glue never had privileged access to the handle — `uow.on(Phase.COMMIT, …)` and `uow.onError(…)` are the whole of what it touches, and the base `UnitOfWork` has no transaction concept for it to reach into — which makes it a HELPER over public shapes, and by this surface's own first rule helpers are not core. It was shared to stop six copies diverging; what it actually did was force one copy to carry every family's needs at once. Split, each package states its binding honestly: **eager** for drizzle, knex, kysely, prisma and typeorm (a `PRE_INVOCATION` hook forces the transaction open before the action, because their token store and DLQ read through the observing accessor and must not be left writing outside it — so those copies have no lazy mode to get wrong), **lazy** for postgres (claimed at mint, begun only when a writer asks, so its read paths pay no begin/commit and claim no connection — and it alone carries `claimed`, the discrimination lazy binding needs).

  The ordering the shared version pinned is still pinned, now against the copies that have to honour it: the eager tests live in `@kronos-ts/drizzle`, the lazy and `claimed` tests in `@kronos-ts/postgres`. And the proof the eviction was sound is in core's own suite — the correlation test that used to import the glue to show an adapter's transaction stays reachable through a composed handle now writes that adapter inline in six lines, against the public phase API, and still passes.

- 1aef927: **Three wiring mistakes stop being runtime discoveries. Scheduling joins snapshotting as a capability tier on the log, persistence families refuse to be mixed, and a dead-letter queue without a lane no longer compiles.**

  One rule ties them together, and it is now written into SURFACE's rules block: **anything wireable that would die at runtime for a reason the compiler could have stated is a bug in our types, not in the user's config.** Three laws serve it — capabilities live in types (adders return intersections), demands are floors (consumers constrain, never name implementations), and pipes preserve (same-seam wrappers are generic identity, adders are additive; a collapsing wrapper launders capabilities and breaks demands).

  ***

  ## 1. Scheduling is a capability tier on the event store

  An event that has not happened yet is still an event, and where it lands when its time comes is **the log**.

  The old `EventScheduler` seam proved it three times over. Every implementation had to be told which log to fire into, and each said it differently: the in-memory one took an `eventSink`, the postgres one an `eventStore` in its config, and the KronosDB one existed only because the server appends the event itself. Three spellings of _"and this is the log"_ is the shape of a capability that belongs **on** the log — so there is no seam beside the store any more, no `eventScheduler` field a host can wire half of, and no `throw new Error("No event scheduler configured")` waiting for the first deadline anybody arms in production.

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
    schedule(
      event: EventMessage,
      at: Date,
      uow?: UnitOfWork
    ): Promise<ScheduleToken>;
    cancelSchedule(
      token: ScheduleToken,
      uow?: UnitOfWork
    ): Promise<CancelResult>;
  };
  type ScheduleCapableEventStore = EventStore & ScheduleCapability;
  ```

  Three wrappers, one per family that has one, each **additive** — `E` in, `E & ScheduleCapability` out:

  ```ts
  inMemorySchedulingEventStore(next, { clock? }?)                        // Map + setTimeout
  postgresSchedulingEventStore(next, pg, { unitOfWork, tagResolver, … }) // table + polling worker
  kronosDbSchedulingEventStore(next, kdb, { serializer })                // server-side; no timer here
  ```

  Each also adds what only it can promise, under its own prefixed name so a composed store carries them without collision: `stopScheduling()` in memory, `startScheduling()`/`stopScheduling()` on postgres, `listSchedules()` on KronosDB. None of them is in the capability, because a capability is what _every_ member of the tier can honestly answer.

  **Axon Server gets no wrapper, and the absence is deliberate.** Its generated protobuf carries a `DcbEventScheduler` service, but the package never built a client for it — no channel, no service definition, nothing in `connection.ts`. There was no scheduler to absorb, and writing one here would be new functionality wearing a refactor's clothes.

  **KronosDB is the odd family out, and the tier makes that visible instead of hiding it.** Its schedules already ride the KronosDB log server-side, so the wrapper holds no table, no timer and no poller — and it does _not_ join the caller's transaction, because the server owns the schedule the instant it is told. A handling that arms one and then throws has armed it. That was already true of the standalone scheduler; it is now written on the same object as the log, where a reader comparing the three families can see it.

  ### The ctx verbs are conditionally present

  The exact mirror of the snapshotting demand, anchored at one alias:

  ```ts
  IfScheduleCapable<E, Capable, Bare>; // THE anchor, in event-scheduling/schedule.ts
  ScheduleVerbs<E> = IfScheduleCapable<
    E,
    { schedule; scheduleAfter; cancelSchedule },
    unknown
  >;
  ```

  Contexts intersect it, so against a bare log the verbs are **structurally absent** rather than present-and-throwing:

  ```ts
  // ✗
  eventHandler(OrderPlaced, async (m, ctx: EventHandlerContext) => {
    await ctx.scheduleAfter(PaymentTimedOut, { orderId }, 900_000);
  });

  // ✓ — say what you need, and the ENTRY must supply it
  eventHandler(
    OrderPlaced,
    async (
      m,
      ctx: EventHandlerContext<UnitOfWork, ScheduleCapableEventStore>
    ) => {
      await ctx.scheduleAfter(PaymentTimedOut, { orderId }, 900_000);
    }
  );
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

  Snapshotting needed _two_ faces because a `state()` can declare that it caches, so there was something to refuse as well as something to offer. Nothing declares that it schedules, so this side of the mirror is one conditional. **Nothing runs**: the demand is erased, and the only trace is one defensive assert whose message names the capability and the wrapper _pattern_ — `<family>SchedulingEventStore(store, …)` — never a specific family, because core does not know which one you chose.

  ### Cancelling answers news, not exceptions

  ```ts
  type CancelResult =
    | { kind: "cancelled" }
    | { kind: "already-appended" }
    | { kind: "not-found" };
  ```

  Every family answers in the same three words, so the compensating branch is written once. KronosDB's `ScheduleAlreadyResolvedError` and `ScheduleNotFoundError` are gone: they were two of these outcomes spelled as throws, from a time when that scheduler had its own vocabulary.

  ### BREAKING

  - **`eventScheduler` is removed from every entry** — `HandlerSite`, `CommandHandlerEntry`, `QueryHandlerEntry`, `EventHandlerEntry`, `HandlerContextDeps`, `CommandInvocationDeps`, `ProcessorHandlerEntry`, `RunEventProcessorOptions`. Wrap the entry's `eventStore` instead.
  - **The `EventScheduler` seam is dissolved.** `ScheduleCapability` is the shape now, with `cancel` renamed `cancelSchedule` (a bare `cancel` on an event store says nothing about what is being cancelled). `ScheduleToken` and `CancelResult` are unchanged and still public — a token is a value you hold.
  - **The standalone scheduler exports are absorbed:** `inMemoryEventScheduler` → `inMemorySchedulingEventStore(next, …)`; `postgresEventScheduler` → `postgresSchedulingEventStore(next, pg, …)` (its `eventStore` config field is gone — the wrapped store _is_ the log); `createKronosDbScheduler` → `kronosDbSchedulingEventStore(next, kdb, { serializer })`. `PostgresEventScheduler`/`PostgresEventSchedulerConfig` → `PostgresSchedulingControl`/`PostgresSchedulingConfig`; `KronosDbScheduler`/`ScheduleOptions` → `KronosDbSchedulingControl`/`KronosDbSchedulingOptions`.
  - **KronosDB's caller-supplied idempotency token is gone from the shared contract.** Neither other family can honour one, and a capability is what every member can promise. `connection.scheduler.scheduleAppend` is still there for a host that wants it.
  - **`ContextScheduleFunction` and `ContextScheduleAfterFunction` are removed** from `command-handling/context.ts` — they duplicated the types in `event-scheduling/schedule.ts`, and the verb types now live with the tier that contributes them.
  - **`@kronos-ts/test`: `controllableScheduler(clock)` → `controllableSchedulingEventStore(next, clock)`**, returning `E & ScheduleCapability & { schedules; due(); resetSchedules() }`. `reset()` became `resetSchedules()` because this tier composes _under_ `recordingEventStore`, which already owns that name — two different resets on one object under one name was a collision waiting to be found at midnight. `ControllableScheduler` → `ScheduleRecording`.

  The fixture composes what a host composes, both tiers included:

  ```ts
  // before
  recordingEventStore(inMemorySnapshottingEventStore(inMemoryEventStore())); // + a separate controllableScheduler

  // after
  recordingEventStore(
    controllableSchedulingEventStore(
      inMemorySnapshottingEventStore(inMemoryEventStore()),
      clock
    )
  );
  ```

  A scope's single `eventStore` parameter is now the log, the fold cache _and_ the schedule book. `wait()` still jumps the clock, asks what is due and appends the fired events **in through the outermost store**, so a fired deadline is recorded exactly as a handler's append is; `scheduled()` and `cancelled()` assertions are unchanged. The fixture's "foreign resources" check collapsed from two to one: bringing a foreign scheduler _is_ bringing a foreign store.

  ***

  ## 2. Persistence families refuse to be mixed

  `Never mix families within one processor` was a sentence in a document — which means it was a sentence nobody could read at 2am while wiring a processor out of two packages that both export a `tokenStore`.

  **The failure it prevents is not a throw. It is silence.** A drizzle token store handed a postgres unit of work does not fail: it asks for _its_ transaction, is told there is none, and falls back to its plain handle. The token update commits **outside** the batch's transaction. Every test passes. Then a crash lands between the projection write and the token write, and a read model is permanently wrong in a way nobody can reconstruct.

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
  eventProcessor({
    name,
    eventStore,
    tokenStore: drizzleTokenStore(db),
    unitOfWork: drizzleUow,
  }); // ✓
  eventProcessor({
    name,
    eventStore,
    tokenStore: inMemoryTokenStore(),
    unitOfWork: drizzleUow,
  }); // ✓ bare demands nothing
  eventProcessor({
    name,
    eventStore,
    tokenStore: drizzleTokenStore(db),
    unitOfWork: postgresUow,
  }); // ✗
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

  That is a general rule for diagnostics, now stated in SURFACE: **an error may only be as specific as the code that owns it is certain about.** Core-owned refusals name the capability and the wrapper _pattern_; a package's own refusals name its own functions. (The landed `SnapshotDemand` FIX string was hardcoding a postgres example inside core; it now reads `wrap this entry's eventStore in the snapshotting wrapper for its persistence family — <family>SnapshottingEventStore(store, …)`, as does the matching runtime throw in `repository.ts`.)

  **Erased, and never constructed.** The brand is a phantom on a unique symbol core declares _ambiently_ — no JavaScript declares it, none reads it, and each decorator returns exactly what it always returned and asserts the branded type. Emitted output is byte for byte unchanged, and the brand and a composed capability coexist:

  ```ts
  const uow = drizzleUnitOfWork(() => correlating(unitOfWork(clock)), db);
  //    ^ () => CorrelatingUnitOfWork & DrizzleFamily — reads both ways round
  ```

  ### BREAKING

  - **`TokenStore` and `SequencedDeadLetterQueue` are generic in `U`** (defaulting to `UnitOfWork`), and their members are **function-typed fields rather than method shorthand**. That is load-bearing, not stylistic: TypeScript checks method parameters _bivariantly_, so a store demanding a branded task would have been silently assignable to a bare slot and the whole demand would have been decoration. It is also what the surface rules already asked for — a shape is a type alias of function-signature fields.
  - **All six persistence packages brand their uow decorator's product** and demand the brand in their token store and dead-letter queue. **Source-compatible for same-family users**: a host already following the rule changes nothing, because the brand only refuses arrangements that were already broken. Bare stores (`inMemoryTokenStore`, `inMemoryDeadLetterQueue`) demand nothing and fit any family — contravariance says so without a special case.

  ***

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

  ***

  ## The probes

  Three load-bearing type-probe files join the root `tsconfig.json` `files` array, so every claim above is judged by `tsc --noEmit` and a `@ts-expect-error` that stops erroring turns the gate red:

  - `packages/core/src/event-scheduling/__tests__/schedule-demand.types.ts` — the verb quadrants, entry contagion, and **both tiers three deep** in every stacking order with `upcastingEventStore`, including the identity wrapper in the middle.
  - `packages/core/src/event-processing/__tests__/processor-demands.types.ts` — all four quadrants of (queue?) × (lane?).
  - `integrationtests/src/__tests__/persistence-family.types.ts` — same-family, cross-family, bare-store and bare-task, the correlating composition, and the variance claim the whole mechanism rests on.

  `integrationtests/src/__tests__/capability-preservation.types.ts` gains the second tier: with one capability the anti-laundering rule was easy to satisfy by accident, and with two it is not — a wrapper that collapsed to _its own_ capability would keep the one it adds and silently drop the other.

- 1aef927: Types are function signatures. The `interface` keyword is extinct across every package: 581 declarations are now `type` aliases, `extends` is an intersection, and an interface that was nothing but a call signature is a bare arrow (`ContextSendFunction`, `ContextQueryFunction`, `EmitUpdateFunction`). Nothing changed shape — a record of functions is still a record, because that is what shared state looks like — so this is source-compatible for anyone who was not declaration-merging our types, which nothing in the emitted `.d.ts` ever invited.

  The handler definitions lose the word "Definition". It was the name for a shape that had to be registered somewhere, and nothing is registered any more:

  - `CommandHandlerDefinition` → `CommandHandler`
  - `QueryHandlerDefinition` → `QueryHandler`
  - `EventHandlerDefinition` → `EventHandler`
  - `StateModule` → `State`

  Type and value namespaces are separate, so `type CommandHandler` and the `commandHandler` function coexist. The entry types (`CommandHandlerEntry`, `QueryHandlerEntry`, `EventHandlerEntry`) keep their names: an entry is a different thing from the handler it points at.

  The last behaviour classes are closures. `unitOfWork()` no longer instantiates a `ManagedUnitOfWork` — the phase buckets, the status and the correlation map are closed over, `phase`/`closed` are accessors because only the lifecycle advances them, and the execute-once guard still throws synchronously. In `@kronos-ts/rabbitmq` the three AMQP components are constructed with a call instead of `new`:

  - `new AmqpRabbitMqCommandTransport(config, channels)` → `amqpRabbitMqCommandTransport(config, channels)`
  - `new AmqpRabbitMqQueryTransport(config, channels)` → `amqpRabbitMqQueryTransport(config, channels)`
  - `new AmqpDistributedSubscriberRegistry(config, channels)` → `amqpDistributedSubscriberRegistry(config, channels)`

  The two transport names survive as the TYPES those functions return. Classes are Errors only now, everywhere — `instanceof` is the one thing a type alias cannot do, and error discrimination is the only place we need it.

  No types were removed. Every candidate we went looking for is still load-bearing: `AnyTagCriteria` is the `{ kind: "any-tag" }` shape four stores switch on, `EventBus`/`SubscribableEventSource`/`EventStorageEngine` are the halves `EventStore` is declared as, and `TagCriteria`/`TypeRestrictedCriteria`/`EitherCriteria` have live readers (`SchemaRegistry` and `IntermediateEventRepresentation` were still live at this point; both were deleted by later changes in this same release — see the upcasting and validation changesets).

### Patch Changes

- Updated dependencies [1aef927]
- Updated dependencies [1aef927]
- Updated dependencies [1aef927]
- Updated dependencies [1aef927]
- Updated dependencies [1aef927]
- Updated dependencies [1aef927]
- Updated dependencies [1aef927]
- Updated dependencies [1aef927]
- Updated dependencies [1aef927]
- Updated dependencies [1aef927]
- Updated dependencies [1aef927]
  - @kronos-ts/core@0.3.0

## 0.4.0

### Minor Changes

- 4d7b0ee: One core package, and edge verbs instead of gateways.

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
  import { qn } from "@kronos-ts/common";
  import { commandHandler, localCommandBus } from "@kronos-ts/messaging";
  import { inMemoryEventStore } from "@kronos-ts/eventsourcing";
  import { state } from "@kronos-ts/modelling";
  import { kronos } from "@kronos-ts/app";

  // after
  import {
    qn,
    commandHandler,
    localCommandBus,
    inMemoryEventStore,
    state,
    kronos,
  } from "@kronos-ts/core";
  ```

  ## Edge verbs replace the gateways

  A gateway was an object with one method that closed over a bus. The verb is the
  same operation with the bus as its first argument — a function of all its real
  arguments, which a host partially applies itself if it wants the bus fixed.

  ```ts
  // before
  const result = await app.commandGateway.send(
    CreateCourse,
    { courseId },
    metadata
  );
  const view = await app.queryGateway.query(GetCourse, { courseId });

  // after
  const result = await send(commandBus, CreateCourse, { courseId }, metadata);
  const view = await query(queryBus, GetCourse, { courseId });
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
  correlatingCommandBus(bus, lineageProvider, tenancyProvider);

  // after
  interceptingCommandBus(bus, (m) => tenancy(lineage(m)));
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
  processors: [
    {
      ...trackingProcessor("balances")
        .eventHandlers(onDebited, onCredited)
        .build(),
      eventStore,
      tokenStore,
      unitOfWork,
      sequencingPolicy: sequentialPerTag("accountId"),
      deadLetterQueue: dlq,
    },
  ];

  // after
  const balances = eventProcessor({
    name: "balances",
    eventStore,
    tokenStore,
    unitOfWork,
    sequence: sequentialPerTag("accountId"),
    deadLetterQueue: dlq,
  });
  eventHandlers: [
    { ...onDebited, commandBus, queryBus, processor: balances },
    { ...onCredited, commandBus, queryBus, processor: balances },
  ];
  ```

  `Sequence = (event) => string` is TOTAL — "no ordering constraint" is the lane
  `(e) => e.identifier`, not a missing answer — and `eventProcessor` REJECTS a
  `deadLetterQueue` given without a `sequence`, because parking is a lane
  operation. Subscribing (on-commit) delivery is gone; delivery is tracked.

  ## `kronos` takes four lists and nothing else

  ```ts
  kronos({ commandHandlers, queryHandlers, eventHandlers, states });
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
  const dlq = drizzleDeadLetterQueue(db, kronosDeadLetters, "balances");
  await dlq.enqueue(letter, uow);

  // after
  const dlq = drizzleDeadLetterQueue(db); // the table is the adapter's
  await dlq.enqueue("balances", letter, uow);
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

- 4d7b0ee: Handler wrappers move from the ENTRY to the FUNCTION. Every NAME is unchanged
  from the entry era — `postgresHandler`, `drizzleHandler`, `kyselyHandler`,
  `knexHandler`, `prismaHandler`, `typeormHandler`, `otlpHandler`,
  `otlpMetricsHandler` — because a shared-package export has to carry its
  provenance. What changed is the LEVEL: a wrapper is now a plain generic function
  over a plain generic function — `(next, ...config) => (message, ctx) => result`,
  with `<M, C, R>` inferred — and the host does the wrapping by spreading the entry
  itself.

  ```ts
  // before — the wrapper owned the entry, and needed a type to describe one
  kronos({
    commandHandlers: billing
      .map((h) => drizzleHandler(h, db))
      .map((h) => otlpHandler({ ...h, name: "billing" }, exporter)),
  });

  // after — same names; the wrapper owns the handler, the entry is the host's business
  kronos({
    commandHandlers: billing.map((h) => ({
      ...h,
      handler: drizzleHandler(otlpHandler(h.handler, exporter), db),
    })),
  });
  ```

  ```ts
  // before
  export function drizzleHandler<D extends DrizzleHandlerEntry>(
    entry: D,
    db: DrizzleDb
  ): WithDrizzleSupplied<D>;

  // after
  export function drizzleHandler<
    M,
    C extends DrizzleCapability & { readonly unitOfWork: UnitOfWork },
    R
  >(
    next: (message: M, context: C) => R,
    db: DrizzleDb
  ): (message: M, context: Omit<C, "db">) => R;
  ```

  **Nothing is read off the entry any more.** The wrappers used to reach into
  `entry.kind`, `entry.descriptor.name` and the optional `entry.name` label. All
  three now come from the MESSAGE, at call time, because that is where they
  honestly live:

  - `otlpHandler` decides parent-vs-link and SERVER-vs-CONSUMER from
    `message.kind`. No kind argument, no per-kind names, no sentinel.
  - the span name and the metric series key default to the message's qualified
    name; `label?: (message: Message) => string` overrides it. A function OF THE
    MESSAGE — never a per-entry string closed over at wiring time.
  - `kronos.handler.group` (span) and `handler_group` (metrics) are GONE, and
    `message_type` is now the message's own kind (`"command"`, not
    `"command-handler"`). Dashboards keyed on those attributes need updating.

  Because no wrapper depends on an entry, every one of them is pre-appliable —
  config bound once, outside the map, and composed by bare name.

  **DELETED.** The entry-constraint types existed only to describe the argument
  these wrappers no longer take: `DrizzleHandlerEntry`, `WithDrizzleSupplied`,
  `PostgresHandlerDefinition`, `Supplied`, `KnexHandlerEntry`, `WithKnexSupplied`,
  `KyselyHandlerEntry`, `WithKyselySupplied`, `PrismaHandlerEntry`,
  `WithPrismaSupplied`, `TypeormHandlerEntry`, `WithTypeormSupplied`, and
  `OtlpHandlerEntry`. The named context types stay — a slice still writes
  `ctx: DrizzleContext`, which is the whole point.

  **The erasure is directional, and the compiler enforces it.** A wrapper takes a
  handler whose ctx has the capability and returns one whose ctx does not, so
  wrapping twice — or wrapping a handler that never asked — is a compile error:

  ```ts
  const supplied = drizzleHandler(asksForDb, db); // (m, ctx: HandlerContext) => …
  drizzleHandler(supplied, db); // ✗ nothing left to supply
  ```

  Wrappers that supply nothing (`otlpHandler`, `otlpMetricsHandler`) erase nothing
  and compose on either side.
  `packages/drizzle/src/__tests__/drizzle-handler-inference.types.ts` pins both
  directions; it is listed in the root `tsconfig.json` `files` array, so
  `bunx tsc --noEmit` judges it.

- 4d7b0ee: Six persistence packages, one identical seven-function family.

  A processor's token store, its dead-letter queue and its handlers must write
  through the SAME client handle, or the token advances in a transaction the
  events never joined. That makes a persistence package a FAMILY keyed by
  transaction identity — not a bag of adapters — so all six now expose the same
  seven functions for their own client type, and none of them delegates to
  another.

  ```ts
  xUnitOfWork(client, make: () => UnitOfWork): () => UnitOfWork
  xTokenStore(client): TokenStore
  xDeadLetterQueue(client): SequencedDeadLetterQueue
  xTransaction(uow): Promise<Tx>          // opens; REJECTS a foreign uow
  activeXTransaction(uow): Tx | undefined // observes, never opens
  xHandler(handler, client): handler      // wraps the FUNCTION; ctx gains the accessor
  interface XContext extends HandlerContext { … }   // + Event / Query variants
  ```

  **`@kronos-ts/postgres` gains the whole family.** The `postgres()` bundle is
  DELETED and decomposed: `postgresPool(connectionString | adapter)` is the
  resource, with `postgresEventStore(pg, { serializer, tagResolver })` and
  `postgresSnapshotStore(pg)` split out of it. NEW: `postgresTokenStore(pg)`,
  `postgresDeadLetterQueue(pg)`, `postgresHandler(handler, pg)` and
  `PostgresContext { sql(): Sql | Tx }`. They are written against the existing
  `PostgresAdapter`, so they work over `pg`, `postgres.js` and `Bun.sql` alike,
  and they use the same table shapes as the ORM families with the schema DDL
  exported for migrations. You no longer need an ORM to run a durable processor.

  `postgresUnitOfWork(pg, make)` opens its transaction LAZILY — that is postgres's
  honest default, where drizzle's is eager. Neither conjures a delegate: `make` is
  explicit in both.

  **The other five are aligned to the drizzle template.** Deleted along the way:
  the five `TransactionManager` remnants, fifteen
  `xCommandHandler`/`xEventHandler`/`xQueryHandler` triples collapsed into one
  generic wrapper each, every DLQ constructor `tableName`/group parameter (the
  seam carries the processing group per call), and the config-record constructors
  — `drizzleTokenStore({ db, table, claimTimeoutMs })` becomes
  `drizzleTokenStore(db, { claimTimeoutMs? })`, a positional handle with a
  trailing options record for genuine tuning only.

  Never mix families within one processor. That was always true; now the
  signatures say so.

- 4d7b0ee: The unit of work is handed down as a parameter. AsyncLocalStorage is deleted.

  **`UoWRunner` hands the unit of work to the action.** Per-message state used to
  live in an `AsyncLocalStorage` holding a `Map<symbol, unknown>`; every capability
  a handler used reached into it at call time. There is now an explicit
  `UnitOfWork` handle, and it travels as an argument.

  ```ts
  // before — the action took nothing; state was ambient
  export type UoWRunner = <R>(
    metadata: Metadata | undefined,
    action: () => Promise<R>
  ) => Promise<R>;

  // after — the action is given the unit of work
  export type UoWRunner = <R>(
    metadata: Metadata | undefined,
    action: (uow: UnitOfWork) => Promise<R>
  ) => Promise<R>;
  ```

  `UnitOfWork` carries as REAL TYPED FIELDS what the resource-key map held loosely:
  `metadata`, `phase`, `closed`, the lifecycle registrations (`on` /
  `onPrepareCommit` / `onCommit` / `onAfterCommit` / `onError` / `whenComplete`),
  `correlationData()` / `contributeCorrelationData()`, the append buffer and
  sourcing infos (`uow.events`), the per-UoW state cache (`uow.stateCache`),
  `replaying`, and the adapter transaction (`transaction()` /
  `activeTransaction()` / `setTransaction()` / `setTransactionOpener()`).

  The phase model is UNCHANGED — `PRE_INVOCATION → INVOCATION → POST_INVOCATION →
PREPARE_COMMIT → COMMIT → AFTER_COMMIT`, same numeric values, same
  late-registration draining (an action registered while its own phase is running
  still runs in that phase; earlier phases are already past).

  **Deleted.** `processing-state.ts` and the whole resource-key system:
  `resourceKey` / `ResourceKey`, `getResource` / `setResource` /
  `computeIfAbsent` / `removeResource` / `hasResource` / `updateResource`,
  `withOverride`, `processingStateStorage`, `initialProcessingState`,
  `requireInvocationPhase`, and every `*_KEY` (`COMMAND_BUS_KEY`, `QUERY_BUS_KEY`,
  `TRANSACTION_KEY`, `CORRELATION_DATA_KEY`, `BUFFERED_EVENTS_KEY`,
  `SOURCING_INFOS_KEY`, `STATE_MANAGER_KEY`, `STATE_CACHE_KEY`,
  `STATE_MODULES_KEY`, `EVENT_SCHEDULER_KEY`, `REPLAY_STATE_KEY`,
  `EVENT_FLUSH_REGISTERED_KEY`, `MARKER_RESOURCE_KEY`, `TAG_RESOURCE_KEY`). Also
  gone: `getActiveTransaction` / `getOrBeginActiveTransaction`,
  `activeCorrelationData`, the module-level `contributeCorrelationData`, the
  no-arg `isReplay()`, the frozen `HANDLER_CONTEXT` / `EVENT_HANDLER_CONTEXT` /
  `QUERY_HANDLER_CONTEXT` singletons, and the `MinimalConfiguration` config-shim.

  `NoActiveUnitOfWork` and `WrongUoWPhase` REMAIN. `closed` replaces the
  ALS-absence check: a ctx used after its unit of work committed throws
  `NoActiveUnitOfWork`; a mutator called outside INVOCATION throws
  `WrongUoWPhase`.

  **Handler contexts are built fresh per invocation.** They were three frozen
  shared singletons that only worked because every capability re-resolved through
  ALS. Each is now a closure over that invocation's unit of work, the buses the
  caller already holds, and the item's stores.

  ```ts
  // before — one frozen object, every capability an ambient lookup
  export const HANDLER_CONTEXT: HandlerContext = Object.freeze({ load, append, send, … })
  handler(message, HANDLER_CONTEXT)

  // after — a closure over this invocation's unit of work
  handler(message, handlerContext({ uow, stateManager, commandBus, queryBus, eventScheduler }))
  ```

  `handlerContext` / `eventHandlerContext` / `queryHandlerContext` are exported.
  Contexts gain `unitOfWork`, `contributeCorrelationData`, and — on the event and
  command contexts — `isReplay()`, which replaces the deleted module-level
  `isReplay()`.

  **Correlation lineage rides on the message.** `correlatingCommandBus` /
  `correlatingQueryBus` no longer read ambient correlation data; they apply their
  `MetadataProvider`s and nothing else. `ctx.send` / `ctx.query` stamp the unit of
  work's lineage onto the outgoing message BEFORE any bus sees it, so the local
  and the remote branch carry identical metadata. End-to-end lineage behaviour is
  unchanged. `applyCorrelationData(uow, message, providers)` takes the unit of
  work first.

  **`TokenStore` and `SequencedDeadLetterQueue` take the unit of work as a
  trailing parameter**, on every method. It is OPTIONAL, because the lifecycle and
  admin paths (`initializeSegments`, the startup `get`, `resetTokens`, `clear`,
  `sequenceIdentifiers`) legitimately run outside any unit of work — exactly where
  the old permissive `getActiveTransaction()` returned `undefined`.

  ```ts
  // before
  store(processorName: string, segment: number, token: TrackingToken): Promise<void>
  enqueue(letter: DeadLetter): Promise<void>

  // after
  store(processorName: string, segment: number, token: TrackingToken, uow?: UnitOfWork): Promise<void>
  enqueue(letter: DeadLetter, uow?: UnitOfWork): Promise<void>
  ```

  Every implementation follows suit — the in-memory ones plus `drizzle`, `knex`,
  `kysely`, `postgres`, `prisma` and `typeorm`. Their writer helper changes shape
  and nothing else:

  ```ts
  // before
  function getDb() {
    return getActiveTransaction<DrizzleTransaction>() ?? config.db;
  }

  // after
  function getDb(uow?: UnitOfWork) {
    return uow?.activeTransaction<DrizzleTransaction>() ?? config.db;
  }
  ```

  `EventScheduler.schedule/cancel`, `EventSink.publish` and
  `EventStorageEngine.append/appendEvents` gain the same trailing `uow?`, so a
  scheduler insert, an event append and a token write inside one unit of work land
  in one adapter transaction.

  **Bus signatures.** `CommandBus.subscribe` and `QueryBus.subscribe` hand the unit
  of work to the handler: `(message, uow) => Promise<unknown>`. `QueryBus.query`
  takes a trailing `uow?` — passing one NESTS the read in that unit of work, which
  is how `ctx.query` shares the handler's transaction. `QueryBus.emitUpdate` /
  `completeSubscription` / `completeSubscriptionExceptionally` take a trailing
  `uow?` so updates defer to its AFTER_COMMIT. `CommandBus.dispatch` deliberately
  does NOT take one: every command is its own fresh unit of work.

  `runInNewUoW` (always fresh) and `runInUoW` (reuse if given) stay distinct;
  `runInUoW(uow, metadata, action, runner?)` now expresses "is one active" as
  "was one passed in".

### Patch Changes

- 4d7b0ee: The `extensions/` directory is gone, and so is the concept.

  ```
  packages/{core,test,rabbitmq,kronosdb,axon-server,postgres,drizzle,knex,kysely,prisma,typeorm,otlp}
  ```

  An "extension" implied a plugin contract that this framework does not have and
  does not want: every one of these is a package of ordinary functions over the
  public core shapes, no more privileged than something you write yourself. Nested
  under `extensions/` they read as a second tier, which made "should this be core
  or an extension?" a question anybody could ask about anything.

  Published package names are unchanged; only repository paths, the workspace
  globs, the tsconfig include and the CI globs moved.

- Updated dependencies [4d7b0ee]
- Updated dependencies [4d7b0ee]
- Updated dependencies [4d7b0ee]
- Updated dependencies [4d7b0ee]
- Updated dependencies [4d7b0ee]
- Updated dependencies [4d7b0ee]
- Updated dependencies [4d7b0ee]
- Updated dependencies [4d7b0ee]
- Updated dependencies [4d7b0ee]
- Updated dependencies [4d7b0ee]
- Updated dependencies [4d7b0ee]
- Updated dependencies [4d7b0ee]
- Updated dependencies [4d7b0ee]
- Updated dependencies [4d7b0ee]
  - @kronos-ts/core@0.2.0

## 0.3.6

### Patch Changes

- Updated dependencies [2f42ed2]
  - @kronos-ts/messaging@0.11.0

## 0.3.5

### Patch Changes

- @kronos-ts/messaging@0.10.1

## 0.3.4

### Patch Changes

- Updated dependencies [b46a045]
  - @kronos-ts/messaging@0.10.0

## 0.3.3

### Patch Changes

- Updated dependencies [f3f9fbc]
  - @kronos-ts/common@0.1.2
  - @kronos-ts/messaging@0.9.2

## 0.3.2

### Patch Changes

- Updated dependencies [ad944b9]
  - @kronos-ts/messaging@0.9.1

## 0.3.1

### Patch Changes

- 9eb84ff: Carry the commit-order key in durable tracking tokens so gap-free tailing resumes correctly.

  The postgres engine tails events in `(transaction_id, sequence_position)` order with a `pg_snapshot_xmin` watermark, but durable tokens stored only `sequence_position`. On stream reopen the catch-up filter compared positions alone, so an event with a lower `sequence_position` but higher `transaction_id` — which happens when a transaction writes other rows (stamping its xid) before appending its event — was permanently skipped.

  - `messaging`: adds `gapAwareToken(sequence, gapKey)` (a `TrackingToken` carrying an opaque commit-order key alongside the position), `advanceTokenTo`, and `serializeToken`/`deserializeToken`. `SequencedEvent` and `StreamingCondition` gain an optional `token`, letting an engine hand the processor its own resume cursor instead of a bare position. Both processors persist the engine-supplied token when present.
  - `postgres`: `open()` emits a gap-aware token per event and, on reopen, resumes the `(transaction_id, sequence_position)` tuple cursor from it. Engines that supply no token (in-memory, Axon Server) are unaffected.
  - token stores (`knex`, `kysely`, `drizzle`, `prisma`, `typeorm`): serialize through the shared `messaging` helpers so the commit-order key round-trips instead of being flattened to a position.

  Token format change: tokens written before this release carry no commit-order key. They rehydrate as position-only tokens and resume via the legacy catch-up branch on first reopen, then mint gap-aware tokens going forward; to close the window immediately, reset the affected processors.

- Updated dependencies [9eb84ff]
  - @kronos-ts/messaging@0.9.0

## 0.3.0

### Minor Changes

- 56bfb6d: Move per-transaction safety timeouts onto the database adapter.

  - `pgAdapter`, `postgresAdapter`, and `bunSqlAdapter` now accept `idleInTransactionTimeoutMs` (default 30000) and `statementTimeoutMs` (default 0) and arm them via `SET LOCAL` on every transaction they open — UoW-scoped commits, event-store own-tx appends, and the scheduler worker tick alike. Each adapter instance is configured independently, so two adapters pointed at two databases stay decoupled.
  - `postgresTransactionManager` no longer takes timeout options and no longer issues `SET LOCAL`; it is now a pure begin/commit/rollback bridge. The `postgres({ transaction: { ... } })` config is removed — set the timeouts on the adapter instead.
  - `drizzleTransactionManager` and `knexTransactionManager` accept an `onBeginTransaction(tx)` hook that runs once per transaction, before the UnitOfWork uses it — the seam for arming session settings (e.g. `SET LOCAL idle_in_transaction_session_timeout`) on those clients so a stalled drain is bounded.

### Patch Changes

- Updated dependencies [56bfb6d]
- Updated dependencies [56bfb6d]
  - @kronos-ts/messaging@0.8.0

## 0.2.4

### Patch Changes

- Updated dependencies [dafdf12]
  - @kronos-ts/messaging@0.7.0

## 0.2.3

### Patch Changes

- Updated dependencies [291acd2]
- Updated dependencies [291acd2]
  - @kronos-ts/messaging@0.6.0

## 0.2.2

### Patch Changes

- @kronos-ts/messaging@0.5.1

## 0.2.1

### Patch Changes

- Updated dependencies [6a3dca4]
  - @kronos-ts/messaging@0.5.0

## 0.2.0

### Minor Changes

- dc0f67e: Add a dead-letter queue (DLQ) for streaming and tracking event processors.

  When a DLQ is configured, a failing event is parked instead of redelivering its batch: the batch commits, the token advances past it, and later events in the same sequence are blocked to preserve ordering. The enqueue runs in the same UnitOfWork transaction as the token update.

  - `SequencingPolicy` (`sequentialPerTag`, `defaultSequencingPolicy`, `fullConcurrencyPolicy`) chooses an event's ordered sequence.
  - Enqueue policies + a `Decisions` factory, including `retryThenEvictPolicy` (caps retries via diagnostics, then evicts).
  - Reprocessing: `reprocessDeadLetters()` replays parked sequences through the same handlers; optional scheduled drain via `dlqRetryInterval`.
  - `DeadLetterListener` observability hook (no-op / logging / multi) and an OpenTelemetry listener.
  - A full queue applies backpressure (`DeadLetterQueueOverflowError`); `resetTokens()` clears the DLQ when `resetClearsDeadLetters` is set.
  - Persistent backends: `drizzleDeadLetterQueue`, `kyselyDeadLetterQueue`, `knexDeadLetterQueue`, `typeormDeadLetterQueue`, `prismaDeadLetterQueue`, each enqueueing inside the active transaction.
  - Builder methods: `.deadLetterQueue()`, `.enqueuePolicy()`, `.sequencingPolicy()`, `.deadLetterListener()`, `.resetClearsDeadLetters()`, `.dlqRetryInterval()`.

  The event-processor `batchSize` default changes from 100 to 1, keeping per-entity `load()` decisions isolated to their own UnitOfWork. Raise it for read-model projections that only apply idempotent view updates.

### Patch Changes

- Updated dependencies [dc0f67e]
  - @kronos-ts/messaging@0.4.0

## 0.1.4

### Patch Changes

- Updated dependencies [4b8faa5]
  - @kronos-ts/messaging@0.3.1

## 0.1.3

### Patch Changes

- Updated dependencies [74dc43d]
  - @kronos-ts/messaging@0.3.0

## 0.1.2

### Patch Changes

- Updated dependencies
- Updated dependencies
  - @kronos-ts/messaging@0.2.0

## 0.1.1

### Patch Changes

- Publish via `bun publish` so `workspace:*` resolves to concrete versions in published manifests. Previously `changeset publish` shelled out to `npm publish`, which does not understand Bun's workspace protocol, leaving literal `"workspace:*"` strings in published manifests and breaking installs for consumers.
- Updated dependencies
  - @kronos-ts/common@0.1.1
  - @kronos-ts/messaging@0.1.1
