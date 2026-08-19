# @kronos-ts/test

## 0.3.0

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
  import { commandHandler, simpleCommandBus } from "@kronos-ts/messaging";
  import { inMemoryEventStore } from "@kronos-ts/eventsourcing";
  import { state } from "@kronos-ts/modelling";
  import { kronos } from "@kronos-ts/app";

  // after
  import {
    qn,
    commandHandler,
    simpleCommandBus,
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

- 4d7b0ee: kronos is two buses and three plainly-typed lists; enhancement is a
  user-composed function.

  **`kronos({ commandBus, queryBus, commandHandlers, queryHandlers, states,
processors })` is the entire options surface.** `module()`, `AppModule`, `ModuleStores`, the `modules` array,
  `unitOfWorkFactory` and `enhance` are all DELETED. The word "module" is gone
  from kronos vocabulary.

  ```ts
  // before — a module wrapper whose only job was to hold stores
  kronos({
    commandBus,
    queryBus,
    unitOfWorkFactory: uow,
    modules: [module("billing", { eventStore }, ...billingSlice)],
  });

  // after — four named lists, persistence rides on the items that need it
  kronos({
    commandBus: correlatingCommandBus(simpleCommandBus(uow)),
    queryBus: correlatingQueryBus(simpleQueryBus(uow)),
    states: [{ ...Bill, eventStore }],
    commandHandlers: [{ ...openBill, eventStore }],
    queryHandlers: [{ ...getBill, eventStore }],
    processors: [{ ...billProjection, eventStore, tokenStore }],
  });
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
  kronos({ enhance: (h, i) => tracing(metering(h, i), i), modules }); // gone

  // after — a map, at the call site, over the handlers you choose
  const handlers = billing
    .map(meteringHandler(recorder))
    .map(tracingHandler(spanFactory));
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

- 4d7b0ee: kronos() flattened; interceptors and correlation are wrapper functions.

  **`kronos()` takes four named things, not a record.** `Components`,
  `inMemoryComponents` and `resolveComponents` are DELETED. Stores are module-only:
  an event store is the system of record for a bounded slice, and an app-level one
  only ever meant "whichever module forgot to say".

  ```ts
  // before — a record, with defaults filled in behind you
  kronos({
    components: inMemoryComponents({ eventStore }),
    modules: [module("billing", ...slices)],
  });

  // after — the buses are yours to build and wrap
  kronos({
    commandBus: correlatingCommandBus(simpleCommandBus(unitOfWork)),
    queryBus: correlatingQueryBus(simpleQueryBus(unitOfWork)),
    modules: [module("billing", { eventStore }, ...slices)],
  });
  ```

  `module(name, stores, ...items)` required a stores record naming an
  `eventStore`. (Both the module wrapper and the record are deleted outright by
  the flat-fields change below; what survives is that persistence is attached by
  the host, per item, at composition.) Per-state `[state, options]` tuples are
  unchanged.

  The UoW-capture trap doc moved onto `simpleCommandBus`, where the capture
  actually happens: the runner you hand the bus and the one you hand `kronos` have
  to be the same value, and writing them on adjacent lines is what makes that
  checkable.

  **Interceptors are wrapper functions.** `interceptingCommandBus`,
  `interceptingQueryBus`, `interceptingEventBus`, `interceptingEventStore`,
  `DispatchInterceptor`, `HandlerInterceptor` and both
  `registerDispatchInterceptor` / `registerHandlerInterceptor` are DELETED.

  ```ts
  // before — a registry, and two far-apart call sites fighting over order
  const bus = interceptingCommandBus(routing);
  bus.registerDispatchInterceptor(correlationDataDispatchInterceptor());

  // after — order is argument order
  const bus = correlatingCommandBus(routing, (m) =>
    metadataAnd(m, "tenantId", tenant)
  );
  ```

  **Providers are functions, and there are two kinds.** A `MetadataProvider` is a
  plain `(metadata: Metadata) => Metadata` handed to `correlatingCommandBus` /
  `correlatingQueryBus` — it runs per DISPATCH and sees only metadata. A
  `CorrelationDataProvider` is `(message: Message) => Record<string, string>`,
  runs ONCE per handler INVOCATION, and can see the incoming message's identifier
  — which is what `causationId` is. Neither has a factory any more:
  `messageOriginProvider`, `simpleCorrelationDataProvider`, `messageOrigin()`,
  `copyMetadataKeys()` and `defaultCorrelationDataProviders` are all DELETED. A
  host writes the rule, because the rule is two lines and reads better than a name
  for it:

  ```ts
  // before — a framework factory, and a registry to hand it to
  app.correlationDataProvider(messageOriginProvider());

  // after — a function the host names, passed where the invocation is built
  const lineage: CorrelationDataProvider = (m) => ({
    correlationId: String(m.metadata.correlationId ?? m.identifier),
    causationId: m.identifier,
  });
  trackingEventProcessor({
    ...opts,
    correlationDataProviders: [lineage, actor],
  });
  ```

  `correlationDataProviders` is gone from `kronos()`. It lives on the CONSTRUCTION
  SITE that builds the invocation wrapper — `subscribeCommandHandlers`,
  `subscribeQueryHandlers`, and all three event processors — defaulting to lineage
  alone. Note the asymmetry that makes the seam necessary: a command handler's
  unit of work is opened from `message.metadata`, so the whole inbound metadata map
  already rides forward through `ctx.send` / `ctx.append` with no provider at all;
  a processor's batch unit of work is opened from `emptyMetadata()`, so a host key
  crosses the automation boundary only if a provider extracted it.

  **One optional enhancer function.** `kronos({ enhance })` replaces
  `handlerEnhancer`. `multiHandlerEnhancerDefinition` is DELETED — two enhancers
  compose the way two functions compose, and writing it out says the nesting order
  that an array argument only implied.

  ```ts
  // before
  handlerEnhancer: multiHandlerEnhancerDefinition([tracing, metering]);

  // after
  enhance: (handler, info) => tracing(metering(handler, info), info);
  ```

  `tracingHandlerEnhancerDefinition` → `tracingEnhancer`,
  `meteringHandlerEnhancerDefinition` → `meteringEnhancer`.

- 4d7b0ee: A test is a VALUE, and the fixture is the SITE it runs at.

  The triple-record fixture said the right thing and typed the wrong one: a record
  has no order, so it could not say whether a wait came before or after the act; a
  `[descriptor, payload]` tuple could not carry metadata or a hole; and `then` had
  two shapes (a list, or a record of three fields) with a rule about which one you
  were allowed to use. The vocabulary is now value constructors, and the grammar is
  a pipe — because order is real at the joints and nowhere else.

  ```ts
  // before
  await testFixture(courseSlice).run({
    given: [
      [CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 1 }],
    ],
    when: [SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" }],
    then: { error: "Course is full", events: [] },
  });

  // after
  await testFixture(courses).run(
    given(
      event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 1 })
    )
      .when(
        command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" })
      )
      .then(error("Course is full"), noEvents())
  );
  ```

  ## The vocabulary

  `event(descriptor, payload, metadata?)`, `command(...)` and `query(...)` name a
  message. The same three name an EXPECTATION, because "the event
  StudentSubscribed with this payload" is one idea whether you are stating it as
  history or claiming it as a consequence. Payloads are compile-checked against
  their descriptor; `any(schema?)` is the positional hole for a field that is not
  the test's business, and it renders as `*` in a diff.

  The rest of `then` is `result(value)`, `error(matcher)`, `noEvents()`,
  `noCommands()`, `scheduled(event(...), duration)` and `cancelled(event(...))`.
  Each CATEGORY is judged only if the scenario mentions it. `error` takes a string
  (substring), a RegExp, or a predicate — `(e) => e instanceof CourseFull` replaces
  the error-class special case, and `error(() => true)` replaces `error: true`.

  ## The grammar

  ```ts
  given(...events) // or scenario(); given() empty is the same empty world
    .wait(duration) // chainable at any joint, repeatable
    .when(action) // EXACTLY ONE command | query | event — the type has no second `when`
    .wait(duration)
    .then(...assertions); // terminal → a Scenario value
  ```

  Every step returns a NEW value, so one prefix finishes several ways and a
  finished `Scenario` — which carries a derived `description` like
  `"given CourseCreated, when SubscribeStudent, then StudentSubscribed"` — runs
  against as many fixtures as you like. Running one scenario against two scopes is
  now a two-line test.

  ## The site

  ```ts
  // before — the fixture took the lists and REPLACED whatever site they carried
  testFixture(courseSlice);

  // after — the fixture creates the resources and the scope is a function of them
  testFixture((eventStore, snapshotStore) =>
    courses(eventStore, snapshotStore)
  );
  testFixture(courses); // the same function a process deploys
  ```

  The fixture owns a recording event store, a snapshot store, a token store, a
  dead-letter queue, both recording buses, one clock and a `controllableScheduler`
  sharing it. It calls the scope full-handed (a shorter parameter list declines the
  rest) and completes any PARTIAL processor an event-handler entry carries —
  `(eventStore, tokenStore, unitOfWork, deadLetterQueue) => EventProcessor` — which
  is the slice idiom typed: the slice closes out its own semantics, the site
  supplies the resources.

  Semantics, pinned:

  - `given` events APPEND to the log and every cursor FAST-FORWARDS past them
    without invoking a handler. History is history; the automations that would have
    fired already fired, long ago. The old fixture let them replay, which made the
    world one the test never described.
  - `when(event)` means the event ARRIVES — appended, and the processors DO react.
    That is the automation shape, and it was unsayable before.
  - `wait(d)` jumps the clock, fires the schedules now due in FIRE-TIME order, and
    quiesces. A fired event is stamped with the instant it FIRES, not the instant it
    was arranged. Against a scope whose resources the fixture does not own, `wait`
    throws a clear error unless `{ realTime: true }`, where it genuinely elapses.
  - `event()`/`noEvents()` is the EXACT ORDERED list of new events; `command()`/
    `noCommands()` the same over dispatches, with the act's own command excluded.
    `metadata` on a then-value is a subset claim over the keys it names.
  - Real-infrastructure scopes are re-judged until `opts.within` (default 5s). An
    all-in-memory scope is judged ONCE — it is deterministic, so waiting for it
    would only make failures slow.
  - Failure is a `ScenarioAssertionError` whose message IS the diff: the scenario's
    own sentence, both lists in full, names aligned by longest common subsequence,
    and a field-level payload diff on the pairs that lined up.

  ## Recorders

  Thing-first decorators now — each takes what it records and returns the same
  shape plus a readable log and a `reset()`:

  ```ts
  // before
  recordingEventStore(): EventStore & { appended }        // was itself a store

  // after
  recordingEventStore(store): EventStore & { appended; reset() }
  recordingCommandBus(bus):   CommandBus & { dispatched; reset() }
  recordingQueryBus(bus):     QueryBus   & { queried;    reset() }
  controllableScheduler(clock): EventScheduler & { schedules; due(); reset() }
  ```

  `controllableScheduler` has no timer and no sink: `due()` hands back the events
  whose fire-time has arrived, and whoever moved the clock decides where they go.
  That is the difference between a deadline test that takes thirty seconds and one
  that takes a millisecond.

  Unit level still needs no fixture at all: folds are reduces over `evolve` tuples,
  handlers are functions called with an inline ctx record.

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

## 0.2.3

### Patch Changes

- Updated dependencies [2f42ed2]
  - @kronos-ts/messaging@0.11.0
  - @kronos-ts/app@0.6.2
  - @kronos-ts/eventsourcing@0.4.2
  - @kronos-ts/modelling@0.3.2

## 0.2.2

### Patch Changes

- 16c32e3: EventPair is readonly (and exported): `as const` event tuples in specs now
  satisfy `.events(...)` / `.expectEvents(...)` instead of failing TS4104.

## 0.2.1

### Patch Changes

- Updated dependencies [9ad1a3c]
  - @kronos-ts/eventsourcing@0.4.1
  - @kronos-ts/app@0.6.1
  - @kronos-ts/messaging@0.10.1
  - @kronos-ts/modelling@0.3.1

## 0.2.0

### Minor Changes

- b46a045: Functional composition — the container is gone.

  BREAKING. The app builder, slot registry, decorator pipeline, lifecycle
  stages, extensions-as-mutators and `defineModule` are removed. The entry
  point is `kronos({ components, modules })`; a module is
  `module(name, overrides?, ...registrations)` with per-state snapshot
  options as `[state, options]` tuples; dependencies are plain function
  arguments.

  Handlers receive their capabilities as a typed context argument
  (`load`/`append`/`send`/`emitUpdate`/`schedule`/`transaction`); the
  module-level `append`/`load`/`send`/`emitUpdate` helpers are no longer
  exported. Query handlers get a read-only context (`load`/`transaction`).
  `append` accepts a batch of `[descriptor, payload]` tuples. `on()` is
  evolver/query only; `onEvent()` is removed.

  Every backend extension is an async factory returning
  `{ components, start(), close() }` with a uniform no-arg `start()`.
  Remote processor administration moved to opt-in control planes
  (`kronosDbControlPlane`, `axonServerControlPlane`) fed by
  `app.processors`. Correlation lineage now survives distributed dispatch
  (interception sits above the local/remote fork) and is seeded from
  incoming messages in the invocation wrappers. Axon reconnect detection is
  armed by the data path, independent of the control plane.

  All 59 `create*`-prefixed factories are renamed to what they return
  (`inMemoryEventStore`, `simpleCommandBus`, `postgresEventStore`, …).
  Drizzle stores no longer take the ORM operator bundle — only
  `{ db, table }`.

### Patch Changes

- Updated dependencies [b46a045]
  - @kronos-ts/app@0.6.0
  - @kronos-ts/messaging@0.10.0
  - @kronos-ts/eventsourcing@0.4.0
  - @kronos-ts/modelling@0.3.0

## 0.1.13

### Patch Changes

- Updated dependencies [f3f9fbc]
  - @kronos-ts/common@0.1.2
  - @kronos-ts/messaging@0.9.2
  - @kronos-ts/modelling@0.2.10
  - @kronos-ts/eventsourcing@0.3.3
  - @kronos-ts/app@0.5.3

## 0.1.12

### Patch Changes

- Updated dependencies [ad944b9]
  - @kronos-ts/messaging@0.9.1
  - @kronos-ts/app@0.5.2
  - @kronos-ts/eventsourcing@0.3.2
  - @kronos-ts/modelling@0.2.9

## 0.1.11

### Patch Changes

- Updated dependencies [9eb84ff]
  - @kronos-ts/messaging@0.9.0
  - @kronos-ts/app@0.5.1
  - @kronos-ts/eventsourcing@0.3.1
  - @kronos-ts/modelling@0.2.8

## 0.1.10

### Patch Changes

- Updated dependencies [56bfb6d]
- Updated dependencies [56bfb6d]
- Updated dependencies [56bfb6d]
  - @kronos-ts/messaging@0.8.0
  - @kronos-ts/app@0.5.0
  - @kronos-ts/eventsourcing@0.3.0
  - @kronos-ts/modelling@0.2.7

## 0.1.9

### Patch Changes

- Updated dependencies [dafdf12]
  - @kronos-ts/messaging@0.7.0
  - @kronos-ts/app@0.4.1
  - @kronos-ts/eventsourcing@0.2.3
  - @kronos-ts/modelling@0.2.6

## 0.1.8

### Patch Changes

- Updated dependencies [291acd2]
- Updated dependencies [291acd2]
  - @kronos-ts/messaging@0.6.0
  - @kronos-ts/app@0.4.0
  - @kronos-ts/eventsourcing@0.2.2
  - @kronos-ts/modelling@0.2.5

## 0.1.7

### Patch Changes

- Updated dependencies [4ac26c0]
  - @kronos-ts/eventsourcing@0.2.1
  - @kronos-ts/app@0.3.4
  - @kronos-ts/messaging@0.5.1
  - @kronos-ts/modelling@0.2.4

## 0.1.6

### Patch Changes

- Updated dependencies [6a3dca4]
  - @kronos-ts/eventsourcing@0.2.0
  - @kronos-ts/messaging@0.5.0
  - @kronos-ts/app@0.3.3
  - @kronos-ts/modelling@0.2.3

## 0.1.5

### Patch Changes

- Updated dependencies [dc0f67e]
- Updated dependencies [f5ed7da]
  - @kronos-ts/messaging@0.4.0
  - @kronos-ts/app@0.3.2
  - @kronos-ts/eventsourcing@0.1.5
  - @kronos-ts/modelling@0.2.2

## 0.1.4

### Patch Changes

- Updated dependencies [4b8faa5]
  - @kronos-ts/messaging@0.3.1
  - @kronos-ts/app@0.3.1
  - @kronos-ts/eventsourcing@0.1.4
  - @kronos-ts/modelling@0.2.1

## 0.1.3

### Patch Changes

- Updated dependencies [c1a1cf5]
- Updated dependencies [74dc43d]
  - @kronos-ts/modelling@0.2.0
  - @kronos-ts/app@0.3.0
  - @kronos-ts/messaging@0.3.0
  - @kronos-ts/eventsourcing@0.1.3

## 0.1.2

### Patch Changes

- Updated dependencies
- Updated dependencies
  - @kronos-ts/messaging@0.2.0
  - @kronos-ts/app@0.2.0
  - @kronos-ts/eventsourcing@0.1.2
  - @kronos-ts/modelling@0.1.2

## 0.1.1

### Patch Changes

- Publish via `bun publish` so `workspace:*` resolves to concrete versions in published manifests. Previously `changeset publish` shelled out to `npm publish`, which does not understand Bun's workspace protocol, leaving literal `"workspace:*"` strings in published manifests and breaking installs for consumers.
- Updated dependencies
  - @kronos-ts/common@0.1.1
  - @kronos-ts/messaging@0.1.1
  - @kronos-ts/modelling@0.1.1
  - @kronos-ts/eventsourcing@0.1.1
  - @kronos-ts/app@0.1.1
