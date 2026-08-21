# @kronos-ts/core

## 0.2.0

### Minor Changes

- 4d7b0ee: One clock per task: `Clock` enters at `unitOfWork`, and every message a task
  gives birth to stamps its instant from there.

  `Date.now()` was called at eight different message-birth sites, so nothing agreed
  about "now" and nothing could be frozen. The instant is a fact about the TASK —
  so it enters where the task does, and `uow.now()` is the one place that answers.

  ```ts
  // before
  export function unitOfWork(): UnitOfWork
  // … and, scattered across the birth sites:
  timestamp: Date.now()

  // after
  type Clock = () => number                      // epoch ms — an instant
  export function unitOfWork(clock?: Clock): UnitOfWork   // absent = system time
  interface UnitOfWork { …; now(): number }
  timestamp: uow.now()
  ```

  `ctx.append`, `ctx.send`, `ctx.query` and `ctx.schedule` all stamp from
  `uow.now()`, and `ctx.scheduleAfter` measures its delay from it — so a frozen
  clock gives a fire-time a test can name.

  ## Edge dispatch settles the instant

  The `send` / `query` / `subscriptionQuery` verbs build the message with no
  timestamp at all, because the instant belongs to a task that does not exist yet.
  The bus mints the unit of work, then stamps:

  ```ts
  // before — the verb guessed, and the handling task disagreed
  send(bus, CreateCourse, payload)   //  timestamp: Date.now()

  // after — the verb builds, the bus that mints the task stamps
  type Unstamped<M extends Message> = Omit<M, "timestamp"> & { timestamp?: number }
  stamped(message, clock)            //  idempotent; already-stamped passes through
  CommandBus.dispatch(m: Unstamped<CommandMessage>)
  QueryBus.query(m: Unstamped<QueryMessage>, uow?)
  ```

  A nested `ctx.query` is stamped by the task it JOINS, so a consulting read and
  the decision that provoked it share one instant. A transport has no task, so it
  stamps from system time at the wire — and hands a locally-shortcut message on
  unstamped, letting the local task supply the instant instead.

  `inMemoryEventScheduler`'s `now?` option is renamed `clock?: Clock`.

  ## `eventScheduler` rides on the entry

  `kronos` never wired a scheduler, so `ctx.schedule` threw in every assembled app.
  It is now a `HandlerSite` field, attached per entry exactly as the buses are —
  which scheduler an automation arms is a deployment fact.

  ```ts
  kronos({
    commandHandlers: billing.map((h) => ({
      ...h,
      eventStore,
      commandBus,
      queryBus,
      eventScheduler,
    })),
  });
  ```

  Still no default: a scheduler is durable infrastructure with a worker behind it,
  and there is nothing honest to conjure.

  ## Left on `Date.now()`, deliberately

  `inMemoryTokenStore`'s claim timestamps, `inMemoryDeadLetterQueue`'s
  `enqueuedAt` / `lastTouched`, and the snapshot record `eventSourcedRepository`
  writes. None of them is a message, none of them is read by a handler, and the
  repository has no unit of work in hand — a lease that expires on a frozen clock
  would be a lease that never expires.

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

- 4d7b0ee: Explicit components, and plain functions for `TagResolver` / `HandlerEnhancer`.

  **`kronos({ components })` is now required and must be complete.** There are no
  implicit defaults: what you pass is what runs. `inMemoryComponents(overrides)` is
  the explicit opt-in to in-memory fallbacks, and its `overrides` argument is the
  ordering-safe position — it resolves AFTER the merge, so a supplied
  `unitOfWorkFactory` is the one the command bus captures.

  ```ts
  // before — kronos silently filled the gaps
  kronos({ modules });
  kronos({ components: { eventStore, snapshotStore }, modules });

  // after — the defaults are something you ask for
  kronos({ components: inMemoryComponents(), modules });
  kronos({
    components: inMemoryComponents({ eventStore, snapshotStore }),
    modules,
  });
  ```

  **`TagResolver` is a bare function type.** `descriptorBasedTagResolver`,
  `metadataBasedTagResolver` and `multiTagResolver` return plain functions.

  ```ts
  // before
  interface TagResolver {
    resolve(event: EventMessage): Tag[];
  }
  const tags = tagResolver.resolve(event);

  // after
  type TagResolver = (event: EventMessage) => Tag[];
  const tags = tagResolver(event);
  ```

  **`HandlerEnhancerDefinition` is now `HandlerEnhancer`, a bare function type.**
  `multiHandlerEnhancerDefinition`, `tracingHandlerEnhancerDefinition`,
  `meteringHandlerEnhancerDefinition` and `openTelemetry().handlerEnhancer` all
  return the function directly rather than a `{ wrapHandler }` wrapper.

  ```ts
  // before
  const enhancer: HandlerEnhancerDefinition = { wrapHandler(handler, metadata) { … } }
  const wrapped = enhancer.wrapHandler(handler, metadata)

  // after
  const enhancer: HandlerEnhancer = (handler, metadata) => { … }
  const wrapped = enhancer(handler, metadata)
  ```

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
    commandBus: correlatingCommandBus(localCommandBus(uow)),
    queryBus: correlatingQueryBus(localQueryBus(uow)),
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

  **The buses own the unit of work.** `localQueryBus(unitOfWork)` now captures a
  factory exactly as `localCommandBus(unitOfWork)` always has, so
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
    commandBus: correlatingCommandBus(localCommandBus(unitOfWork)),
    queryBus: correlatingQueryBus(localQueryBus(unitOfWork)),
    modules: [module("billing", { eventStore }, ...slices)],
  });
  ```

  `module(name, stores, ...items)` required a stores record naming an
  `eventStore`. (Both the module wrapper and the record are deleted outright by
  the flat-fields change below; what survives is that persistence is attached by
  the host, per item, at composition.) Per-state `[state, options]` tuples are
  unchanged.

  The UoW-capture trap doc moved onto `localCommandBus`, where the capture
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

- 4d7b0ee: State queries are derived PER EVENT TYPE, not once for the whole state.

  Deriving one query item of (all folded types) × (the state's whole tag record) was
  too coarse. It paired every folded type with tags that type may not even carry,
  so the sourcing query — and the append condition derived from it — claimed a
  conflict window wider than the events could ever justify, and a multi-stream
  state had to spell its scope out as an explicit array of tag records.

  **The derivation now intersects, per event type.** For each entry in `evolve`,
  the state's tag record is intersected with the tag keys that event type declares;
  the distinct intersections become the ITEMS of the derived query — items are ORed
  — and each event type joins every item whose tag set it declares in full. Several
  shared keys are ANDed within one item. An event type sharing NO key with the
  state's tags is a boot error naming both — that fold can never fire, so it is a
  modelling mistake rather than a silent no-op.

  ```ts
  // before — the array form was REQUIRED to span streams, and every folded type
  // got paired with both tag records, including combinations that cannot match
  const Subscription = state({
    name: "CourseSubscription",
    id: { courseId: z.string(), studentId: z.string() },
    tags: (id) => [{ courseId: id.courseId }, { studentId: id.studentId }],
    evolve: [
      [CourseCreated, …],              // carries courseId only
      [StudentEnrolledInFaculty, …],   // carries studentId only
      [StudentSubscribedToCourse, …],  // carries both
    ],
  })

  // after — one plain record; the scope falls out of what each event declares
  const Subscription = state({
    name: "CourseSubscription",
    id: { courseId: z.string(), studentId: z.string() },
    tags: (id) => ({ courseId: id.courseId, studentId: id.studentId }),
    evolve: [ …unchanged… ],
  })
  ```

  Both forms derive the same two-item OR for that state, but the derived one drops
  the impossible pairings (`studentId` on a course event, `courseId` on a faculty
  enrolment) that the array form could not. `Subscription.query({ courseId, studentId })`
  returns exactly:

  ```ts
  [
    {
      tags: { courseId: "cs-101" },
      types: ["CourseCreated", "StudentSubscribedToCourse"],
    },
    {
      tags: { studentId: "stu-1" },
      types: ["StudentEnrolledInFaculty", "StudentSubscribedToCourse"],
    },
  ];
  ```

  **This is a behavior refinement, and it applies to the APPEND CONDITION too** —
  the same derived query is the sourcing query and the conflict window, so windows
  get narrower and more accurate together. Nothing widens: every derived item
  is at least as specific as what the previous derivation produced, and the derived
  query can never be match-all (an item with no tags is impossible, because an
  empty intersection throws first).

  Note the one case that deliberately does NOT narrow to an exact AND. When a
  sibling event type pins a narrower item, a type declaring that item's tags in
  full joins it as well. A subscription event carrying both `courseId` and
  `studentId` therefore also rides the `courseId`-only branch, which is what lets a
  capacity check see OTHER students' subscriptions to the same course. Pinning it
  to `courseId AND studentId` would under-source the fold and leave the append
  condition too narrow to catch the conflict it exists to catch. Where nothing
  forces the wider read, the full intersection is kept — a state scoped by
  `{ tenantId, orderId }` folding only order events still ANDs both keys and never
  sources a whole tenant.

  **`EventDescriptor` gains `tagKeys`, and `event()` derives it.** The intersection
  needs an event's tag KEYS without a payload in hand, so `tags` now also accepts a
  record of extractors whose own keys ARE the tag keys:

  ```ts
  // before — keys buried inside a function body, unknowable to the framework
  event({
    name: qn("university", "StudentSubscribedToCourse"),
    payload: z.object({ courseId: z.string(), studentId: z.string() }),
    tags: (p) => [tag("courseId", p.courseId), tag("studentId", p.studentId)],
  });

  // after — keys are data; `tagKeys` is derived as ["courseId", "studentId"]
  event({
    name: qn("university", "StudentSubscribedToCourse"),
    payload: z.object({ courseId: z.string(), studentId: z.string() }),
    tags: { courseId: (p) => p.courseId, studentId: (p) => p.studentId },
  });
  ```

  The `tags` FUNCTION form still works, for tag sets an extractor record cannot
  express — a payload-dependent key, or a variable number of tags. Its keys are
  genuinely not knowable, so `tagKeys` stays `undefined` and is NEVER guessed at:
  a state folding such an event fails at boot telling you to convert the descriptor
  or declare `tagKeys` explicitly.

  ```ts
  event({
    name: qn("catalog", "ItemsRelabelled"),
    payload: z.object({ items: z.array(z.string()) }),
    tags: (p) => p.items.map((id) => tag("itemId", id)),
    tagKeys: ["itemId"],
  });
  ```

  An event with no `tags` at all declares the EMPTY key set rather than an unknown
  one, so it is caught by the shared-key check like any other unmatchable fold.
  Passing `tagKeys` alongside a `tags` record throws — the two cannot disagree.

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

- 4d7b0ee: The last four seams nobody implemented: no event bus, no monitors, no retrying bus, no connection manager.

  **`simpleEventBus` is DELETED**, along with the `EventBus` and
  `SubscribableEventSource` exports. Event delivery here is tracked-only — an
  `eventProcessor` over an `EventStore`, reading a stream and keeping a token —
  so there is no on-commit lane for an in-memory bus to serve and no caller it
  could honestly have had. The two-method SHAPE survives as an INTERNAL type,
  because `EventStore` really is both halves (it persists events and it notifies
  subscribers) and `EventStore extends EventStorageEngine, EventBus` is how that
  gets said once. A host names `EventStore`; it never names `EventBus`.

  **`MessageMonitor`, `MonitorCallback`, `noOpMessageMonitor`,
  `multiMessageMonitor`, `MessageMonitorRegistry` and `messageMonitorRegistry` are
  DELETED.** A monitor is a wrapper over a bus you already have — that is exactly
  what `otlpCommandBus(bus, exporter)` is — so the seam bought nothing over
  writing the wrapper, and the registry existed to hold monitors nobody ever
  registered. Registration is not a concept in this framework.

  **`retryingCommandBus`, `RetryPolicy` and `exponentialBackoffRetryPolicy` are
  DELETED.** Retrying a command generically is wrong by default: without knowing
  whether the handler is idempotent, a retry at the bus is a duplicate write. The
  transports that need it retry their own reconnect, where the failure is
  transport-shaped and the answer is knowable.

  **`connectionManager` / `AxonServerConnectionManager` are DELETED** from
  `@kronos-ts/axon-server`. It cached one gRPC channel PER CONTEXT, which is the
  thing this release stopped doing: a context is a per-call header on ONE channel,
  so a per-context channel cache contradicts `axonServerEventStore(conn, ctx)`
  outright. Nothing used it.

  None of the five had a consumer anywhere in the tree.

- 4d7b0ee: `@kronos-ts/opentelemetry` is REMOVED, replaced by `@kronos-ts/otlp`: the protocol, not the ecosystem.

  The old package took the OpenTelemetry API as a peer and an SDK pair as dev
  dependencies, and reached core through a pair of seams core had to carry for its
  benefit — `SpanFactory` and `MetricsRecorder`, plus `tracingHandler` and
  `meteringHandler`. Those seams are DELETED from core, which now contains ZERO
  tracing vocabulary. Observability is a package of functions over the public
  shapes, which anybody could have written — so it is one.

  `@kronos-ts/otlp` speaks OTLP/JSON over `fetch` and depends on
  `@kronos-ts/core` and nothing else. No SDK, no global tracer, no patching, and
  no OpenTelemetry dependency anywhere in the tree.

  ```ts
  const exporter = otlpExporter({
    endpoint: "http://collector:4318",
    serviceName: "billing",
  });

  const commandBus = otlpCommandBus(
    interceptingCommandBus(bus, lineage),
    exporter
  );
  const handlers = slice.commandHandlers.map((h) => ({
    ...h,
    handler: otlpHandler(h.handler, exporter),
  }));
  ```

  - `otlpExporter({ endpoint, serviceName, flushIntervalMs? })` — a resource:
    batches spans and metrics, flushes on an interval, POSTs to `/v1/traces` and
    `/v1/metrics`, and `close()` flushes then stops. W3C trace and span ids are
    generated here; 64-bit nanosecond times are encoded as strings, as OTLP/JSON
    requires.
  - `otlpCommandBus(bus, exporter)` / `otlpQueryBus(bus, exporter)` — a span per
    dispatch, with `traceparent` injected into the outgoing message metadata.
  - `otlpHandler(handler, exporter, label?)` — wraps the handler FUNCTION and
    extracts `traceparent` from the handled message. Command and query MESSAGES
    become CHILDREN of the extracted context; event messages get their own trace
    with a LINK back to the producing span, so a projection catching up over a
    batch of old events is not swallowed into whatever produced them. Which leg it
    is comes from `message.kind` — there is no kind argument and no entry to ask.
  - `otlpMetricsHandler(handler, exporter, label?)` — duration, throughput and failure
    counters, sliced by `message_type` and `message_name`, both read off the
    message.
  - `label` ABSENT names the span (and keys the series) by the message's qualified
    name. Pass a `(message: Message) => string` to name it otherwise — a function
    OF THE MESSAGE, never a per-handler string closed over at wiring time.

  otel-js interop is a consumer concern: write wrappers over the same public
  shapes. That was always the honest boundary, and pretending otherwise cost core
  two seams.

- 4d7b0ee: A state says what it is scoped BY. The query is derived.

  **`state()` takes `tags` as plain data — `criteria` is gone.**

  ```ts
  // before — a criteria expression, built by a fluent builder, at every state
  state({
    name: "Course",
    id: { courseId: z.string() },
    criteria: (id) => EventCriteria.havingTags(tag("courseId", id.courseId)),
    evolve: [[CourseCreated, …], [CourseCapacityChanged, …]],
  })

  // after — the scope is a record
  state({
    id: { courseId: z.string() },
    tags: (id) => ({ courseId: id.courseId }),
    evolve: [[CourseCreated, …], [CourseCapacityChanged, …]],
  })
  ```

  ONE record is the answer even for a state spanning several streams — the
  derivation scopes each tag key to the event types that declare it. See the
  "granular query derivation" changeset for the rules. An ARRAY of records remains
  available as an explicit override for scopes the derivation cannot express:

  ```ts
  tags: (id) => [{ courseId: id.courseId }, { studentId: id.studentId }];
  ```

  **The event-TYPE half of the query is DERIVED from `evolve`.** A fold only
  reacts to what it lists, so the types are already stated; `state()` assembles
  the DCB query from the tags and the folded types, and you never write the type
  list twice.

  This NARROWS the DCB conflict window, deliberately. Every state previously
  passed tags only, so its sourcing query — which flows through
  `SourcingCondition` into the `AppendCondition` conflict detection runs against —
  claimed a conflict over EVERY event carrying that tag, including types it never
  folded. It now conflicts on the folded types only. Reviewed and accepted: no
  existing state relied on the wider window. If one ever needs it, that will be an
  explicit optional field on `state()`, not a return to hand-written queries.
  A state with no evolvers gets no type filter — an empty fold means "all", not
  "none".

  **The public `EventCriteria` builder is DELETED, and so is the `eventQuery()`
  compiler that briefly replaced it.** A read is now described by PLAIN DATA in the
  DCB specification's own vocabulary (dcb.events): a **query** is one **query
  item** — `{ types?: any-of, tags?: all-of }` — or an array of items ORed
  together. There is nothing to call and nothing to construct.

  ```ts
  // before — a fluent builder at every reading surface
  EventCriteria.havingTags(tag("courseId", "cs-101")).ofTypes(CourseCreated)
  EventCriteria.either(EventCriteria.havingTags(a), EventCriteria.havingTags(b))

  // then — a function wrapping a record literal, which is ceremony
  eventQuery({ tags: { courseId: "cs-101" }, types: [CourseCreated] })
  eventQuery([{ tags: a }, { tags: b }])

  // now — the query IS the literal
  { tags: { courseId: "cs-101" }, types: [CourseCreated] }
  [{ tags: a }, { tags: b }]                      // an array is the OR
  ```

  `@kronos-ts/messaging` exports `QueryItem` and
  `EventQuery = QueryItem | readonly QueryItem[]`, and
  `packages/messaging/src/event-criteria.ts` is now `event-query.ts`.

  **Every reading surface takes the query directly, under the field name `query`.**
  The old `criteria` field is gone from `SourcingCondition`, `AppendCondition`,
  `StreamingCondition`, `SourcingInfo` and `StateModule`:

  ```ts
  // before
  eventStore.source({ criteria: eventQuery({ tags: { courseId } }) });
  eventStore.append(events, { criteria, marker });
  Course.criteria({ courseId });

  // after
  eventStore.source({ query: { tags: { courseId } } });
  eventStore.append(events, { query, marker });
  Course.query({ courseId });
  ```

  A `commandHandler`'s `appendCondition` override now receives and returns an
  `EventQuery` rather than an `EventCriteria`:

  ```ts
  // before
  appendCondition: (command, sourcedCriteria) => EventCriteria.havingAnyTag();
  // after
  appendCondition: (command, sourcedQuery) => ({
    tags: { billId: command.payload.billId },
  });
  ```

  **The `EventCriteria` union survives as the STORE side of the boundary** — the
  tagged shape the in-memory matcher, the Postgres WHERE builder and the KronosDB /
  Axon Server criterion converters switch on. It is produced in exactly one way:
  `compileQuery(query)`, called once per read at each store's entry point.
  `queryItems(query)` is the single normalisation step for the one-item-vs-array
  split, and the single place a malformed query is rejected — an empty item array
  ("zero ORed items match nothing") and a non-item or nested-array query each fail
  with an error that names what was passed. Nothing downstream re-tests the shape;
  combining the queries of several `load()` calls into one append condition is now
  a flat concat of their items rather than a hand-built `either` node.

  Excess-property checking still bites at literal call sites despite `EventQuery`
  being a union — `{ tags: { … }, typ: [] }` is an error at the typo, in single and
  array positions alike — so no overloads were needed to keep it.

  **`name` is now OPTIONAL on `state()`.** Its only job is durable snapshot
  identity — the key snapshots are written under — so it is required only when
  that state is configured with a `snapshotPolicy` or a `snapshotStore`. `kronos`
  refuses to boot otherwise, with an error naming the state by its index in
  `states` and the events it folds (it has no name to quote).

  Everything else keys on a new `identity` the definition carries: a
  process-unique token `state()` assigns per definition. It is a property, not the
  object reference, because hosts spread states to attach stores
  (`{ ...Course, stores }`) — the identity rides through the spread, the reference
  does not. `StateManager` registers and resolves repositories by it, and the
  per-UnitOfWork `ctx.load` cache keys on `${identity}:${id}` instead of
  `${name}:${id}`. `StateRepository.stateName` is optional and diagnostic.

- 4d7b0ee: A transport takes your local bus and returns a bus. Core learns no transport vocabulary.

  Core briefly owned a generic routing layer — `distributedCommandBus` /
  `distributedQueryBus` over a `CommandBusConnector` / `QueryBusConnector` seam,
  plus a `SubscriptionRegistry` for the query side, plus `RoutingStrategy`. All of
  it is DELETED. Nothing but a transport ever implemented those interfaces, and
  splitting one routing decision across a package boundary meant the reply
  timeout, the identity-named reply queue and the prefer-local fork lived in two
  places that had to agree.

  Every transport now exposes the SAME two-argument shape: it takes your local
  segment and returns a bus of the same type, so composition is uniform and
  interception always wraps from outside.

  ```ts
  // before — a generic router in core, a connector in the transport
  interceptingCommandBus(
    distributedCommandBus(local, rabbitMqCommandConnector(rabbit)),
    lineage
  );

  // after — the transport owns its own routing
  interceptingCommandBus(rabbitMqCommandBus(rabbit, local), lineage);
  interceptingCommandBus(kronosDbCommandBus(kdb, local), lineage);
  interceptingCommandBus(axonServerCommandBus(conn, local), lineage);
  ```

  **Deleted from `@kronos-ts/core`:** `CommandBusConnector`, `QueryBusConnector`,
  `RemoteDispatchOptions`, `SubscriptionRegistry`, `SubscriberRecord`,
  `SubscriptionDelivery`, `distributedCommandBus`, `distributedQueryBus` and their
  options types, `RoutingStrategy`, `metadataRoutingStrategy`,
  `payloadFieldRoutingStrategy`. `SerializedError` survives, moved to the
  primitives beside `generateIdentifier`. Core now contains no word for "remote".

  **`@kronos-ts/rabbitmq`** — `amqpConnection` is renamed `rabbitMqConnection`,
  and its options are exactly what the broker topology needs:
  `rabbitMqConnection(url, { serviceName, instanceId, topology?, retry? })`. The
  connector pair is gone; `rabbitMqCommandBus(rabbit, local, { preferLocal?,
timeoutMs? }?)` and `rabbitMqQueryBus(rabbit, local, { preferLocal?, timeoutMs?
}?)` absorb what the router and the connector did together. Reply timeouts moved
  from the connection to the bus, where the dispatch that waits actually is.
  Client-side routing semantics are unchanged: competing consumers on durable
  per-command queues, identity-named exclusive reply queues, correlation-id
  matched replies, dead-lettering, and the gossip subscriber mirror that lets a
  plain function predicate filter subscription queries across instances.

  **`@kronos-ts/kronosdb` and `@kronos-ts/axon-server`** — the local segment
  becomes a REAL bus. It used to be a private `Map<string, handler>`, and an
  inbound server-routed command was run in a freshly minted `unitOfWork()` that
  the bus was separately handed. Now `subscribe` registers on `local` and
  announces the name to the server, and inbound work is dispatched INTO `local`:

  ```ts
  // before — two sources of truth for one policy
  kronosDbCommandBus(connection, unitOfWork, latch, serializer, flowControl, loadFactor)

  // after — `local` carries the unit-of-work policy, and only `local`
  kronosDbCommandBus(kdb, localCommandBus(unitOfWork))
  kronosDbCommandBus(kdb, postgresUnitOfWork(pg, unitOfWork) |> localCommandBus)
  ```

  That is what makes a `postgresUnitOfWork` apply to server-routed work exactly as
  it applies to local work. Server-side routing is untouched: both are smart hubs,
  so an outbound dispatch still always goes to the server — there is no
  client-side prefer-local fork on either.

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

- 4d7b0ee: `lineage` seeds roots instead of clobbering every hop's cause.

  `causationId` was stamped unconditionally as the dispatched message's own
  identifier. That is right for a message born at an edge and wrong for every
  other message in the system: `ctx.send` / `ctx.query` / `ctx.append` already
  stamp the handled message's identifier onto everything a handler emits — the
  TRUE cause — and the bus edge then overwrote it. Every message ended up claiming
  to have caused itself, so the causal graph was a set of self-loops and no
  multi-hop chain could be reconstructed.

  ```ts
  // before
  causationId: message.identifier;

  // after
  causationId: String(message.metadata.causationId ?? message.identifier);
  ```

  Both fields are `??` seeds now, which also makes double application a true
  no-op — a transport bus may wrap a local segment that is itself intercepting.
  `correlationId` is unchanged in behaviour.

  Tests that encoded the old behaviour are updated, including the real-broker
  RabbitMQ one: a command sent from a handler across the wire now arrives with the
  OUTER command's identifier as its cause.

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
