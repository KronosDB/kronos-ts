# @kronos-ts/postgres

## 0.10.0

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

## 0.9.2

### Patch Changes

- Updated dependencies [2f42ed2]
  - @kronos-ts/messaging@0.11.0
  - @kronos-ts/app@0.6.2
  - @kronos-ts/eventsourcing@0.4.2

## 0.9.1

### Patch Changes

- Updated dependencies [9ad1a3c]
  - @kronos-ts/eventsourcing@0.4.1
  - @kronos-ts/app@0.6.1
  - @kronos-ts/messaging@0.10.1

## 0.9.0

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
  (`inMemoryEventStore`, `localCommandBus`, `postgresEventStore`, …).
  Drizzle stores no longer take the ORM operator bundle — only
  `{ db, table }`.

### Patch Changes

- Updated dependencies [b46a045]
  - @kronos-ts/app@0.6.0
  - @kronos-ts/messaging@0.10.0
  - @kronos-ts/eventsourcing@0.4.0

## 0.8.3

### Patch Changes

- Updated dependencies [f3f9fbc]
  - @kronos-ts/common@0.1.2
  - @kronos-ts/messaging@0.9.2
  - @kronos-ts/eventsourcing@0.3.3
  - @kronos-ts/app@0.5.3

## 0.8.2

### Patch Changes

- Updated dependencies [ad944b9]
  - @kronos-ts/messaging@0.9.1
  - @kronos-ts/app@0.5.2
  - @kronos-ts/eventsourcing@0.3.2

## 0.8.1

### Patch Changes

- 3246b67: Order the gap-free tail by `transaction_id` numerically instead of lexically.

  The streaming query selects `transaction_id::text AS transaction_id`, and `ORDER BY transaction_id` bound to that text alias — so the xid8 tail cursor was sorted as text. Once a working set's transaction ids straddle a power-of-ten boundary (e.g. 999 → 1000), lexical order (`"1000" < "999"`) diverges from numeric order while the `WHERE (transaction_id, sequence_position) > (…)` resume comparison stays numeric. The two disagree, so the stream delivers events out of commit order and strands events when reopened from a tracking token (e.g. after a handler error redelivery). `ORDER BY` now references the `transaction_id` / `sequence_position` columns directly, matching the numeric comparison used everywhere else.

## 0.8.0

### Minor Changes

- 9eb84ff: Carry the commit-order key in durable tracking tokens so gap-free tailing resumes correctly.

  The postgres engine tails events in `(transaction_id, sequence_position)` order with a `pg_snapshot_xmin` watermark, but durable tokens stored only `sequence_position`. On stream reopen the catch-up filter compared positions alone, so an event with a lower `sequence_position` but higher `transaction_id` — which happens when a transaction writes other rows (stamping its xid) before appending its event — was permanently skipped.

  - `messaging`: adds `gapAwareToken(sequence, gapKey)` (a `TrackingToken` carrying an opaque commit-order key alongside the position), `advanceTokenTo`, and `serializeToken`/`deserializeToken`. `SequencedEvent` and `StreamingCondition` gain an optional `token`, letting an engine hand the processor its own resume cursor instead of a bare position. Both processors persist the engine-supplied token when present.
  - `postgres`: `open()` emits a gap-aware token per event and, on reopen, resumes the `(transaction_id, sequence_position)` tuple cursor from it. Engines that supply no token (in-memory, Axon Server) are unaffected.
  - token stores (`knex`, `kysely`, `drizzle`, `prisma`, `typeorm`): serialize through the shared `messaging` helpers so the commit-order key round-trips instead of being flattened to a position.

  Token format change: tokens written before this release carry no commit-order key. They rehydrate as position-only tokens and resume via the legacy catch-up branch on first reopen, then mint gap-aware tokens going forward; to close the window immediately, reset the affected processors.

### Patch Changes

- Updated dependencies [9eb84ff]
  - @kronos-ts/messaging@0.9.0
  - @kronos-ts/app@0.5.1
  - @kronos-ts/eventsourcing@0.3.1

## 0.7.0

### Minor Changes

- 56bfb6d: Move per-transaction safety timeouts onto the database adapter.

  - `pgAdapter`, `postgresAdapter`, and `bunSqlAdapter` now accept `idleInTransactionTimeoutMs` (default 30000) and `statementTimeoutMs` (default 0) and arm them via `SET LOCAL` on every transaction they open — UoW-scoped commits, event-store own-tx appends, and the scheduler worker tick alike. Each adapter instance is configured independently, so two adapters pointed at two databases stay decoupled.
  - `postgresTransactionManager` no longer takes timeout options and no longer issues `SET LOCAL`; it is now a pure begin/commit/rollback bridge. The `postgres({ transaction: { ... } })` config is removed — set the timeouts on the adapter instead.
  - `drizzleTransactionManager` and `knexTransactionManager` accept an `onBeginTransaction(tx)` hook that runs once per transaction, before the UnitOfWork uses it — the seam for arming session settings (e.g. `SET LOCAL idle_in_transaction_session_timeout`) on those clients so a stalled drain is bounded.

### Patch Changes

- Updated dependencies [56bfb6d]
- Updated dependencies [56bfb6d]
- Updated dependencies [56bfb6d]
  - @kronos-ts/messaging@0.8.0
  - @kronos-ts/app@0.5.0
  - @kronos-ts/eventsourcing@0.3.0

## 0.6.0

### Minor Changes

- da0ccae: Simplify the postgres event-store time columns.

  - The events table drops `recorded_at` (the DB insert time). It was written by default but never read — the store carries only the EventMessage's authored timestamp.
  - The authored-timestamp column is renamed `message_timestamp` → `timestamp` on both the events and scheduled-events tables, so a schedule row and the event it materialises into share the same column names (`version`, `timestamp`). It stays a `BIGINT` of epoch milliseconds (btree/BRIN-indexable).

  Schema change is CREATE-only: an events or scheduled-events table created before this release must be hand-migrated (`ALTER TABLE … DROP COLUMN recorded_at`, `ALTER TABLE … RENAME COLUMN message_timestamp TO timestamp`) or reset before upgrading.

### Patch Changes

- Updated dependencies [dafdf12]
  - @kronos-ts/messaging@0.7.0
  - @kronos-ts/app@0.4.1
  - @kronos-ts/eventsourcing@0.2.3

## 0.5.0

### Minor Changes

- 291acd2: Add a `kind` discriminator to messages and bring the postgres event store to full `EventMessage` round-trip parity.

  - `Message` now carries `readonly kind: "command" | "event" | "query"` (exported as `MessageKind`), narrowed to the literal on `CommandMessage`, `EventMessage`, and `QueryMessage`. Handler interceptors can branch on message category at runtime via `message.kind`. Gateways, `send()`, `append()`, `schedule()`, and every event-store reconstruction set it; `kind` is derived, never persisted.
  - The postgres event store now persists `version` and `message_timestamp` and selects `event_id`, so `source()` and `open()` reconstruct the complete `EventMessage` — `identifier`, authored `timestamp`, and `version`. Previously these three fields were dropped on read, diverging from the in-memory, axon-server, and kronosdb engines.
  - Schema change is CREATE-only. `CREATE TABLE IF NOT EXISTS` does not add columns to an existing table and the new columns are `NOT NULL`, so an events table created before this release must be hand-migrated (`ALTER TABLE ... ADD COLUMN`) or reset before upgrading.

### Patch Changes

- Updated dependencies [291acd2]
- Updated dependencies [291acd2]
  - @kronos-ts/messaging@0.6.0
  - @kronos-ts/app@0.4.0
  - @kronos-ts/eventsourcing@0.2.2

## 0.4.0

### Minor Changes

- 4ac26c0: Validate `schedule()`/`scheduleAfter()` inputs and bound transaction lifetime.

  - `schedule()` rejects an invalid `at` (`Invalid Date`); `scheduleAfter()` rejects a non-finite `delayMs`. A past time / negative delay is still allowed and fires as soon as possible.
  - `postgresTransactionManager` applies `idle_in_transaction_session_timeout` (default 30000ms) via `SET LOCAL` on every transaction, with an optional `statement_timeout`. Configurable through `postgres({ transaction: { idleInTransactionTimeoutMs, statementTimeoutMs } })`. Set either to `0` to disable.
  - `postgresTransactionManager.begin()` now rejects instead of hanging when the transaction callback fails before the handle is returned.

### Patch Changes

- Updated dependencies [4ac26c0]
  - @kronos-ts/eventsourcing@0.2.1
  - @kronos-ts/app@0.3.4
  - @kronos-ts/messaging@0.5.1

## 0.3.3

### Patch Changes

- Updated dependencies [6a3dca4]
  - @kronos-ts/eventsourcing@0.2.0
  - @kronos-ts/messaging@0.5.0
  - @kronos-ts/app@0.3.3

## 0.3.2

### Patch Changes

- Updated dependencies [dc0f67e]
- Updated dependencies [f5ed7da]
  - @kronos-ts/messaging@0.4.0
  - @kronos-ts/app@0.3.2
  - @kronos-ts/eventsourcing@0.1.5

## 0.3.1

### Patch Changes

- Updated dependencies [4b8faa5]
  - @kronos-ts/messaging@0.3.1
  - @kronos-ts/app@0.3.1
  - @kronos-ts/eventsourcing@0.1.4

## 0.3.0

### Minor Changes

- 74dc43d: Command handlers now run inside the UnitOfWork's transaction, so a handler's appended events and any other writes it makes commit — or roll back — atomically.

  Previously the command bus opened a fresh UnitOfWork that bypassed the configured `unitOfWorkFactory`, so a transaction provided by a backend never reached command handlers (each `append()` opened its own short-lived transaction instead). The in-memory `createLocalCommandBus` now runs handlers through the configured `unitOfWorkFactory` — matching the distributed buses (kronosdb / axon-server), which already did this. With the in-memory default factory (`runInNewUoW`) behavior is unchanged; a transactional backend gives each command's UoW a transaction.

  `@kronos-ts/postgres`: command handlers are transactional out of the box — **lazy**, so pure-read handlers never claim a connection. `PostgresAdapterTransaction` gains `unwrap<T>()`, which returns the live driver connection backing the UoW transaction (pg `PoolClient`, or the scoped `sql` for porsager/Bun). Use it to run your own SQL, or bind an ORM, in the same transaction as your events:

  ```ts
  const tx = await getOrBeginActiveTransaction<PostgresAdapterTransaction>();
  await tx!.query("UPDATE widgets SET name = $1 WHERE id = $2", [name, id]);
  append(WidgetUpdated, { id, name }); // same commit
  // or hand tx.unwrap() to Drizzle/Kysely — see the @kronos-ts/postgres README.
  ```

  **Breaking (`@kronos-ts/messaging`):** `createCommandGateway(bus, unitOfWorkRunner?)` is now `createCommandGateway(bus)` — the gateway is a thin message-builder and no longer opens a UnitOfWork; the command bus owns the single per-command UoW (AF5-aligned). `createLocalCommandBus()` now accepts an optional `UoWRunner` (defaults to `runInNewUoW`). Direct callers of `createCommandGateway` that passed a runner should drop the second argument; the transactional runner now belongs on the `unitOfWorkFactory` slot, which the bus consumes.

  **Breaking (`@kronos-ts/postgres`)** for custom adapter authors only: `PostgresAdapterTransaction` now requires an `unwrap<T>(): T` method returning the underlying driver connection. The three bundled adapters (pg / postgres / bun-sql) implement it; custom adapters must add it.

### Patch Changes

- Updated dependencies [c1a1cf5]
- Updated dependencies [74dc43d]
  - @kronos-ts/app@0.3.0
  - @kronos-ts/messaging@0.3.0
  - @kronos-ts/eventsourcing@0.1.3

## 0.2.0

### Minor Changes

- Add a durable EventScheduler for deferring events to a future time.

  `schedule(event, at)` is callable only inside a UnitOfWork so a scheduled
  event commits or rolls back atomically with the originating command;
  `cancel(token)` returns a `CancelResult` discriminated union
  (`cancelled` | `already-appended` | `not-found`).

  - `@kronos-ts/messaging` exports the `EventScheduler` contract and a
    `setTimeout`-backed in-memory implementation for tests, plus a lazy
    transactional UnitOfWork runner so writers share one transaction per UoW.
  - `@kronos-ts/app` adds `eventScheduler` as a typed `KronosComponents` slot
    with an in-memory default that emits a durability startup warning.
  - `@kronos-ts/postgres` provides a durable scheduler backed by
    `kronos_scheduled_events` with a `FOR UPDATE SKIP LOCKED` polling worker;
    `schedule_id` is reused as the event id so re-fires after a crash dedupe
    via the events table's UNIQUE constraint.

### Patch Changes

- Updated dependencies
- Updated dependencies
  - @kronos-ts/messaging@0.2.0
  - @kronos-ts/app@0.2.0
  - @kronos-ts/eventsourcing@0.1.2

## 0.1.1

### Patch Changes

- Publish via `bun publish` so `workspace:*` resolves to concrete versions in published manifests. Previously `changeset publish` shelled out to `npm publish`, which does not understand Bun's workspace protocol, leaving literal `"workspace:*"` strings in published manifests and breaking installs for consumers.
- Updated dependencies
  - @kronos-ts/common@0.1.1
  - @kronos-ts/messaging@0.1.1
  - @kronos-ts/eventsourcing@0.1.1
  - @kronos-ts/app@0.1.1
