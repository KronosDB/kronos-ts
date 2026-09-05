# @kronos-ts/otlp

## 0.4.0

### Minor Changes

- 303f268: Live updates are the third capability tier — the first on a bus. The base
  `QueryBus` shrinks to two members; the subscription surface moves to
  `SubscriptionCapability`, and `ctx.emitUpdate` exists only against a bus that
  claims it. BREAKING.

  ```ts
  // before — every QueryBus implementer carried seven members
  type QueryBus<U> = {
    query;
    subscribe;
    subscriptionQuery;
    subscribeToUpdates;
    emitUpdate;
    completeSubscription;
    completeSubscriptionExceptionally;
  };

  // after — the seam is two; the tier is claimed, never implied
  type QueryBus<U> = { query; subscribe };
  type SubscriptionCapableQueryBus<U> = QueryBus<U> & SubscriptionCapability;
  ```

  Same construction as the two store tiers: `IfSubscriptionCapable<Q, …, …>` is
  the anchor, `SubscriptionEmit<Q>` derives the context face, and the contexts
  take the bus beside the log — `EventHandlerContext<E, Q, U>` /
  `CommandHandlerContext<E, Q, U>`, each parameter defaulted so plain code never
  writes any of them.

  ```ts
  // a projection that pushes live updates says so — and an entry whose bus
  // cannot serve them refuses it at compile time
  eventHandler(Enrolled, async (m, ctx) => {
    ctx.emitUpdate(Watch, …)        // ✗ property does not exist
  })
  eventHandler(Enrolled, async (m, ctx: EventHandlerContext & EmitCapability) => {
    ctx.emitUpdate(Watch, …)        // ✓ and the entry's queryBus must claim the tier
  })
  ```

  A handler demands the tier by intersecting `EmitCapability` — one name for the
  one thing it uses. The type parameters are the SUPPLY side (an entry threads
  its bus in, and `Q` is inferred from the bus the entry names, so hosts write no
  type arguments on either side); intersecting is the DEMAND side, exactly as the
  persistence packages' `DrizzleCapability` / `PostgresCapability` are written.

  The `subscriptionQuery` edge verb demands `SubscriptionCapableQueryBus`.
  `localQueryBus` offers the tier natively; the kronosdb, axon-server and
  rabbitmq buses offer it server- or broker-mediated; `interceptingQueryBus`,
  `otlpQueryBus` and `recordingQueryBus` preserve whatever tier the wrapped bus
  carried (`B` in, `B` out) instead of naming the members.

  Interception wraps the tier where it exists: `subscriptionQuery` /
  `subscribeToUpdates` run the same intercept the primary `query` runs, so
  subscription queries travel correlated across transports — the KNOWN-GAP
  comments in kronosdb/axon-server described an older core and are retired,
  pinned by a test.

### Patch Changes

- Updated dependencies [0a6a030]
- Updated dependencies [303f268]
- Updated dependencies [303f268]
- Updated dependencies [6890230]
- Updated dependencies [303f268]
  - @kronos-ts/core@0.4.0

## 0.3.0

### Minor Changes

- 1aef927: **Snapshotting is not a mechanism and not a seam. It is a CAPABILITY TIER on the event store, and the compiler makes you wire it.**

  It used to be the fifth mechanism, with a `SnapshotStore` seam beside the log, a `snapshotStore` field on every entry, and a generic decorator whose only job was marrying incapable stores to that separate seam. All of it is gone. There are **four** mechanisms — `interception/`, `correlation/`, `upcasting/`, `validation/` — and snapshotting is not among them, because a mechanism is a wrap-in that lives in core and serves every backend identically, and this cannot be: fusing a cache lookup into a read is a property of the **store you are reading from**, and the store families live in their own packages.

  ***

  **THE BASE CONTRACT MENTIONS SNAPSHOTS NOWHERE, AND IT IS COMPLETE WITHOUT THEM.**

  A log you can source, append, stream and subscribe to is everything the DCB model needs, and most well-designed projects never need one line more. If snapshotting exists at all, it exists **on the event store**, added by wrapping one.

  ```ts
  // before — two objects, two fields, and a host who could wire half of it
  const eventStore = postgresEventStore(pg, { serializer, tagResolver });
  const snapshotStore = postgresSnapshotStore(pg, { serializer });
  kronos({
    commandHandlers: h.map((x) => ({ ...x, eventStore, snapshotStore })),
  });

  // after — ONE object, one field, one serializer
  const eventStore = postgresSnapshottingEventStore(
    postgresEventStore(pg, { tagResolver }),
    pg,
    { serializer }
  );
  kronos({ commandHandlers: h.map((x) => ({ ...x, eventStore })) });
  ```

  ```ts
  // the capability, and the ONE member it adds
  type SnapshotCapability = {
    storeSnapshot(
      key: string,
      snapshot: Snapshot,
      uow?: UnitOfWork
    ): Promise<void>;
  };
  type SnapshotCapableEventStore = EventStore & SnapshotCapability;
  ```

  There is no `loadSnapshot`, because **reading is not a second call**: a capable store honours `condition.snapshot` inside `source()` and leads its `SourcingResult` with the cached fold. The read was already in `EventStore`'s shape; only the write needed a name.

  ***

  **THE HEADLINE: A COMPILE-TIME DEMAND.**

  A snapshot policy used to be a wish. Declare one, forget the store, and you got a silent full replay plus a cache nobody read — a performance mystery, months later, with nothing in the diff to point at. Now the state's **type** says it caches, and `ctx.load` refuses it against a log that cannot serve one.

  ```ts
  const Course = state({ /* … */, snapshot: { key: "course-v1", when: afterEvents(100) } })

  // ✗ — does not compile
  commandHandler(OpenCourse, async (m, ctx: HandlerContext) => {
    await ctx.load(Course, { courseId })
  })

  // ✓ — say what you need, and the ENTRY must supply it
  commandHandler(OpenCourse, async (m, ctx: HandlerContext<UnitOfWork, SnapshotCapableEventStore>) => {
    await ctx.load(Course, { courseId })
  })
  ```

  The real diagnostic, verbatim:

  ```
  error TS2345: Argument of type 'State<InferIdFromSchema<{ courseId: ZodString; }>, CourseState, true>'
  is not assignable to parameter of type 'State<{ courseId: string; }, CourseState, any> &
  { readonly snapshot?: { readonly ERROR: "this state declares a snapshot policy, but this
  handler's eventStore cannot serve one"; readonly FIX: "wrap this entry's eventStore in the
  snapshotting wrapper for its persistence family — <family>SnapshottingEventStore(store, …)"; ...'.
    Types of property 'snapshot' are incompatible.
      Type 'SnapshotConfig | undefined' is not assignable to type '{ readonly ERROR: …;
      readonly FIX: …; } | undefined'.
        Type 'SnapshotConfig' is missing the following properties from type
        '{ readonly ERROR: …; readonly FIX: …; }': ERROR, FIX
  ```

  The demand travels the way correlation's does: annotate the context, and the entry that places the handler must carry a log which satisfies it — so the mistake stops at the composition root.

  **Three types changed to carry it, and every default is such that plain code writes nothing.**

  ```ts
  // before
  type State<Id = unknown, S = unknown> = { …, snapshot?: SnapshotConfig }
  type HandlerContext<U extends UnitOfWork = UnitOfWork> = EventHandlerContext<U> & { append }

  // after — `Snap` is what `snapshot` is TYPED BY, and state() infers it off your config
  type State<Id = unknown, S = unknown, Snap extends boolean = false> = {
    …
    readonly snapshot?: Snap extends true ? SnapshotConfig : undefined
  }
  type HandlerContext<U extends UnitOfWork = UnitOfWork, E extends EventStore = EventStore> =
    EventHandlerContext<U, E> & { append }
  ```

  `E` is the **entry's** event store, threaded from the composition root through the subscribe glue and the context builders. Buses never carry a store; entries do. `EventHandlerContext` and `QueryHandlerContext` take it too, as does `HandlerSite`, `Sited`, all three entry types and `kronos` itself.

  **ONE ALIAS IS THE DEMAND, and both read surfaces derive from it.**

  ```ts
  // event-sourcing/load.ts — THE anchor. Add a face; never add a predicate.
  type IfSnapshotCapable<E extends EventStore, Capable, Bare> =
    E extends SnapshotCapableEventStore ? Capable : Bare

  type SnapshotReads<E>  = IfSnapshotCapable<E, { source: FusedSourceFunction }, unknown>
  type SnapshotDemand<E> = IfSnapshotCapable<E, unknown, { snapshot?: <branded refusal> }>

  type LoadFunction<E extends EventStore = EventStore> =
    <Id, S>(state: State<Id, S, any> & SnapshotDemand<E>, id: Id) => Promise<S>
  ```

  Contexts are **assembled by intersection** — the base shape `& SnapshotReads<E>` — so against a bare log the fused `ctx.source(query, { snapshot })` overload is structurally **absent**, not present-and-erroring. Asking for it reads `Expected 1 arguments, but got 2`, which is the truth: on that log, `source` takes one.

  **Nothing runs.** The whole demand is erased. The JavaScript a demanded `ctx.load` emits is identical to what an undemanded one emitted, and the only runtime trace of the entire feature is **one defensive `throw` in `repository.ts`**, for JavaScript callers who had no compiler to be held by.

  ***

  **FOUR WRAPPERS, ONE PER FAMILY. The generic decorator and all four snapshot stores are gone.**

  ```ts
  inMemorySnapshottingEventStore<E extends EventStore>(next: E): E & SnapshotCapability
  postgresSnapshottingEventStore<E extends EventStore>(next: E, pg, { serializer }): E & SnapshotCapability
  kronosDbSnapshottingEventStore<E extends EventStore>(next: E, kdb, context?): E & SnapshotCapability
  axonServerSnapshottingEventStore<E extends EventStore>(next: E, conn, context): E & SnapshotCapability
  ```

  **Postgres fuses in ONE round trip** because it holds the connection: the wrapper absorbed `postgresSnapshotStore`'s upsert **and** the CTE that used to live natively in `postgresEventStore`, so the base store is now snapshot-free — `PostgresEventStoreConfig` loses `serializer` entirely, and the wrapper has the only one. **The two-serializer footgun is gone with it:** one function now writes the bytes it later reads.

  **KronosDB fuses in ONE round trip too, natively** — see below. **The other two fuse client-side** — `getLast`/`Map` lookup, then a source after its position, inside the one function. Two calls where postgres needs one, which is a difference in what a wrapper can **reach**, not in what the capability **means**. Axon Server's `SnapshottedDcbEventStore.Source` is `UNIMPLEMENTED` on `2025.2.5` **and** `2026.0.4` (`DcbSnapshotStore/GetLast` answers on both). When it lands it changes one function body and **no host's code**, because the capability was never a promise about round trips.

  ***

  **KRONOSDB SERVES THE FUSED READ ITSELF NOW — and the client-side fusion is deleted.**

  KronosDB 0.8 puts snapshots **on the log** (its ADR-0005): a snapshot is a system record appended through the ordinary replication path, not a row in a sidecar store. The standalone `SnapshotStore` service — `Add`/`Delete`/`List`/`GetLast` — **is gone from the server**, and two RPCs on `EventStore` replace it: `AppendSnapshot` for the write, `SnapshottedSource` for the read. So the wrapper stopped fusing and started asking.

  ```ts
  // before — two calls, assembled here
  const snapshot = await connection.snapshotStore.getLast({ key })   // call 1
  const start    = snapshot.position + 1n
  const result   = await next.source({ ...plain, start })            // call 2
  return { ...result, snapshot }

  // after — one call, and the server leads the stream with the fold
  const stream = connection.eventStore.snapshottedSource({ criteria, key, batchSize: 0 })
  for await (const response of stream) {
    if (response.snapshot) snapshot = fromProto(response.snapshot)   // ≤1 frame, always first
    else { events.push(...); marker = markerAt(batch.consistencyMarker) }
  }
  return { events, marker, snapshot }
  ```

  There is **no fallback**. A server that does not serve the RPC fails loudly rather than quietly costing twice.

  **And the client-side fusion was wrong.** A KronosDB consistency marker is **next-exclusive** — it is already the sequence a replay resumes AT — so `snapshot.position + 1` stepped over any event that landed between the fold and the snapshot write. The server resumes at `position` exactly, for exactly that reason. The bug did not survive the move, and an integration test now pins the boundary rather than a count:

  ```ts
  // an event lands BETWEEN the fold and the snapshot write, so it sits AT the marker
  expect(fused.events.length).toBe(1); // native path: returned
  expect(offByOne.events.length).toBe(0); // `position + 1`: dropped
  ```

  `storeSnapshot` stays **fire-and-forget**, by contract: the record is appended after the transaction it summarizes commits, and is not enlisted in it. `snapshot.position` crosses the wire **unmodified** in both directions — no arithmetic on either side.

  The server also exposes `GetSnapshot` (the latest entry alone, for adapters that load snapshots separately). It is **deliberately not wired**: reading a cached fold is not a second call, and `SnapshottedSource` is the path this capability means.

  **BREAKING for `@kronos-ts/kronosdb` — `SnapshotStoreDefinition` is removed**, along with `proto/snapshot.proto`, its generated module, and the `snapshotStore` client on `KronosDbConnection`. They addressed a service the server no longer runs, so keeping them would only let a host wire a client that cannot connect. `kronosDbServiceDefinitions` loses its `snapshotStore` entry. Nothing that goes through `kronosDbSnapshottingEventStore` is affected — the capability is unchanged; only the transport under it moved.

  ***

  **BREAKING — wrappers are capability-preserving now, and some were not.**

  A wrapper whose input and output are the same seam but typed `(Base) => Base` **launders**: the runtime object still delegates everything the inner one had, but the signature threw the capability away — so a genuinely capable configuration gets rejected by a demand for a capability it actually has. That is worse than no demand at all, because it is unfixable from the call site. The rule, now a SURFACE doctrine line:

  > Same-seam wrappers are generic identity; capability adders are additive intersections.

  ```ts
  // before                                          // after
  upcastingEventStore(next: EventStore, u): EventStore   → <E extends EventStore>(next: E, u): E
  otlpCommandBus(next: CommandBus, x): CommandBus        → <B extends CommandBus<any>>(next: B, x): B
  otlpQueryBus(next: QueryBus, x): QueryBus              → <B extends QueryBus<any>>(next: B, x): B
  interceptingCommandBus<U>(next: CommandBus<U>, i)      → <B extends CommandBus<any>>(next: B, i): B
  interceptingQueryBus<U>(next: QueryBus<U>, i)          → <B extends QueryBus<any>>(next: B, i): B
  recordingEventStore(store: EventStore)                 → <E extends EventStore>(store: E): E & EventRecording
  recordingCommandBus<U>(bus: CommandBus<U>)             → <B extends CommandBus<any>>(bus: B): B & CommandRecording
  recordingQueryBus<U>(bus: QueryBus<U>)                 → <B extends QueryBus<any>>(bus: B): B & QueryRecording
  ```

  `otlpCommandBus`/`otlpQueryBus` were the live bug: typed bare, they erased `U` **and** rebuilt a narrower record, so tracing a correlating chain produced a bus no correlating handler would typecheck behind — the runtime worked and the build did not. Both now spread the wrapped bus and preserve its type. `RecordingEventStore`/`RecordingCommandBus`/`RecordingQueryBus` still exist and mean what they meant; the added members are also exported on their own (`EventRecording`, `CommandRecording`, `QueryRecording`) so the wrappers can be additive. `rabbitMqCommandBus`, `axonServerCommandBus` and `kronosDbCommandBus` were already `U`-preserving and are unchanged.

  Type probes pin all of it: all four family wrappers satisfy the capability, both stacking orders (upcasting-inside-snapshotting and snapshotting-inside-upcasting) keep both, the recorder-outermost fixture composition stays capable, and the correlating → local → rabbitMq/otlp → intercepting chains keep `CommandBus<CorrelatingUnitOfWork>`.

  ***

  **BREAKING — the `snapshotStore` entry field is removed.**

  `HandlerSite`, `CommandInvocationDeps`, `HandlerContextDeps`, `ProcessorHandlerEntry`, `subscribeQueryHandlers`'s deps and every doc site lose it. One store object per entry, capabilities and all. `eventSourcedRepository(state, eventStore)` and `repositoryFor(state, eventStore)` lose their trailing parameter, and the per-site repository cache drops from three levels to two — there is one object to key on now, which is what it was always really keyed on.

  **BREAKING for `@kronos-ts/test` — the fixture scope takes ONE store.**

  ```ts
  // before
  type FixtureScope = (
    eventStore: EventStore,
    snapshotStore: SnapshotStore
  ) => FixtureLists;
  testFixture((eventStore, snapshotStore) =>
    courses(eventStore, snapshotStore)
  );

  // after
  type FixtureScope = (eventStore: FixtureEventStore) => FixtureLists;
  testFixture((eventStore) => courses(eventStore));
  ```

  `FixtureEventStore` is the one object the fixture owns: in-memory, recording, and snapshot-capable. The fixture composes what a host composes — `recordingEventStore(inMemorySnapshottingEventStore(inMemoryEventStore()))`, recorder outermost so `appended` is still what left the fixture — and because both wrappers are additive, the capability survives the layer above the store that has it. `PartialProcessor`'s first parameter is that same store.

  Migration for a scope: delete the second parameter, and delete `snapshotStore` from the entries it spread. If your states declare snapshot policies, that is all — the fixture's log already serves them, and the compiler will tell you if it does not.

  ***

  **Everything the mechanism MEANT is unchanged.**

  Latest-only; never migrated; never load-bearing. The key is a string you wrote, and changing it is the whole invalidation story. `state({ snapshot: { key, when } })` is still sugar over the raw pair — which is now two members of one object:

  ```ts
  const key = `course:${courseId}`;
  const { snapshot, events, position } = await ctx.source(query, {
    snapshot: key,
  });
  const state = events.reduce(fold, (snapshot?.state as S) ?? initial);
  if (events.length > 100)
    await eventStore.storeSnapshot(key, { state, position });
  ```

  Fusing still does not narrow the append condition. The leading snapshot is still its own field on `SourcingResult` and still not an event. The read still belongs to the store and the write still belongs to the fold, fire-and-forget with its failure swallowed. Structural fitness is still the safety net under the key, judged once in core against `initial(id)` for every backend. `Snapshot`, `SnapshotPolicy`, `SnapshotConfig`, `afterEvents`, `whenSourcingTimeExceeds`, `noSnapshotPolicy`, `snapshotIdentifier` and `matchesInitialStructure` all keep their shapes; they moved from `snapshotting/` into `event-sourcing/`, beside the fold that asks for them.

  `withoutSnapshotKey(condition)` is newly exported from core — the four wrappers all need it and none of them owns it.

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

## 0.2.0

### Minor Changes

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
