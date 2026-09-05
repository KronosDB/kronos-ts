# @kronos-ts/kronosdb

## 0.8.0

### Minor Changes

- 0a6a030: A context capability exists only when a handler has something new to call. BREAKING renames and deletions.

  **Snapshotting is a store tier with no context type.** `SnapshotReads`, `SnapshotDemand`, `IfSnapshotCapable`, `FusedSourceFunction` and `SnapshottedSource` are gone. `ctx.source` has one signature everywhere, `ctx.load` accepts any state against any context, and a snapshot-policy state loaded through a bare log throws at runtime on the first load (`capableOrThrow`). Wire `<family>SnapshottingEventStore` underneath and declare `state({ snapshot })`; no handler names the tier.

  **One naming rule.** `<Tier>Capability` is what a handler intersects on its context. `<Tier>StoreCapability` / `<Tier>BusCapability` is what a wrapper adds to a store or bus. `<Tier>Capable<Thing>` aliases for composition roots are unchanged.

  | before                                     | after                                                                 |
  | ------------------------------------------ | --------------------------------------------------------------------- |
  | `EmitCapability` (ctx)                     | `SubscriptionCapability`                                              |
  | `ScheduleFunctions` (ctx)                  | `ScheduleCapability`                                                  |
  | `SubscriptionCapability` (bus)             | `SubscriptionBusCapability`                                           |
  | `ScheduleCapability` (store)               | `ScheduleStoreCapability`                                             |
  | `SnapshotCapability` (store)               | `SnapshotStoreCapability`                                             |
  | `ScheduleVerbs<E>` / `SubscriptionEmit<Q>` | `SuppliedScheduleCapability<E>` / `SuppliedSubscriptionCapability<Q>` |

  **The per-package `<Pkg>CommandContext` / `<Pkg>EventContext` / `<Pkg>QueryContext` aliases are deleted** (drizzle, knex, kysely, prisma, postgres). A host names its context once:

  ```ts
  // before
  commandHandler(Edit, async (m, ctx: CommandHandlerContext<SnapshotCapableEventStore & ScheduleCapableEventStore> & EmitCapability & DrizzleCapability) => …)
  commandHandler(Edit, async (m, ctx: DrizzleCommandContext) => …)

  // after — one contexts file, no type parameters in slice code
  type CmdCtx = CommandHandlerContext & ScheduleCapability & SubscriptionCapability & DrizzleCapability
  commandHandler(Edit, async (m, ctx: CmdCtx) => …)
  ```

- 303f268: Buses are named, contexts are logs: the bus wrappers take a plain bus-name
  string and stop addressing event store contexts. Aligns with server 0.9
  (ADR-0006 decoupled buses, ADR-0007 messaging fabric). BREAKING.

  ```ts
  // before — messaging borrowed the store's context header
  kronosDbCommandBus(next, kdb, { context: "orders" });
  kronosDbQueryBus(next, kdb, { context: "orders" });

  // after — a bus is its own dimension, a plain string, "default" when omitted
  kronosDbCommandBus(next, kdb, "orders");
  kronosDbQueryBus(next, kdb, "orders", { timeoutMs });
  ```

  Every messaging RPC (handler streams, dispatch, query, subscription queries)
  now carries the per-call `kronosdb-bus` header and never `kronosdb-context`.
  The server has NO fallback from bus to context — against a 0.9 server, a
  client that named contexts for messaging isolation lands on the `default`
  bus; name your buses (after your contexts, if 1:1 is what you meant). Store
  wrappers are unchanged and keep `kronosdb-context`. With the 0.9 fabric a bus
  name is cluster-wide: subscribe via any node, dispatch via any node.

  Also: `kronosDbSchedulingEventStore` takes an optional trailing
  `context: string` (defaults to the connection's), matching the other store
  wrappers; `busMetadata(bus, { token })` is exported beside `kronosMetadata`.

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

- 303f268: Export `busMetadata` beside `kronosMetadata`. The messaging-plane header
  helper was added with the named-bus split but never re-exported, so a host
  composing its own calls could reach the context helper and not the bus one.
- 303f268: Subscription queries refill their flow-control credit. Long-lived subscriptions
  used to stop delivering after the initial window and never resume.

  The server grants a subscription a window of credit, decrements it per update,
  and DROPS updates once it reaches zero — silently, with nothing sent back to say
  so. The client granted `bufferSize ?? 256` on `subscribe` and never topped it
  up, so every subscription query stopped delivering after that many updates, with
  no error on either side to read as the cause.

  ```ts
  // before — one grant, no refill: works for `window` updates, then stops
  outboundSub.send({ subscribe: { subscriptionIdentifier, numberOfPermits: BigInt(bufferSize ?? 256), … } })

  // after — the window is topped back up as updates are consumed
  handler.offer(update)
  if (++consumedSinceRefill >= refillBatch && !subscriptionClosed) {
    outboundSub.send({ flowControl: { subscriptionIdentifier, numberOfPermits: BigInt(consumedSinceRefill) } })
    consumedSinceRefill = 0
  }
  ```

  Refills go out once a quarter-window has accrued, so the wire carries one small
  message per quarter-window rather than one per update, and none is sent after
  `close()`.

  The requested window is also clamped to the server's own `[1, 1024]` bound
  instead of being trusted: a host passing `bufferSize: 5000` is granted 1024, and
  pacing refills against a 5000-wide window would let 1250 updates pass before
  topping up — 226 of them beyond the credit the server actually holds, and
  dropped. Refill cadence follows what the server granted.

  Nothing changes on the handler leg: credit is enforced centrally on the server's
  subscription registry, so a handler tracking permits locally would double-count
  the same window.

- Updated dependencies [0a6a030]
- Updated dependencies [303f268]
- Updated dependencies [303f268]
- Updated dependencies [6890230]
- Updated dependencies [303f268]
  - @kronos-ts/core@0.4.0

## 0.7.0

### Minor Changes

- 1aef927: Bring your own schema library. Descriptors now take any [Standard Schema](https://standardschema.dev), so zod, valibot, arktype and anything else that carries a `~standard` property all work — and `@kronos-ts/core` depends on none of them.

  ```ts
  // before — the constraint named one library, and core shipped it
  export type CommandDescriptor<P extends z.ZodType, R extends z.ZodType | undefined> = …
  export type EventDescriptor<P extends z.ZodType> = { tags?: (p: z.infer<P>) => Tag[] }

  // after — the constraint names the CONTRACT
  export type CommandDescriptor<P extends StandardSchemaV1, R extends StandardSchemaV1 | undefined> = …
  export type EventDescriptor<P extends StandardSchemaV1> = { tags?: (p: InferOutput<P>) => Tag[] }
  ```

  Nothing a zod consumer writes changes. `command({ payload: z.object({ courseId: z.string() }) })` still gives a handler `message.payload: { courseId: string }`, exactly; a wrong payload is still a compile error at the call site; `state({ id: { courseId: z.string() } })` still infers `{ courseId: string }`. That claim is a compile-time test — `packages/core/src/messaging/__tests__/standard-schema.types.ts`, registered in the root `tsconfig.json` `files` array beside the correlation and drizzle probes — which pins the exact inferred types AND accepts a twelve-line hand-written schema object with no library anywhere.

  `zod` moves from `dependencies` to `devDependencies` in `@kronos-ts/core` and `@kronos-ts/test`. The contract itself is VENDORED, types-only, in `messaging/standard-schema.ts` — ninety lines transcribed from the published `@standard-schema/spec`, because a types-only dependency is still a dependency that has to resolve at install time for a package whose runtime never touches it.

  **BREAKING — `zodValidatingSerializer` is gone.** It briefly became `validatingSerializer` during this release cycle, and then serializer-side validation was deleted outright: a serializer encodes, and validation moved to where the descriptor is — see the validation changeset in this same release. `any(schema?)` in `@kronos-ts/test` took the same treatment — a diff is computed and rendered in one breath, so an async schema is reported as the mistake it is.

  ***

  **BREAKING — `Unstamped<M>` and `stamped()` are gone.** `timestamp` is now optional on `Message`, and unset means one thing: this message has not been through a task yet.

  ```ts
  // before
  type Unstamped<M extends Message> = Omit<M, "timestamp"> & { timestamp?: number }
  stamped(message: Unstamped<M>, clock: Clock): M
  dispatch(m: Unstamped<CommandMessage>): Promise<unknown>

  // after
  type Message<P> = { …; readonly timestamp?: number }        // unset = not through a task yet
  type EventMessage<P> = Message<P> & { …; readonly timestamp: number }   // a fact HAS an instant
  dispatch(m: CommandMessage): Promise<unknown>
  ```

  Nothing about WHEN the instant is settled changed. The bus still fills it from `uow.now()` when it mints the unit of work, a transport still fills it from system time at the wire, `ctx.append` still stamps at birth — the stamping is an unexported internal now, and the vocabulary for it is simply not on the surface. `EventMessage` (and therefore `SequencedEventMessage`, and everything a processor hands a handler or a store returns from a read) narrows `timestamp` back to REQUIRED, because a fact you can read has an instant, always.

  A pleasant consequence: `interceptingCommandBus` and `interceptingQueryBus` have no casts left. `Intercept<M> = (m: M) => M` now takes and returns exactly the type the bus takes, because there is no longer a second type for the same message one moment earlier.

  **BREAKING — the `Clock` type is gone.** Every site writes the arrow.

  ```ts
  // before
  export type Clock = () => number
  unitOfWork(clock?: Clock) · testFixture(scope, { clock?: Clock })

  // after
  unitOfWork(clock?: () => number) · testFixture(scope, { clock?: () => number })
  ```

  Same rule that leaves a unit-of-work factory spelled `() => UnitOfWork` and never named: naming a one-arrow type buys an import and hides the one thing the reader needed to see. What the clock MEANS — an instant, epoch milliseconds, the same unit `message.timestamp` carries — now lives on `unitOfWork`'s parameter, which is where it enters.

  ***

  **Upcasting is a mechanism, and it moved to the log boundary.** It is the third one, deliberately the same shape as the other two: `Intercept` is `(message) => message` where a bus hands a message on, `Upcast` is `(event) => event` where the LOG hands an event back.

  ```ts
  // before — a predicate/action method pair, a chain object, and raw JSON at the wire
  type EventUpcaster = {
    canUpcast(type, revision): boolean;
    upcast(rep): rep | rep[];
  };
  upcasterChain(v1ToV2, v2ToV3);
  upcastingSerializer(jsonSerializer(), chain);

  // after — a total function in the DOMAIN form, composed in function space
  type Upcast = (event: EventMessage) => EventMessage; // identity when unconcerned
  upcastingEventStore(store, (e) => v2ToV3(v1ToV2(e)));

  const v1ToV2: Upcast = (e) =>
    is(e, CourseCreatedV1) // the OUTDATED version, as its own descriptor
      ? {
          ...e,
          version: CourseCreated.version,
          payload: { ...e.payload, capacity: 30 },
        }
      : e;
  ```

  `EventUpcaster`, `upcasterChain`, `IntermediateEventRepresentation`, `singleEventUpcaster` and `upcastingSerializer` are all **removed**, and so is the shipped `upcastTo` constructor that briefly replaced them. `canUpcast` was a class in disguise — a predicate method and an action method that had to agree — and totality replaces it: "not mine" is "return it unchanged", so nothing has to be asked. `upcasterChain`'s runtime dispatch over a list is plain composition. And writing the match by hand IS the lesson: `is()` makes it a typed switch, the old shape gets its own descriptor so the compiler knows what `payload` looked like back then, and the target version is read off the CURRENT descriptor, where it already lives, so a version is never written twice and can never disagree with itself.

  The store is the right boundary for four reasons that are one reason from different sides. The serializer never sees the domain form, so an upcaster written there is written against raw JSON and cannot say `event.tags`. An in-memory store has no serializer at all, so serializer-based upcasting silently skipped every test that used one. One placement covers a processor's deliveries (`open()`) and a `ctx.load` fold (`source()`) uniformly. And a validating serializer under an upcaster would judge the 2019 payload against the 2026 schema and reject it before anything could fix it.

  Read paths only — `source()`, `open()`, `subscribe()`. Every write member passes straight through, so what was appended is what is stored, forever; upcasting is a reinterpretation on the way out. Commands and queries need nothing new, because a message crossing versions at a BUS is what `Intercept` already is; only events have a second boundary, because only events are kept.

  ***

  **BREAKING — resilience has left core.** `withRetry`, `healthCheck`, `ResilienceConfig` and `RetryEvent` are no longer exported from `@kronos-ts/core`. Each of the three packages that used them — `@kronos-ts/axon-server`, `@kronos-ts/kronosdb`, `@kronos-ts/postgres` — now owns a package-private `src/resilience.ts`, exported from no barrel.

  Same reasoning as the transaction glue, one folder up: it is `setTimeout` and a loop over a function, it touches no message and no unit of work, core's own `src/` never called it, and by this surface's own first rule a helper is not core. Nothing changes for a host — `kronosDbConnection({ resilience: { maxAttempts: 3 } })` and `postgresPool(url, { resilience })` are the same options with the same defaults. It breaks for anyone who imported the helpers directly, and the fix is to own the hundred lines. `postgres` carries only what it uses: no health probe, because a pool bootstraps and then either works or throws.

  ***

  **`message/` is `messaging/`, and its five declaration files are one.**

  ```
  // before                          // after
  src/message/qualified-name.ts  ┐
  src/message/metadata.ts        │
  src/message/message.ts         ├─  src/messaging/messages.ts
  src/message/descriptor.ts      │
  src/message/namespace.ts       ┘
  src/message/clock.ts               (deleted — the arrow IS the contract)
  src/message/converter.ts           src/messaging/serialization/converter.ts
  src/message/serializer.ts          src/messaging/serialization/serializer.ts
  src/message/upcaster.ts            src/upcasting/upcasting-event-store.ts
  src/message/tag.ts                 src/messaging/tag.ts            (unchanged)
  src/message/identifier.ts          src/messaging/identifier.ts     (unchanged)
  src/message/serialized-error.ts    src/messaging/serialized-error.ts (unchanged)
  src/resilience.ts                  (deleted — three private copies)
  ```

  A qualified name, a metadata map, a message and the descriptor that declares one are not four topics that happen to live near each other; they are the single answer to "what is a message, before any kind picks it up", and splitting them made five imports of one idea. What genuinely stood alone stayed alone. The barrel exports the same names from the same package, so this is invisible unless you were deep-importing into `@kronos-ts/core/src/message/...`, which was never a supported address.

  `messaging/serialization/` is a folder rather than two loose files because a binary serializer is coming and it lands beside these, not on top of them.

  ***

  **`state()` reads in dependency order.** The options are `id · tags · evolve · snapshot? · lifecycle?` now, and every example and test literal is reordered to match. The order is an argument: tags are a function of the id, and `evolve` carries its own seed at position zero.

  ```ts
  // before                                       // after
  state({                                         state({
    id: { courseId: z.string() },                   id: { courseId: z.string() },
    initial: () => ({ capacity: 0 }),               tags: (id) => ({ courseId: id.courseId }),
    tags: (id) => ({ courseId: id.courseId }),      evolve: [() => ({ capacity: 0 }), … ],
    evolve: [ … ],                                })
  })
  ```

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

- 1aef927: **BREAKING — transport buses take the local bus FIRST, connection after.** `rabbitMqCommandBus(rabbit, local)` was the last decorator family violating the standing rule: the decorated thing comes first, config after. A transport wraps YOUR bus — same species as `interceptingCommandBus(bus, intercept)` and `otlpCommandBus(bus, exporter)`, which already lead with the bus.

  ```ts
  // before
  rabbitMqCommandBus(rabbit, local, { preferLocal });
  kronosDbCommandBus(kdb, local);
  axonServerCommandBus(conn, local);

  // after — the wrapped bus leads, everywhere
  rabbitMqCommandBus(local, rabbit, { preferLocal });
  kronosDbCommandBus(local, kdb);
  axonServerCommandBus(local, conn);
  ```

  The parameter is named `next` in every bus wrapper — transports, intercepting, otlp alike — the same word every handler wrapper uses for the thing it wraps. One pattern, whatever the species: take `next`, return the same shape, config trails.

  Stores are unchanged (`kronosDbEventStore(kdb, context)`) — a store is built FROM a connection, not wrapped around a bus, so the resource leads there.

  Also in core: interception moved into its own folder, `core/src/interception/` — it is a mechanism of the setup in its own right, built the same way as everything else (functions wrapping over the configuration, providing new capabilities, typechecked at the composition site), and now it sits beside `correlation/` as a peer. Barrel exports are unchanged.

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

## 0.6.0

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

- 4d7b0ee: Connection is an explicit resource; everything else is a function over it.

  All three transports shipped as a bundle that hid one connection behind a
  `{ components, start, close }` record. A bundle is only legitimate when its
  members share a hidden resource — so the resource is named, and each capability
  becomes a function of it.

  **RabbitMQ.** `rabbitMq(options)` is DELETED. `rabbitMqConnection(url, {
serviceName, instanceId, topology?, retry? })` is the resource — one broker
  connection, its three channels, and the lifecycle — and the buses are plain
  functions over it.

  ```ts
  // before — one bundle, both buses whether you wanted them or not
  const rabbit = await rabbitMq({
    url,
    identity: { serviceName, instanceId },
    localCommandBus: base.commandBus,
    localQueryBus: base.queryBus,
  });

  // after — the resource is named; take only the capabilities you need
  const rabbit = await rabbitMqConnection(url, { serviceName, instanceId });
  const commandBus = interceptingCommandBus(
    rabbitMqCommandBus(rabbit, base.commandBus),
    lineage
  );
  await rabbit.start(); // unchanged: bind-and-consume barrier, after handlers register
  ```

  The old channel-only `amqpConnection(url, connect)` is now `amqpChannelSource`,
  which is what a transport borrows.

  **KronosDB and Axon Server.** `kronosDb(options)` and `axonServer(options)` are
  DELETED — each opened a gRPC channel, a platform stream and a heartbeat PER
  CALL, so a service addressing N contexts paid N connections. `kronosDbConnection`
  / `axonServerConnection` own the one channel.

  `kronosDbContext(kdb, options)` — the four-components-at-once record that sat in
  between — is DELETED too, and so is Axon's `AxonServerBackend`. A context is not
  a thing you build; it is a STRING two functions take:

  ```ts
  // before — one connection per context, then a bundle per context
  const billing = await kronosDb({
    componentName,
    context: "billing",
    serializer,
    unitOfWorkFactory,
  });

  // after — one channel, N contexts, one function per seam
  const kdb = await kronosDbConnection({ componentName, serializer });
  const billingEvents = kronosDbEventStore(kdb, "billing");
  const catalogEvents = kronosDbEventStore(kdb, "catalog");
  await kdb.start(); // ONE readiness barrier, idempotent, covering every context
  ```

  Both still share the one socket — the per-call context header is the whole
  difference — and now the caller who wants only an event store builds only an
  event store.

  The SERIALIZER moves onto the connection options. It is a property of this
  client's wire, not of a context or a bus, and a store keyed by
  `(connection, context)` had nowhere honest to put it. The remaining per-bus
  knobs (flow control, load factor, query timeout, the local-shortcut flag,
  resilience) live in one trailing optional record.

  `start()` keeps the platform-start / subscriptionsAcked ordering and is
  memoised, so N contexts share one barrier. `close()` drains every bus latch
  registered on the connection before stopping the platform stream and the
  channel. `kronosDbControlPlane(kdb, app.processors)` /
  `axonServerControlPlane(conn, app.processors)` take the connection.

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

## 0.5.2

### Patch Changes

- Updated dependencies [2f42ed2]
  - @kronos-ts/messaging@0.11.0
  - @kronos-ts/eventsourcing@0.4.2
  - @kronos-ts/modelling@0.3.2

## 0.5.1

### Patch Changes

- Updated dependencies [9ad1a3c]
  - @kronos-ts/eventsourcing@0.4.1
  - @kronos-ts/messaging@0.10.1
  - @kronos-ts/modelling@0.3.1

## 0.5.0

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
  - @kronos-ts/messaging@0.10.0
  - @kronos-ts/eventsourcing@0.4.0
  - @kronos-ts/modelling@0.3.0

## 0.4.0

### Minor Changes

- 440b453: Event scheduler client for KronosDB's server-side scheduled appends.

  `createKronosDbScheduler(connection, serializer)` exposes `schedule`, `cancel`,
  and `list`. The store appends the event when due — no client-side timers or
  polling — and the schedule is durable once `schedule()` resolves. Supply your
  own token to make retried schedule calls idempotent. gRPC failures map to
  typed errors: `ScheduleAlreadyExistsError`, `ScheduleAlreadyResolvedError`,
  `ScheduleNotFoundError`.

### Patch Changes

- 1c86acb: Reconnect backoff now applies ±25% jitter so scaled-out service instances
  that lose their connection together (server restart, failover) don't
  reconnect as one synchronized wave.

## 0.3.1

### Patch Changes

- f3f9fbc: Publish compiled `dist` entrypoints in npm manifests instead of development-only TypeScript source paths. This makes the packages directly importable in Node.js while retaining concrete versions for workspace dependencies.
- Updated dependencies [f3f9fbc]
  - @kronos-ts/common@0.1.2
  - @kronos-ts/messaging@0.9.2
  - @kronos-ts/modelling@0.2.10
  - @kronos-ts/eventsourcing@0.3.3
  - @kronos-ts/app@0.5.3

## 0.3.0

### Minor Changes

- e71323e: Adopt KronosDB 0.5 batched read wire format (requires server >= 0.5).

  Source and Stream responses now arrive as event batches (`SequencedEventBatch`)
  instead of one event per gRPC message; the client unpacks them transparently,
  so `source()` results and event-processor streams behave exactly as before —
  just faster (server-side batching removes per-message framing overhead on
  whole-log reads and live tailing). Permit-based flow control now counts
  events, not messages. `SourceRequest`/`StreamSubscribe` gain a `batchSize`
  option (0 = server default). Older servers (<= 0.4) are not supported by this
  version of the client.

  Also fixes `generate-proto` to emit `.js` import suffixes so regenerated
  sources pass typecheck under `moduleResolution: node16`.

- 3651f7a: Add a `messaging` option to the `kronosDb()` extension config (default `true`).

  With `messaging: false` the extension populates only the `eventStore` and `snapshotStore` slots, leaving `commandBus`/`queryBus` free for another transport (e.g. the RabbitMQ extension) or the in-memory defaults. The platform control plane (processor pause/start/split/merge, status reporting) stays active in both modes.

## 0.2.10

### Patch Changes

- Updated dependencies [ad944b9]
  - @kronos-ts/messaging@0.9.1
  - @kronos-ts/app@0.5.2
  - @kronos-ts/eventsourcing@0.3.2
  - @kronos-ts/modelling@0.2.9

## 0.2.9

### Patch Changes

- Updated dependencies [9eb84ff]
  - @kronos-ts/messaging@0.9.0
  - @kronos-ts/app@0.5.1
  - @kronos-ts/eventsourcing@0.3.1
  - @kronos-ts/modelling@0.2.8

## 0.2.8

### Patch Changes

- Updated dependencies [56bfb6d]
- Updated dependencies [56bfb6d]
- Updated dependencies [56bfb6d]
  - @kronos-ts/messaging@0.8.0
  - @kronos-ts/app@0.5.0
  - @kronos-ts/eventsourcing@0.3.0
  - @kronos-ts/modelling@0.2.7

## 0.2.7

### Patch Changes

- Updated dependencies [dafdf12]
  - @kronos-ts/messaging@0.7.0
  - @kronos-ts/app@0.4.1
  - @kronos-ts/eventsourcing@0.2.3
  - @kronos-ts/modelling@0.2.6

## 0.2.6

### Patch Changes

- Updated dependencies [291acd2]
- Updated dependencies [291acd2]
  - @kronos-ts/messaging@0.6.0
  - @kronos-ts/app@0.4.0
  - @kronos-ts/eventsourcing@0.2.2
  - @kronos-ts/modelling@0.2.5

## 0.2.5

### Patch Changes

- Updated dependencies [4ac26c0]
  - @kronos-ts/eventsourcing@0.2.1
  - @kronos-ts/app@0.3.4
  - @kronos-ts/messaging@0.5.1
  - @kronos-ts/modelling@0.2.4

## 0.2.4

### Patch Changes

- Updated dependencies [6a3dca4]
  - @kronos-ts/eventsourcing@0.2.0
  - @kronos-ts/messaging@0.5.0
  - @kronos-ts/app@0.3.3
  - @kronos-ts/modelling@0.2.3

## 0.2.3

### Patch Changes

- Updated dependencies [dc0f67e]
- Updated dependencies [f5ed7da]
  - @kronos-ts/messaging@0.4.0
  - @kronos-ts/app@0.3.2
  - @kronos-ts/eventsourcing@0.1.5
  - @kronos-ts/modelling@0.2.2

## 0.2.2

### Patch Changes

- Updated dependencies [4b8faa5]
  - @kronos-ts/messaging@0.3.1
  - @kronos-ts/app@0.3.1
  - @kronos-ts/eventsourcing@0.1.4
  - @kronos-ts/modelling@0.2.1

## 0.2.1

### Patch Changes

- Updated dependencies [c1a1cf5]
- Updated dependencies [74dc43d]
  - @kronos-ts/modelling@0.2.0
  - @kronos-ts/app@0.3.0
  - @kronos-ts/messaging@0.3.0
  - @kronos-ts/eventsourcing@0.1.3

## 0.2.0

### Minor Changes

- Add distributed subscription queries over the QueryService stream. The
  KronosDB server holds the subscriptionId↔handler mapping, so each handler
  tracks the server-injected subscribers it is given and targets emits directly
  by subscriptionIdentifier. `handleSubscriptionQueryRequest` processes inbound
  subscribe (runs the handler, returns initialResult) and unsubscribe;
  `emitUpdate` / `completeSubscription` / `completeSubscriptionExceptionally`
  iterate tracked subscribers, apply the `SubscriptionFilter`, and send
  per-subscriber responses. Function and `payloadEquals` filters both evaluate
  locally on the emitter.

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
