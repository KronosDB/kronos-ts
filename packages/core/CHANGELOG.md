# @kronos-ts/core

## 0.3.0

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

- 1aef927: Three renames-and-fixes to the core surface:

  - `simpleCommandBus` / `simpleQueryBus` are now `localCommandBus` / `localQueryBus` — the same word the transports already use for that argument, so the composition reads as a sentence: `rabbitMqCommandBus(localCommandBus(unitOfWork), rabbit)`. The `inMemory*` prefix stays reserved for stores, where volatility is the fact worth naming.
  - The `lineage` intercept is now `correlation` — it seeds `correlationId`/`causationId`, and the surface already speaks that vocabulary (`correlationData()`, `contributeCorrelationData()`). The word lineage is gone from the codebase.
  - Event processor poll timers are `unref()`d, so an all-in-memory process exits naturally when its work is done instead of being held alive by idle pollers.

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

- 1aef927: States are pure values now. `kronos` registers BEHAVIOUR; data needs no invitation.

  **BREAKING — `kronos` takes three lists.** The `states` field is gone, and with it `StateEntry`, `StateOptions`, `StateManager`, `stateManager()`, `StateManagerLike` and every `Sited<State…>` form. A state was never behaviour: it says which events it folds and how, and the only thing that ever wanted it was a handler that already holds the value.

  ```ts
  // before                                          // after
  kronos({                                           kronos({
    states: [{ ...Course, eventStore }],               commandHandlers: [{ ...openCourse, eventStore, commandBus, queryBus }],
    commandHandlers: [{ ...openCourse, eventStore,     queryHandlers:   [{ ...getCourse, eventStore, queryBus }],
                        commandBus, queryBus }],       eventHandlers:   [{ ...project, commandBus, queryBus, processor }],
  })                                                 })
  ```

  `ctx.load(Course, id)` is unchanged at the call site and needs nothing declared in advance: the state arrives as the argument, the log arrives on the entry's `eventStore`, and that pair IS the fold. Internally `repositoryFor(state, eventStore)` builds it on first use and remembers it in a `WeakMap` keyed on the store object — a CACHE, not a registry. Forget it and you pay a rebuild; forget a registry and `ctx.load` used to throw "No repository registered for state". Entries naming the same `eventStore` object still share their folds, for the reason they always did: it is the same object, and nobody had to say so.

  The doctrine that entries sharing a log "share a repository set" is retired with the grouping code it described. `kronos.ts` lost `LogGroup`, `groupFor`, `PartitionedState`, `requireSnapshotName` and the whole per-log resolution pass; snapshot store and tag resolver now ride per entry, where an entry that wants its own simply says so.

  ***

  **BREAKING — the seed joined the fold. `state({ initial })` is gone.** `evolve` is one tuple whose FIRST element is the seed:

  ```ts
  // before                                          // after
  const Course = state({                             const Course = state({
    id: { courseId: z.string() },                      id: { courseId: z.string() },
    tags: (id) => ({ courseId: id.courseId }),         tags: (id) => ({ courseId: id.courseId }),
    initial: () => ({ capacity: 0, taken: 0 }),        evolve: [
    evolve: [                                            () => ({ capacity: 0, taken: 0 }),
      [CourseCreated, (s, { payload }) =>                 [CourseCreated, (s, { payload }) =>
        ({ ...s, capacity: payload.capacity })],            ({ ...s, capacity: payload.capacity })],
      [StudentSubscribed, (s) =>                          [StudentSubscribed, (s) =>
        ({ ...s, taken: s.taken + 1 })],                    ({ ...s, taken: s.taken + 1 })],
    ],                                                 ],
  })                                                 })
  ```

  The fold is `cases.reduce(...)` seeded by `evolve[0]` — the seed is the evolver of nothing, so it belongs in the same list as the evolvers of something rather than in a field beside it. The grammar is POSITIONAL and statically typed: element zero, always. `state()` destructures it once (`const [initial, ...cases] = evolve`) and nothing downstream ever asks which shape an element is — no `Array.isArray`, no union to narrow, none of the dance this codebase spent a release deleting.

  It is also what fixes `S`, which is why the per-case inference is unchanged: the seed takes no arguments, so TypeScript resolves it first, and every case is then checked against THAT `S` and against ITS OWN descriptor. A wrong `msg.payload` access or a wrong return value is still reported at that case, not at the array. The DCB query derivation reads the descriptors off `evolve.slice(1)`; the tag-key intersection, the unmatchable-fold boot error and the multi-stream OR all behave exactly as before.

  The seed takes NO id. `initial: (id) => …` could read the id it was being created for; `() => S` cannot, because the evolver of nothing has nothing to read. A fold that needs to know which entity it is learns it from the event that created it — the query is already scoped to that id, so the only event of that type it can see is the right one.

  `State.create` is `State.initial` for the same reason: the signature changed, so the name had to.

  ***

  **Snapshot configuration rides on the state value.** `state({ name, …, snapshot: afterEvents(50) })`. How often a state snapshots is a property of its event volume, so it belongs to the state; WHERE a snapshot lands is a deployment fact, so `snapshotStore` stays a site property on the entry. A snapshot is read or written only when BOTH halves are present.

  ```ts
  // before                                          // after
  kronos({ states: [                                 const Course = state({
    [{ ...Course, eventStore, snapshotStore },         name: "Course",
     { snapshotPolicy: afterEvents(50) }],             …,
  ]})                                                  snapshot: afterEvents(50),
                                                     })
                                                     kronos({ commandHandlers: [
                                                       { ...openCourse, eventStore, snapshotStore, … },
                                                     ]})
  ```

  `snapshot` without `name` is a CONSTRUCTION error now, thrown by `state()` and naming the events the state folds — a snapshot with nowhere durable to be written is not a thing to discover at boot. `eventSourcedRepository(state, eventStore)` lost its trailing policy parameter; it reads `state.snapshot`.

  ***

  **`is()` is one guard for all three message kinds.**

  ```ts
  is<D extends MessageDescriptor>(message: Message, descriptor: D): message is <the message type for D>
  ```

  Kinds equal, qualified names equal, and — for an EVENT, the only kind that carries a version on the message — versions equal. A command or a query is a request in flight rather than a stored fact, so its descriptor's version is declaration-side only and there is nothing on the message to compare it against. The narrowing maps the descriptor to its own message type with the payload inferred off its schema: `CommandDescriptor → CommandMessage<…>`, `QueryDescriptor → QueryMessage<…>`, `EventDescriptor → EventMessage<…>`.

  "Is this message that message type" was always one question; it had an event-only answer because upcasting asked it first.

  ***

  **BREAKING — `upcastTo` is gone.** `is()` replaced it, and writing the match by hand IS the documented idiom:

  ```ts
  const CourseCreatedV1 = ns.event("CourseCreated", {
    version: "1.0",
    payload: z.object({ courseId: z.string(), name: z.string() }), // no capacity back then
    tags: { courseId: (p) => p.courseId },
  });

  const capacityAdded: Upcast = (e) =>
    is(e, CourseCreatedV1)
      ? {
          ...e,
          version: CourseCreated.version,
          payload: { ...e.payload, capacity: 30 },
        }
      : e;
  ```

  Declare the outdated version as its own descriptor and the compiler knows what `payload` looked like back then. The target version is read off the CURRENT descriptor, never restated. Plurality composes in function space, as before: `upcastingEventStore(store, (e) => v3(v2(v1(e))))`.

  ***

  **`@kronos-ts/test`: `FixtureLists` loses `states`.** A scope that returned them stops; the fixture wires nothing for them, because there is nothing to wire. Everything else about a scope is unchanged — it is still a composition root that takes `(eventStore, snapshotStore)` and hands back the lists a process would deploy.

  `event-sourcing/manager.ts` is gone; `LoadResult` and `StateRepository` moved into `repository.ts`, which is where the fold and its cache both live, and a file named for a manager that no longer exists would have been a lie.

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

- 1aef927: Two layers of the sourcing story: `state()` DERIVES the query, `ctx.source` lets you WRITE it — and the seed reads its identity again.

  **`ctx.source(query)` — the raw layer.** A new capability on all three read-bearing contexts.

  ```ts
  source(query: EventQuery): Promise<ReadonlyArray<EventMessage>>
  ```

  `state()` writes a query for you: one per folded event type, derived from the tag record and the fold, and the SAME query becomes the append condition. This is the layer under that. You write the query, you run the fold, and what comes back is the matching events in stream order — the input a fold takes.

  ```ts
  // before — the raw read was a store call, and a store call is not a decision
  const { events } = await eventStore.source({ query: { tags: { courseId } } });
  //    ^ nothing about this read reaches ctx.append. The write is unconditioned.

  // after
  const events = await ctx.source({
    tags: { courseId },
    types: [CourseCreated, StudentSubscribed],
  });
  const course = events.reduce(
    (s, e) => {
      if (is(e, CourseCreated)) return { ...s, capacity: e.payload.capacity };
      if (is(e, StudentSubscribed)) return { ...s, taken: s.taken + 1 };
      return s;
    },
    { capacity: 0, taken: 0 }
  );
  if (course.taken >= course.capacity) throw new Error("course is full");
  ctx.append(StudentSubscribed, payload); // conditioned on that very read
  ```

  **The last line is the whole point.** `ctx.source` records what it read onto the task — the query, and the position it read up to — through the same bookkeeping `ctx.load` goes through, and the PREPARE_COMMIT flush turns those entries into the append condition. So a hand-rolled `is()` + `reduce` has the IDENTICAL DCB optimistic-concurrency guarantee a `state()` fold has: another task appending a matching event between this read and this write makes this write fail. A raw fold is a first-class decision, not an escape hatch that gives one up.

  The query is the plain data it always was: within an item `types` is an any-of (descriptors, qualified names or strings) and `tags` an all-of; an array of items is an OR. **Declaring `types` NARROWS the conflict window**; omitting it is legal and means "every event carrying these tags", which is wider and conflicts more often. That narrowing is one of the things the state derivation was doing on your behalf.

  It reads the ENTRY's store — the same object `ctx.load` reads — so a store composed with `upcastingEventStore` hands this layer upcasted events too. It is on the query context as a pure read (there is no `append` there to condition) and on the event context likewise. Nothing new is exported: `source` is a capability you reach through `ctx`, not a free function.

  ***

  **The seed takes the id.** `evolve[0]` is handed the state's inferred id — the same record `tags` takes.

  ```ts
  // before                                          // after
  const Subscription = state({                       const Subscription = state({
    id: { courseId: z.string(),                        id: { courseId: z.string(),
          studentId: z.string() },                           studentId: z.string() },
    tags: (id) => ({ ...id }),                         tags: (id) => ({ ...id }),
    evolve: [                                          evolve: [
      () => ({ courseId: "", taken: 0 }),                (id) => ({ courseId: id.courseId, taken: 0 }),
      [CourseCreated, (s, { payload }) =>                [CourseCreated, (s, { payload }) =>
        ({ ...s, courseId: payload.courseId })],           ({ ...s, capacity: payload.capacity })],
      //  ^ which course is LEARNED from an event      //  ^ which course was never in question
    ],                                                 ],
  })                                                 })
  ```

  Being the evolver of nothing is not the same as being the knower of nothing. No event has been folded yet, so the identity is the one thing a zeroth state can honestly know — and a fold that carries its own key stops having to wait for an event to tell it what it already is. The previous release said the seed takes no id because `() => S` had nothing to read; that was a statement about the signature, not about the fold, and the signature was the thing that was wrong.

  **Nothing breaks.** A zero-parameter seed literal stays assignable to `(id) => S` by TypeScript's arity rule, so every existing `evolve: [() => ({ … }), …]` compiles untouched and most folds still never mention an id. What changed for a caller is `State.initial`, which now takes one: `Course.initial({ courseId })`.

  Per-case inference is unchanged, and that is the load-bearing claim. An id-reading seed literal is a CONTEXT-SENSITIVE expression, and a context-sensitive element zero is exactly what the naive spellings of `EvolveTuple` cannot survive — the inference variable gets fixed to its constraint and every case's descriptor goes with it, silently, leaving `payload` as `any` while the folds keep running. Element zero is therefore an INTERSECTION: `((id: Id) => S) & E[K]`. The first half types the seed's `id` and gives `S` an inference site; the second keeps the reverse-mapping — and the cases — alive. `EvolveShape` leaves position zero open (`seed: unknown`) for the same reason, since a constraint that demands a function there is what collapses the tuple; what a seed must be is stated in `EvolveTuple`, which is the type the argument is actually checked against. A new type-level probe, `event-sourcing/__tests__/state-initial.types.ts`, is listed in the root `tsconfig.json` `files` array so the claim is judged by `tsc` rather than asserted in a comment.

  ***

  **`ctx.load` and `ctx.source` share their bookkeeping.** `event-sourcing/load.ts` now holds both reads and the two internals they have in common: `requireLog`, which is the one "no `eventStore` on this entry" error however it was noticed, and `recordSourcing`, the ONLY route from a read to an append condition. That the two reads are the same guarantee is now a fact about the code rather than a claim about two implementations.

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

- 1aef927: Validation is a mechanism you compose, and it needs no registry — the descriptor is already in your hand at every site that validates.

  `validation/` joins `interception/`, `correlation/` and `upcasting/` as the fourth mechanism, and like them it is functions you wrap in rather than infrastructure you configure. It is two exports:

  ```ts
  validate(descriptor, payload); // the primitive — anywhere a descriptor is in hand
  validatingHandler(next, descriptor); // the mechanism — inbound, and every ctx birth
  ```

  **Why there is nothing to register.** A descriptor carries its own payload schema, and it is an ARGUMENT at every face except the one where it is born: the edge verbs take one (`send(bus, descriptor, payload)`), the birth verbs take one (`ctx.append(descriptor, payload)`), an entry pairs one with its handler. A schema registry exists to answer "which schema goes with this type name" — a question that only arises somewhere holding a name and not a descriptor, which was the serializer and nowhere else. So the question stopped being asked.

  **`validatingHandler` composes at the ENTRY**, the one place a descriptor and a handler already sit together:

  ```ts
  kronos({
    commandHandlers: handlers
      .map((h) => ({
        ...h,
        handler: validatingHandler(h.handler, h.descriptor),
      }))
      .map((h) => ({ ...h, commandBus, queryBus, eventStore })),
  });
  ```

  Wrapped, a handling is gated in both directions. INBOUND, the message's payload is checked against the entry's own descriptor — a message off a wire is a claim, not a fact, and a transport that decoded JSON knows the shape parsed, not that it is a command this handler can act on. OUTBOUND, the context's birth verbs are overlaid exactly the way `correlatingHandler` overlays them (only the verbs the context has, so one wrapper serves all three kinds), and each verb validates against the descriptor IT was called with — `append`'s batch form per tuple.

  The write side is the one that matters most:

  ```ts
  // before — the log's guard was a registry entry somebody had to remember to add
  validatingSerializer(jsonSerializer(), registry); // …and registry.register("Charged", "1.0", schema)

  // after — the guard is the descriptor the verb was called with
  ctx.append(Charged, { accountId }); // ← missing `amount`: throws HERE, before the store
  //   sees it, not in a replay years from now
  ```

  **The parsed value replaces the input, on both paths.** Standard validation is a parse — coercions, defaults and transforms are part of what a schema says — so `next` is handed a message whose payload is what the schema produced, and each wrapped verb gives birth to the produced value rather than the one the caller passed. Dropping that value would keep the check and throw away half of it.

  The sync/async split falls out of the verbs. `ctx.send` and `ctx.query` already answer promises, so a schema that validates asynchronously is simply awaited. `ctx.append` returns `void` and `ctx.schedule` builds its message in the caller's turn, so an async schema there throws, naming the message type and the verb — the same rule (and the same reason) the serializer had, one boundary over. Validation asks the context for no type it did not already have, so unlike `correlatingHandler` it adds no demand: a wrapped handler wires against exactly the buses the unwrapped one did, and it stacks with `correlatingHandler` and `otlpHandler` in either order.

  **And the edge is yours.** The edge verbs are unclosed on purpose, and a host closes them out once — documented, never exported, because it is two lines and they are the host's two lines:

  ```ts
  export const dispatch = (d, payload, actor) =>
    send(commandBus, d, validate(d, payload), { actor });
  export const ask = (d, payload, actor) =>
    query(queryBus, d, validate(d, payload), { actor });
  ```

  Controllers never see the unclosed verb, so validation and per-request metadata are one visible decision in one file.

  **BREAKING — `validatingSerializer` is gone, and so is the whole registry apparatus.** `SchemaRegistry`, the `.register()` ceremony, and `eventSchemaRegistry()` / `commandSchemaRegistry()` / `querySchemaRegistry()` are deleted from `messaging/serialization/` and from the barrel. A serializer ENCODES; `jsonSerializer()` is what is left, and it does one thing.

  ```ts
  // before
  const registry = eventSchemaRegistry();
  registry.register("Charged", "1.0", ChargedSchema);
  const serializer = validatingSerializer(jsonSerializer(), registry);
  postgresEventStore(pg, { serializer, tagResolver });

  // after
  postgresEventStore(pg, { serializer: jsonSerializer(), tagResolver });
  kronos({
    commandHandlers: handlers.map((h) => ({
      ...h,
      handler: validatingHandler(h.handler, h.descriptor),
    })),
  });
  ```

  **BREAKING — `MessageDescriptor` is widened to `CommandDescriptor<any, any> | EventDescriptor<any> | QueryDescriptor<any, any>`.** It was the defaulted spelling, and a descriptor CARRIES A FUNCTION: an event's `tags` extractor takes the payload, so it is checked contravariantly and `EventDescriptor<{ courseId: string }>` could not stand in for `EventDescriptor<StandardSchemaV1>` — nor could a descriptor with a `result` schema stand in for the `undefined` default. Between them that ruled out every descriptor anybody actually declares, which `is()` had been quietly carrying:

  ```ts
  // before — did not compile, for any event with tags or any command with a result
  is(message, CourseCreated);

  // after — compiles, and narrows to the exact payload
  if (is(message, CourseCreated)) message.payload.courseId;
  ```

  The widening is the CONSTRAINT, never the value: every function taking a descriptor takes it as `D extends MessageDescriptor` and reads the real payload type back off `D`, so `validate(CreateCourse, body)` returns the exact parsed object and `is()` narrows exactly as it always promised to.

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
