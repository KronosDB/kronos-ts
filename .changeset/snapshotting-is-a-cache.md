---
"@kronos-ts/core": minor
"@kronos-ts/postgres": minor
"@kronos-ts/axon-server": minor
"@kronos-ts/kronosdb": minor
"@kronos-ts/otlp": minor
"@kronos-ts/test": minor
---

**Snapshotting is not a mechanism and not a seam. It is a CAPABILITY TIER on the event store, and the compiler makes you wire it.**

It used to be the fifth mechanism, with a `SnapshotStore` seam beside the log, a `snapshotStore` field on every entry, and a generic decorator whose only job was marrying incapable stores to that separate seam. All of it is gone. There are **four** mechanisms — `interception/`, `correlation/`, `upcasting/`, `validation/` — and snapshotting is not among them, because a mechanism is a wrap-in that lives in core and serves every backend identically, and this cannot be: fusing a cache lookup into a read is a property of the **store you are reading from**, and the store families live in their own packages.

---

**THE BASE CONTRACT MENTIONS SNAPSHOTS NOWHERE, AND IT IS COMPLETE WITHOUT THEM.**

A log you can source, append, stream and subscribe to is everything the DCB model needs, and most well-designed projects never need one line more. If snapshotting exists at all, it exists **on the event store**, added by wrapping one.

```ts
// before — two objects, two fields, and a host who could wire half of it
const eventStore    = postgresEventStore(pg, { serializer, tagResolver })
const snapshotStore = postgresSnapshotStore(pg, { serializer })
kronos({ commandHandlers: h.map((x) => ({ ...x, eventStore, snapshotStore })) })

// after — ONE object, one field, one serializer
const eventStore = postgresSnapshottingEventStore(
  postgresEventStore(pg, { tagResolver }),
  pg,
  { serializer },
)
kronos({ commandHandlers: h.map((x) => ({ ...x, eventStore })) })
```

```ts
// the capability, and the ONE member it adds
type SnapshotCapability = {
  storeSnapshot(key: string, snapshot: Snapshot, uow?: UnitOfWork): Promise<void>
}
type SnapshotCapableEventStore = EventStore & SnapshotCapability
```

There is no `loadSnapshot`, because **reading is not a second call**: a capable store honours `condition.snapshot` inside `source()` and leads its `SourcingResult` with the cached fold. The read was already in `EventStore`'s shape; only the write needed a name.

---

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

---

**FOUR WRAPPERS, ONE PER FAMILY. The generic decorator and all four snapshot stores are gone.**

```ts
inMemorySnapshottingEventStore<E extends EventStore>(next: E): E & SnapshotCapability
postgresSnapshottingEventStore<E extends EventStore>(next: E, pg, { serializer }): E & SnapshotCapability
kronosDbSnapshottingEventStore<E extends EventStore>(next: E, kdb, context?): E & SnapshotCapability
axonServerSnapshottingEventStore<E extends EventStore>(next: E, conn, context): E & SnapshotCapability
```

**Postgres fuses in ONE round trip** because it holds the connection: the wrapper absorbed `postgresSnapshotStore`'s upsert **and** the CTE that used to live natively in `postgresEventStore`, so the base store is now snapshot-free — `PostgresEventStoreConfig` loses `serializer` entirely, and the wrapper has the only one. **The two-serializer footgun is gone with it:** one function now writes the bytes it later reads.

**KronosDB fuses in ONE round trip too, natively** — see below. **The other two fuse client-side** — `getLast`/`Map` lookup, then a source after its position, inside the one function. Two calls where postgres needs one, which is a difference in what a wrapper can **reach**, not in what the capability **means**. Axon Server's `SnapshottedDcbEventStore.Source` is `UNIMPLEMENTED` on `2025.2.5` **and** `2026.0.4` (`DcbSnapshotStore/GetLast` answers on both). When it lands it changes one function body and **no host's code**, because the capability was never a promise about round trips.

---

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
expect(fused.events.length).toBe(1)                        // native path: returned
expect(offByOne.events.length).toBe(0)                     // `position + 1`: dropped
```

`storeSnapshot` stays **fire-and-forget**, by contract: the record is appended after the transaction it summarizes commits, and is not enlisted in it. `snapshot.position` crosses the wire **unmodified** in both directions — no arithmetic on either side.

The server also exposes `GetSnapshot` (the latest entry alone, for adapters that load snapshots separately). It is **deliberately not wired**: reading a cached fold is not a second call, and `SnapshottedSource` is the path this capability means.

**BREAKING for `@kronos-ts/kronosdb` — `SnapshotStoreDefinition` is removed**, along with `proto/snapshot.proto`, its generated module, and the `snapshotStore` client on `KronosDbConnection`. They addressed a service the server no longer runs, so keeping them would only let a host wire a client that cannot connect. `kronosDbServiceDefinitions` loses its `snapshotStore` entry. Nothing that goes through `kronosDbSnapshottingEventStore` is affected — the capability is unchanged; only the transport under it moved.

---

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

---

**BREAKING — the `snapshotStore` entry field is removed.**

`HandlerSite`, `CommandInvocationDeps`, `HandlerContextDeps`, `ProcessorHandlerEntry`, `subscribeQueryHandlers`'s deps and every doc site lose it. One store object per entry, capabilities and all. `eventSourcedRepository(state, eventStore)` and `repositoryFor(state, eventStore)` lose their trailing parameter, and the per-site repository cache drops from three levels to two — there is one object to key on now, which is what it was always really keyed on.

**BREAKING for `@kronos-ts/test` — the fixture scope takes ONE store.**

```ts
// before
type FixtureScope = (eventStore: EventStore, snapshotStore: SnapshotStore) => FixtureLists
testFixture((eventStore, snapshotStore) => courses(eventStore, snapshotStore))

// after
type FixtureScope = (eventStore: FixtureEventStore) => FixtureLists
testFixture((eventStore) => courses(eventStore))
```

`FixtureEventStore` is the one object the fixture owns: in-memory, recording, and snapshot-capable. The fixture composes what a host composes — `recordingEventStore(inMemorySnapshottingEventStore(inMemoryEventStore()))`, recorder outermost so `appended` is still what left the fixture — and because both wrappers are additive, the capability survives the layer above the store that has it. `PartialProcessor`'s first parameter is that same store.

Migration for a scope: delete the second parameter, and delete `snapshotStore` from the entries it spread. If your states declare snapshot policies, that is all — the fixture's log already serves them, and the compiler will tell you if it does not.

---

**Everything the mechanism MEANT is unchanged.**

Latest-only; never migrated; never load-bearing. The key is a string you wrote, and changing it is the whole invalidation story. `state({ snapshot: { key, when } })` is still sugar over the raw pair — which is now two members of one object:

```ts
const key = `course:${courseId}`
const { snapshot, events, position } = await ctx.source(query, { snapshot: key })
const state = events.reduce(fold, (snapshot?.state as S) ?? initial)
if (events.length > 100) await eventStore.storeSnapshot(key, { state, position })
```

Fusing still does not narrow the append condition. The leading snapshot is still its own field on `SourcingResult` and still not an event. The read still belongs to the store and the write still belongs to the fold, fire-and-forget with its failure swallowed. Structural fitness is still the safety net under the key, judged once in core against `initial(id)` for every backend. `Snapshot`, `SnapshotPolicy`, `SnapshotConfig`, `afterEvents`, `whenSourcingTimeExceeds`, `noSnapshotPolicy`, `snapshotIdentifier` and `matchesInitialStructure` all keep their shapes; they moved from `snapshotting/` into `event-sourcing/`, beside the fold that asks for them.

`withoutSnapshotKey(condition)` is newly exported from core — the four wrappers all need it and none of them owns it.
