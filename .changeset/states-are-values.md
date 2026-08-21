---
"@kronos-ts/core": minor
"@kronos-ts/test": minor
---

States are pure values now. `kronos` registers BEHAVIOUR; data needs no invitation.

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

---

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

---

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

---

**`is()` is one guard for all three message kinds.**

```ts
is<D extends MessageDescriptor>(message: Message, descriptor: D): message is <the message type for D>
```

Kinds equal, qualified names equal, and — for an EVENT, the only kind that carries a version on the message — versions equal. A command or a query is a request in flight rather than a stored fact, so its descriptor's version is declaration-side only and there is nothing on the message to compare it against. The narrowing maps the descriptor to its own message type with the payload inferred off its schema: `CommandDescriptor → CommandMessage<…>`, `QueryDescriptor → QueryMessage<…>`, `EventDescriptor → EventMessage<…>`.

"Is this message that message type" was always one question; it had an event-only answer because upcasting asked it first.

---

**BREAKING — `upcastTo` is gone.** `is()` replaced it, and writing the match by hand IS the documented idiom:

```ts
const CourseCreatedV1 = ns.event("CourseCreated", {
  version: "1.0",
  payload: z.object({ courseId: z.string(), name: z.string() }),   // no capacity back then
  tags: { courseId: (p) => p.courseId },
})

const capacityAdded: Upcast = (e) =>
  is(e, CourseCreatedV1)
    ? { ...e, version: CourseCreated.version, payload: { ...e.payload, capacity: 30 } }
    : e
```

Declare the outdated version as its own descriptor and the compiler knows what `payload` looked like back then. The target version is read off the CURRENT descriptor, never restated. Plurality composes in function space, as before: `upcastingEventStore(store, (e) => v3(v2(v1(e))))`.

---

**`@kronos-ts/test`: `FixtureLists` loses `states`.** A scope that returned them stops; the fixture wires nothing for them, because there is nothing to wire. Everything else about a scope is unchanged — it is still a composition root that takes `(eventStore, snapshotStore)` and hands back the lists a process would deploy.

`event-sourcing/manager.ts` is gone; `LoadResult` and `StateRepository` moved into `repository.ts`, which is where the fold and its cache both live, and a file named for a manager that no longer exists would have been a lie.
