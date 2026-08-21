---
"@kronos-ts/core": minor
---

Two layers of the sourcing story: `state()` DERIVES the query, `ctx.source` lets you WRITE it — and the seed reads its identity again.

**`ctx.source(query)` — the raw layer.** A new capability on all three read-bearing contexts.

```ts
source(query: EventQuery): Promise<ReadonlyArray<EventMessage>>
```

`state()` writes a query for you: one per folded event type, derived from the tag record and the fold, and the SAME query becomes the append condition. This is the layer under that. You write the query, you run the fold, and what comes back is the matching events in stream order — the input a fold takes.

```ts
// before — the raw read was a store call, and a store call is not a decision
const { events } = await eventStore.source({ query: { tags: { courseId } } })
//    ^ nothing about this read reaches ctx.append. The write is unconditioned.

// after
const events = await ctx.source({ tags: { courseId }, types: [CourseCreated, StudentSubscribed] })
const course = events.reduce(
  (s, e) => {
    if (is(e, CourseCreated))     return { ...s, capacity: e.payload.capacity }
    if (is(e, StudentSubscribed)) return { ...s, taken: s.taken + 1 }
    return s
  },
  { capacity: 0, taken: 0 },
)
if (course.taken >= course.capacity) throw new Error("course is full")
ctx.append(StudentSubscribed, payload)            // conditioned on that very read
```

**The last line is the whole point.** `ctx.source` records what it read onto the task — the query, and the position it read up to — through the same bookkeeping `ctx.load` goes through, and the PREPARE_COMMIT flush turns those entries into the append condition. So a hand-rolled `is()` + `reduce` has the IDENTICAL DCB optimistic-concurrency guarantee a `state()` fold has: another task appending a matching event between this read and this write makes this write fail. A raw fold is a first-class decision, not an escape hatch that gives one up.

The query is the plain data it always was: within an item `types` is an any-of (descriptors, qualified names or strings) and `tags` an all-of; an array of items is an OR. **Declaring `types` NARROWS the conflict window**; omitting it is legal and means "every event carrying these tags", which is wider and conflicts more often. That narrowing is one of the things the state derivation was doing on your behalf.

It reads the ENTRY's store — the same object `ctx.load` reads — so a store composed with `upcastingEventStore` hands this layer upcasted events too. It is on the query context as a pure read (there is no `append` there to condition) and on the event context likewise. Nothing new is exported: `source` is a capability you reach through `ctx`, not a free function.

---

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

---

**`ctx.load` and `ctx.source` share their bookkeeping.** `event-sourcing/load.ts` now holds both reads and the two internals they have in common: `requireLog`, which is the one "no `eventStore` on this entry" error however it was noticed, and `recordSourcing`, the ONLY route from a read to an append condition. That the two reads are the same guarantee is now a fact about the code rather than a claim about two implementations.
