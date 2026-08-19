---
"@kronos-ts/core": minor
---

State queries are derived PER EVENT TYPE, not once for the whole state.

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
  { tags: { courseId: "cs-101" }, types: ["CourseCreated", "StudentSubscribedToCourse"] },
  { tags: { studentId: "stu-1" }, types: ["StudentEnrolledInFaculty", "StudentSubscribedToCourse"] },
]
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
})

// after — keys are data; `tagKeys` is derived as ["courseId", "studentId"]
event({
  name: qn("university", "StudentSubscribedToCourse"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: { courseId: (p) => p.courseId, studentId: (p) => p.studentId },
})
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
})
```

An event with no `tags` at all declares the EMPTY key set rather than an unknown
one, so it is caught by the shared-key check like any other unmatchable fold.
Passing `tagKeys` alongside a `tags` record throws — the two cannot disagree.
