---
"@kronos-ts/core": minor
"@kronos-ts/postgres": patch
"@kronos-ts/kronosdb": patch
"@kronos-ts/axon-server": patch
---

A state says what it is scoped BY. The query is derived.

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
tags: (id) => [{ courseId: id.courseId }, { studentId: id.studentId }]
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
eventStore.source({ criteria: eventQuery({ tags: { courseId } }) })
eventStore.append(events, { criteria, marker })
Course.criteria({ courseId })

// after
eventStore.source({ query: { tags: { courseId } } })
eventStore.append(events, { query, marker })
Course.query({ courseId })
```

A `commandHandler`'s `appendCondition` override now receives and returns an
`EventQuery` rather than an `EventCriteria`:

```ts
// before
appendCondition: (command, sourcedCriteria) => EventCriteria.havingAnyTag()
// after
appendCondition: (command, sourcedQuery) => ({ tags: { billId: command.payload.billId } })
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
