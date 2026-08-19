---
"@kronos-ts/test": minor
---

A test is a VALUE, and the fixture is the SITE it runs at.

The triple-record fixture said the right thing and typed the wrong one: a record
has no order, so it could not say whether a wait came before or after the act; a
`[descriptor, payload]` tuple could not carry metadata or a hole; and `then` had
two shapes (a list, or a record of three fields) with a rule about which one you
were allowed to use. The vocabulary is now value constructors, and the grammar is
a pipe — because order is real at the joints and nowhere else.

```ts
// before
await testFixture(courseSlice).run({
  given: [[CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 1 }]],
  when:  [SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" }],
  then:  { error: "Course is full", events: [] },
})

// after
await testFixture(courses).run(
  given(event(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 1 }))
    .when(command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" }))
    .then(error("Course is full"), noEvents()),
)
```

## The vocabulary

`event(descriptor, payload, metadata?)`, `command(...)` and `query(...)` name a
message. The same three name an EXPECTATION, because "the event
StudentSubscribed with this payload" is one idea whether you are stating it as
history or claiming it as a consequence. Payloads are compile-checked against
their descriptor; `any(schema?)` is the positional hole for a field that is not
the test's business, and it renders as `*` in a diff.

The rest of `then` is `result(value)`, `error(matcher)`, `noEvents()`,
`noCommands()`, `scheduled(event(...), duration)` and `cancelled(event(...))`.
Each CATEGORY is judged only if the scenario mentions it. `error` takes a string
(substring), a RegExp, or a predicate — `(e) => e instanceof CourseFull` replaces
the error-class special case, and `error(() => true)` replaces `error: true`.

## The grammar

```ts
given(...events)          // or scenario(); given() empty is the same empty world
  .wait(duration)         // chainable at any joint, repeatable
  .when(action)           // EXACTLY ONE command | query | event — the type has no second `when`
  .wait(duration)
  .then(...assertions)    // terminal → a Scenario value
```

Every step returns a NEW value, so one prefix finishes several ways and a
finished `Scenario` — which carries a derived `description` like
`"given CourseCreated, when SubscribeStudent, then StudentSubscribed"` — runs
against as many fixtures as you like. Running one scenario against two scopes is
now a two-line test.

## The site

```ts
// before — the fixture took the lists and REPLACED whatever site they carried
testFixture(courseSlice)

// after — the fixture creates the resources and the scope is a function of them
testFixture((eventStore, snapshotStore) => courses(eventStore, snapshotStore))
testFixture(courses)      // the same function a process deploys
```

The fixture owns a recording event store, a snapshot store, a token store, a
dead-letter queue, both recording buses, one clock and a `controllableScheduler`
sharing it. It calls the scope full-handed (a shorter parameter list declines the
rest) and completes any PARTIAL processor an event-handler entry carries —
`(eventStore, tokenStore, unitOfWork, deadLetterQueue) => EventProcessor` — which
is the slice idiom typed: the slice closes out its own semantics, the site
supplies the resources.

Semantics, pinned:

- `given` events APPEND to the log and every cursor FAST-FORWARDS past them
  without invoking a handler. History is history; the automations that would have
  fired already fired, long ago. The old fixture let them replay, which made the
  world one the test never described.
- `when(event)` means the event ARRIVES — appended, and the processors DO react.
  That is the automation shape, and it was unsayable before.
- `wait(d)` jumps the clock, fires the schedules now due in FIRE-TIME order, and
  quiesces. A fired event is stamped with the instant it FIRES, not the instant it
  was arranged. Against a scope whose resources the fixture does not own, `wait`
  throws a clear error unless `{ realTime: true }`, where it genuinely elapses.
- `event()`/`noEvents()` is the EXACT ORDERED list of new events; `command()`/
  `noCommands()` the same over dispatches, with the act's own command excluded.
  `metadata` on a then-value is a subset claim over the keys it names.
- Real-infrastructure scopes are re-judged until `opts.within` (default 5s). An
  all-in-memory scope is judged ONCE — it is deterministic, so waiting for it
  would only make failures slow.
- Failure is a `ScenarioAssertionError` whose message IS the diff: the scenario's
  own sentence, both lists in full, names aligned by longest common subsequence,
  and a field-level payload diff on the pairs that lined up.

## Recorders

Thing-first decorators now — each takes what it records and returns the same
shape plus a readable log and a `reset()`:

```ts
// before
recordingEventStore(): EventStore & { appended }        // was itself a store

// after
recordingEventStore(store): EventStore & { appended; reset() }
recordingCommandBus(bus):   CommandBus & { dispatched; reset() }
recordingQueryBus(bus):     QueryBus   & { queried;    reset() }
controllableScheduler(clock): EventScheduler & { schedules; due(); reset() }
```

`controllableScheduler` has no timer and no sink: `due()` hands back the events
whose fire-time has arrived, and whoever moved the clock decides where they go.
That is the difference between a deadline test that takes thirty seconds and one
that takes a millisecond.

Unit level still needs no fixture at all: folds are reduces over `evolve` tuples,
handlers are functions called with an inline ctx record.
