---
"@kronos-ts/test": minor
---

The fixture takes your infrastructure instead of conjuring its own, so a test
can run against the arrangement it is going to ship on. BREAKING: a scope now
receives every resource, not just the log.

```ts
// before — the fixture built the buses, the task and the log; a scope got one of them
testFixture((eventStore) => ({ commandHandlers: [{ ...openCourse, eventStore }] }))

// after — the scope is handed the whole arrangement, already wrapped for recording
testFixture(({ eventStore, commandBus, queryBus }) => ({ … }), {
  infrastructure: (task) => {
    const uow = postgresUnitOfWork(task, pg)
    return {
      unitOfWork: uow,
      eventStore: postgresSnapshottingEventStore(postgresEventStore(pg, …), pg, …),
      commandBus: localCommandBus(uow),
      queryBus: localQueryBus(uow),
      tokenStore: postgresTokenStore(pg),
    }
  },
})
```

Omit `infrastructure` and the fixture builds the in-memory stack it always did.

**The factory is handed a task, not a clock.** The fixture needs two things of
every task — that it reads the fixture's clock so `wait` can move time, and
that it carries so `then` can assert a causal chain — and both used to be
things a host had to know and repeat. `task` arrives already both; decorate it
the way a deployed root does. A raw `clock` is the second argument, for the one
case that needs the arrow itself: infrastructure with its own schedule book.

`wait` now fires due deadlines only when the fixture built the log — a postgres
poller or a kronosdb server holds its schedules out of reach, and only real
elapsed time fires those, which is what `realTime` was always for.
