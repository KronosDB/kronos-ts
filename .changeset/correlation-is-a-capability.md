---
"@kronos-ts/core": minor
"@kronos-ts/test": minor
"@kronos-ts/postgres": minor
"@kronos-ts/drizzle": minor
"@kronos-ts/knex": minor
"@kronos-ts/kysely": minor
"@kronos-ts/prisma": minor
"@kronos-ts/typeorm": minor
---

Correlation is no longer knowledge the unit of work is born with — it is a capability you compose. Wrap your handlers, and the compiler makes you wrap your unit of work.

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
})
```

More cargo is more function — `correlatingHandler(h.handler, (m) => ({ ...correlationFrom(m), actor: String(m.metadata.actor) }))`.

**BREAKING — births no longer carry the handled message's metadata.** `ctx.send`, `ctx.query`, `ctx.append` and `ctx.schedule` used to take the handled message's whole metadata as their base, so anything on an incoming command rode forward for free. They no longer do: a birth's metadata is exactly the trailing `metadata?` argument it was given, and nothing else.

```ts
// before — `actor` arrived on the appended event because the base was the command's metadata
ctx.append(CourseCreated, { courseId })

// after — compose the cargo, once, at the composition root
commandHandlers: handlers
  .map((h) => ({ ...h, handler: correlatingHandler(h.handler, (m) => ({
    ...correlationFrom(m),
    actor: String(m.metadata.actor ?? ""),
  })) }))
```

The free carry read as a convenience and behaved as a policy: which of a message's keys are safe to propagate is a host decision, and a primitive that decides it silently propagates a tenant id into a message that crosses a tenant boundary. It is now one function, written down where a reader can find it.

**BREAKING — a child's `causationId` is always the parent's identifier.** `correlationFrom` reads `causationId: parent.identifier`, unconditionally, never `parent.metadata.causationId ?? parent.identifier`. A child is caused by its parent, not by its grandparent. So an automation's dispatched command is caused by the event it reacted to, not by the command that appended that event. The `correlation` intercept is unchanged — it still `??`-seeds both fields, because it runs on messages born at an edge with no parent to ask. Between them: the edge seeds a root, every hop re-stamps.

**BREAKING — the adapter unit-of-work decorators reverse their arguments.** `<pkg>UnitOfWork(client, make)` is now `<pkg>UnitOfWork(next, client)` in all six persistence packages — thing-first, like every other decorator on the surface. The thing being decorated is the factory; the client is configuration.

```ts
// before
const uow = drizzleUnitOfWork(db, unitOfWork)
const uow = postgresUnitOfWork(pg, unitOfWork)

// after
const uow = drizzleUnitOfWork(unitOfWork, db)
const uow = postgresUnitOfWork(() => correlating(unitOfWork(clock)), pg)
```

They are also capability-preserving: each returns `() => U` for whatever `U` it was handed and decorates the SAME handle rather than rebuilding a record from it, so a composed capability survives the type AND the runtime — the adapter's transaction is keyed on the very object `ctx.unitOfWork` hands back.

**The unit of work goes pure.** `UnitOfWork` loses `correlationData()` and `contributeCorrelationData()`, and the map with them; the contexts lose `ctx.contributeCorrelationData` (a handler that wants a mid-handling attach reaches `ctx.unitOfWork.attachCorrelationData` on a correlating task); the event processor no longer stamps a correlation rule onto each batch. Core mentions correlation nowhere outside `correlation/`. `requireInvocation` / `requireLive` are now `<U extends UnitOfWork>(uow: U): U` — a guard checks a unit of work, it does not launder one.

**The demand is conditional, which is the whole point.** Buses, processors, contexts and the `kronos` entry types are now parametric in the unit of work their factory mints — `localCommandBus<U>(unitOfWork: () => U): CommandBus<U>`, threaded to `ctx.unitOfWork` and preserved across every transport. `U` defaults to the bare `UnitOfWork` everywhere, so uncorrelated code reads exactly as it did and never writes a type argument. What the threading buys is that a `correlatingHandler`-wrapped handler does NOT typecheck against a bus or processor built from a bare `() => unitOfWork()`:

```ts
kronos({
  commandHandlers: [{
    ...h,
    handler: correlatingHandler(h.handler, correlationFrom),
    commandBus: localCommandBus(unitOfWork),   // ← compile error: mints bare units of work
  }],
})
```

An earlier attempt hardcoded a correlation capability into `ctx` and the bus signatures. It was reverted, and the lesson is in this shape: an unconditional demand propagates contravariantly through every transport, so every bus in the world has to know about correlation. A conditional one propagates exactly as far as somebody asked for it. A new type probe — registered in the root `tsconfig` `files` array beside the drizzle one, so a `@ts-expect-error` that stops erroring turns the build red — pins both directions: the wiring that must compile, and the four that must not.

**The test fixture composes correlation**, because a fixture is a composition root and composes like a host: its tasks are `() => correlating(unitOfWork(clock))` and every handler the scope hands it is wrapped with `correlatingHandler(handler, correlationFrom)`. Scenario correlation semantics are unchanged; a scope wanting other cargo wraps its own handlers first.
