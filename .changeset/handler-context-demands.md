---
"@kronos-ts/core": minor
---

A handler names what it uses, never its task: `correlatingHandler` demands on
its output, the contexts take `E` before `U`, and the command context is
`CommandHandlerContext`.

Three changes to one seam, and all three are BREAKING.

**`correlatingHandler` demands on its output.** Wrapping used to require the
handler to have already claimed a correlating task; now the wrapper claims it on
the handler's behalf, and the entry's bus is what must agree.

```ts
// before — the handler had to say it
const enroll = commandHandler(Enroll, async (m, ctx: HandlerContext<CorrelatingUnitOfWork>) => { … })
handler: correlatingHandler(enroll.handler, from)

// after — the handler says nothing; the wrapper's result asks, the bus answers
const enroll = commandHandler(Enroll, async (m, ctx) => { … })
handler: correlatingHandler(enroll.handler, from)      // (m, ctx: C & { unitOfWork: CorrelatingUnitOfWork }) => R
commandBus: localCommandBus(() => unitOfWork())        // ✗ still the same compile error, same place
```

A handler that reaches for the map itself (`ctx.unitOfWork.attachCorrelationData`)
still annotates `ctx`, the way any other demand is written.

**`E` before `U` on every context.** A handler annotates the log it uses; the
task is defaulted and stays out of handler code.

```ts
// before
ctx: HandlerContext<UnitOfWork, SnapshotCapableEventStore>
ctx: EventHandlerContext<UnitOfWork, ScheduleCapableEventStore>

// after
ctx: CommandHandlerContext<SnapshotCapableEventStore>
ctx: EventHandlerContext<ScheduleCapableEventStore>
```

**`HandlerContext` is `CommandHandlerContext`** (and `handlerContext` is
`commandHandlerContext`), beside `EventHandlerContext` and
`QueryHandlerContext`. One name per kind.
