---
"@kronos-ts/core": minor
"@kronos-ts/rabbitmq": minor
"@kronos-ts/kronosdb": minor
"@kronos-ts/axon-server": minor
---

One clock per task: `Clock` enters at `unitOfWork`, and every message a task
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
  commandHandlers: billing.map((h) => ({ ...h, eventStore, commandBus, queryBus, eventScheduler })),
})
```

Still no default: a scheduler is durable infrastructure with a worker behind it,
and there is nothing honest to conjure.

## Left on `Date.now()`, deliberately

`inMemoryTokenStore`'s claim timestamps, `inMemoryDeadLetterQueue`'s
`enqueuedAt` / `lastTouched`, and the snapshot record `eventSourcedRepository`
writes. None of them is a message, none of them is read by a handler, and the
repository has no unit of work in hand — a lease that expires on a frozen clock
would be a lease that never expires.
