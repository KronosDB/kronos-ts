/**
 * THE STACKING PROBE — correlating AND drizzle on one handler, both orders,
 * with a handler that names NO task.
 *
 * Two wrappers, two different demand shapes, and the point is that they stack
 * without the handler knowing either exists:
 *
 *   - `drizzleHandler` demands on its INPUT (`ctx.db()` is something the
 *     handler USES, so the handler says so: `ctx: DrizzleCommandContext`) and
 *     ERASES it on the way out — the entry never sees `db`.
 *   - `correlatingHandler` demands on its OUTPUT (carrying is something done
 *     TO a handling, so the handler never mentions it) — what comes out asks
 *     for a correlating task, and the ENTRY's bus must mint one.
 *
 * So the only thing a handler ever writes is the capability it reaches for.
 * The task — `CorrelatingUnitOfWork & DrizzleUnitOfWork` — appears exactly
 * once, on the factory, and the compiler carries it to the bus, the processor
 * and the entry from there.
 *
 * Nothing here runs; it is judged by `bunx tsc --noEmit` through the root
 * `tsconfig.json` `files` array.
 */
import {
  command,
  commandHandler,
  correlating,
  correlatingHandler,
  inMemoryEventStore,
  localCommandBus,
  localQueryBus,
  qn,
  unitOfWork,
  type CommandHandlerEntry,
  type CorrelatingUnitOfWork,
  type Message,
  type Metadata,
  type StandardSchemaV1,
} from "@kronos-ts/core"
import {
  drizzleHandler,
  drizzleUnitOfWork,
  type DrizzleCommandContext,
  type DrizzleDb,
} from "../drizzle-transaction.js"

declare const db: DrizzleDb

const correlationFrom = (parent: Message): Metadata => ({
  correlationId: String(parent.metadata.correlationId ?? parent.identifier),
  causationId: String(parent.identifier),
})

declare const enrollPayload: StandardSchemaV1<{ studentId: string }>
const Enroll = command({ name: qn("probe", "Enroll"), payload: enrollPayload })

/** The handler says ONE thing — it uses `db()` — and nothing about its task. */
const enroll = commandHandler(Enroll, async ({ payload }, ctx: DrizzleCommandContext) => {
  void ctx.db()
  void payload.studentId
})

// The task is named ONCE, here, by composing the factory. Everything below
// reads it off this value. `drizzleUnitOfWork` decorates what it is given and
// adds no mark of its own: what a unit of work IS, is what it can do.
const uow = drizzleUnitOfWork(() => correlating(unitOfWork()), db)
type Task = CorrelatingUnitOfWork
export const mints: () => Task = uow

const commandBus = localCommandBus(uow)
const queryBus = localQueryBus(uow)
const eventStore = inMemoryEventStore()

// ---------------------------------------------------------------------------
// (a) BOTH ORDERS STACK. Drizzle outside erases `db` from a handler that
// correlation already made ask for a correlating task; drizzle inside erases
// `db` first and correlation adds its demand on top. Either way the entry
// sees a handler that asks for a correlating task and nothing else.
// ---------------------------------------------------------------------------

export const drizzleOutside: CommandHandlerEntry<Task> = {
  ...enroll,
  handler: drizzleHandler(correlatingHandler(enroll.handler, correlationFrom), db),
  commandBus,
  queryBus,
  eventStore,
}

export const drizzleInside: CommandHandlerEntry<Task> = {
  ...enroll,
  handler: correlatingHandler(drizzleHandler(enroll.handler, db), correlationFrom),
  commandBus,
  queryBus,
  eventStore,
}

// ---------------------------------------------------------------------------
// (b) DROP CORRELATION FROM THE FACTORY — the bus is what disagrees, and the
// error lands on the bus, in both orders. The handler file is untouched.
// ---------------------------------------------------------------------------

const drizzleOnly = drizzleUnitOfWork(() => unitOfWork(), db)
const plainCommandBus = localCommandBus(drizzleOnly)

export const busForgotCorrelation: CommandHandlerEntry<Task> = {
  ...enroll,
  handler: correlatingHandler(drizzleHandler(enroll.handler, db), correlationFrom),
  // @ts-expect-error — this bus mints a bare task; the wrapped handler asks for a correlating one
  commandBus: plainCommandBus,
  queryBus,
  eventStore,
}

export const busForgotCorrelationOtherOrder: CommandHandlerEntry<Task> = {
  ...enroll,
  handler: drizzleHandler(correlatingHandler(enroll.handler, correlationFrom), db),
  // @ts-expect-error — same refusal, same place, regardless of wrap order
  commandBus: plainCommandBus,
  queryBus,
  eventStore,
}

// ---------------------------------------------------------------------------
// (c) FORGET `drizzleHandler` — the handler still asks for `db()`, and the
// entry refuses a handler it cannot supply.
// ---------------------------------------------------------------------------

export const forgotDrizzle: CommandHandlerEntry<Task> = {
  ...enroll,
  // @ts-expect-error — `db()` was asked for and nothing supplied it
  handler: correlatingHandler(enroll.handler, correlationFrom),
  commandBus,
  queryBus,
  eventStore,
}

// ---------------------------------------------------------------------------
// (d) THE PLAIN PATH — a handler that asked for nothing, wired to the composed
// factory. A richer task satisfies a bare slot; the demand runs one way.
// ---------------------------------------------------------------------------

const plain = commandHandler(Enroll, async ({ payload }) => {
  void payload.studentId
})

export const plainOnRichTask: CommandHandlerEntry = {
  ...plain,
  commandBus,
  queryBus,
  eventStore,
}
