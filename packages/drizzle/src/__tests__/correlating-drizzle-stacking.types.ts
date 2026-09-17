/**
 * THE STACKING PROBE — correlatingHandler AND drizzleHandler on one handler,
 * both orders, with a handler that names its own capability demand.
 *
 * `correlatingHandler` is `C` IN, `C` OUT — it demands nothing of the context
 * and adds nothing to what a wrapped handler asks a bus for (see
 * `correlation-demand.types.ts`). `drizzleHandler` demands on its INPUT
 * (`ctx.db()` is something the handler USES, so the handler says so:
 * `ctx: CommandHandlerContext & DrizzleCapability`) and ERASES it on the way
 * out — the entry never sees `db`.
 *
 * So stacking the two, in either order, changes NOTHING about what the entry
 * must supply: the wrapped handler still asks for exactly what
 * `drizzleHandler` alone would ask for, because correlating's wrapper is
 * invisible to the type. The task stays the bare {@link UnitOfWork} the
 * factory mints — no richer capability rides on it, because nothing here
 * demands one of it any more.
 *
 * Nothing here runs; it is judged by `bunx tsc --noEmit` through the root
 * `tsconfig.json` `files` array.
 */
import {
  command,
  commandHandler,
  correlatingHandler,
  inMemoryEventStore,
  localCommandBus,
  localQueryBus,
  qn,
  unitOfWork,
  type CommandHandlerContext,
  type CommandHandlerEntry,
  type StandardSchemaV1,
} from "@kronos-ts/core"
import {
  drizzleHandler,
  drizzleUnitOfWork,
  type DrizzleCapability,
  type DrizzleDb,
} from "../drizzle-transaction.js"

declare const db: DrizzleDb

declare const enrollPayload: StandardSchemaV1<{ studentId: string }>
const Enroll = command({ name: qn("probe", "Enroll"), payload: enrollPayload })

/** The handler says ONE thing — it uses `db()` — and nothing about its task. */
const enroll = commandHandler(Enroll, async ({ payload }, ctx: CommandHandlerContext & DrizzleCapability) => {
  void ctx.db()
  void payload.studentId
})

// The task is named ONCE, here, by composing the factory. Everything below
// reads it off this value. `drizzleUnitOfWork` decorates what it is given and
// adds no mark of its own: what a unit of work IS, is what it can do.
const uow = drizzleUnitOfWork(unitOfWork, db)

const commandBus = localCommandBus(uow)
// The query bus takes the PLAIN factory — a query is always its own task and
// needs no transaction, and `localQueryBus` refuses a transactional one at
// compile time.
const queryBus = localQueryBus(unitOfWork)
const eventStore = inMemoryEventStore()

// ---------------------------------------------------------------------------
// (a) BOTH ORDERS STACK, AND NEITHER CHANGES THE DEMAND. Drizzle outside
// erases `db` from a handler correlating already passed through unchanged;
// drizzle inside erases `db` first and correlating adds nothing on top.
// Either way the entry sees a handler that asks for nothing beyond the bare
// task and wires against the bare buses below.
// ---------------------------------------------------------------------------

export const drizzleOutside: CommandHandlerEntry = {
  ...enroll,
  handler: drizzleHandler(correlatingHandler(enroll.handler), db),
  commandBus,
  queryBus,
  eventStore,
}

export const drizzleInside: CommandHandlerEntry = {
  ...enroll,
  handler: correlatingHandler(drizzleHandler(enroll.handler, db)),
  commandBus,
  queryBus,
  eventStore,
}

// ---------------------------------------------------------------------------
// (b) FORGET `drizzleHandler` — the handler still asks for `db()`, and the
// entry refuses a handler it cannot supply. Correlating does not change this
// either way, because it never touched the demand.
// ---------------------------------------------------------------------------

export const forgotDrizzle: CommandHandlerEntry = {
  ...enroll,
  // @ts-expect-error — `db()` was asked for and nothing supplied it
  handler: correlatingHandler(enroll.handler),
  commandBus,
  queryBus,
  eventStore,
}

// ---------------------------------------------------------------------------
// (c) THE PLAIN PATH — a handler that asked for nothing, wired to the same
// factory. A handler naming no capability is satisfied by any task.
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
