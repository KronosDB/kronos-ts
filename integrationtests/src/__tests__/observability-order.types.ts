/**
 * The TYPE test for the observability stack's ORDER, across packages. Listed
 * in the root `tsconfig.json` `files` array, so `bunx tsc --noEmit` judges it.
 *
 * What it pins, with the real wrappers:
 *
 * - The documented stack compiles and wires into an entry: `drizzleHandler`
 *   from the postgres package's subpath inside `postgresHandler`, inside
 *   correlation and logging, `otlpHandler` outermost.
 * - `otlpHandler` inside `correlatingHandler` or `loggingHandler` is refused
 *   with a sentence.
 * - An OBSERVED pool makes `postgresHandler` demand `trace`, so the entry
 *   refuses to wire until `otlpHandler` sits outside it; a plain pool demands
 *   nothing.
 * - A transactional factory is refused by `localQueryBus`.
 */
import { z } from "zod"
import {
  command,
  commandHandler,
  consoleLogger,
  correlatingHandler,
  inMemoryEventStore,
  localCommandBus,
  localQueryBus,
  loggingHandler,
  qn,
  unitOfWork,
  type CommandHandlerContext,
  type CommandHandlerEntry,
  type LogCapability,
} from "@kronos-ts/core"
import { observed, postgresHandler, postgresUnitOfWork, type PostgresAdapter } from "@kronos-ts/postgres"
import { drizzleHandler, type DrizzleCapability } from "@kronos-ts/postgres/drizzle"
import { otlpExporter, otlpHandler, type TraceCapability } from "@kronos-ts/otlp"
import { drizzle } from "drizzle-orm/postgres-js"

declare function pipe<A, B>(a: A, ab: (a: A) => B): B
declare function pipe<A, B, C>(a: A, ab: (a: A) => B, bc: (b: B) => C): C
declare function pipe<A, B, C, D>(a: A, ab: (a: A) => B, bc: (b: B) => C, cd: (c: C) => D): D
declare function pipe<A, B, C, D, E>(a: A, ab: (a: A) => B, bc: (b: B) => C, cd: (c: C) => D, de: (d: D) => E): E
declare function pipe<A, B, C, D, E, F>(a: A, ab: (a: A) => B, bc: (b: B) => C, cd: (c: C) => D, de: (d: D) => E, ef: (e: E) => F): F

const Place = command({ name: qn("orders", "Place"), payload: z.object({ id: z.string() }) })
declare const pg: PostgresAdapter
const observedPg = observed(pg)
const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "probe" })
const log = consoleLogger()
const eventStore = inMemoryEventStore()
const uow = postgresUnitOfWork(unitOfWork, pg)
const commandBus = localCommandBus(uow)
const queryBus = localQueryBus(unitOfWork)

// The one persistence step a slice takes: Drizzle over the task's
// transaction, from the postgres package's subpath, typed as Drizzle types it.
const drizzleOver = (client: unknown) => drizzle(client as never)
type Db = ReturnType<typeof drizzleOver>

type Ctx = CommandHandlerContext & DrizzleCapability<Db> & LogCapability & Partial<TraceCapability>
const place = commandHandler(Place, async (_m, ctx: Ctx) => {
  ctx.db.select
  ctx.log.info("placed")
})

// ---------------------------------------------------------------------------
// (a) THE DOCUMENTED STACK — innermost first, otlpHandler outermost.
// ---------------------------------------------------------------------------

export const documented: CommandHandlerEntry = {
  ...place,
  handler: pipe(
    place.handler,
    (h) => drizzleHandler(h, drizzleOver),
    (h) => postgresHandler(h, observedPg),
    (h) => correlatingHandler(h),
    (h) => loggingHandler(h, log),
    (h) => otlpHandler(h, exporter),
  ),
  commandBus,
  queryBus,
  eventStore,
}

// ---------------------------------------------------------------------------
// (b) ORDER — the stamping wrapper inside a reader is a sentence.
// ---------------------------------------------------------------------------

export const tracingInsideCorrelation = pipe(
  place.handler,
  (h) => drizzleHandler(h, drizzleOver),
  (h) => postgresHandler(h, pg),
  (h) => otlpHandler(h, exporter),
  // @ts-expect-error — "a wrapper that stamps message.metadata is inside correlatingHandler: move it outside…"
  (h) => correlatingHandler(h),
)

export const tracingInsideLogging = pipe(
  place.handler,
  (h) => drizzleHandler(h, drizzleOver),
  (h) => postgresHandler(h, pg),
  (h) => otlpHandler(h, exporter),
  // @ts-expect-error — "a wrapper that stamps message.metadata is inside loggingHandler: move it outside…"
  (h) => loggingHandler(h, log),
)

// ---------------------------------------------------------------------------
// (c) AN OBSERVED POOL DEMANDS TRACING — the entry refuses until a tracer sits
// outside postgresHandler. A plain pool demands nothing.
// ---------------------------------------------------------------------------

export const observedWithoutTracer: CommandHandlerEntry = {
  ...place,
  // @ts-expect-error — `trace` is demanded by postgresHandler(observed) and nothing supplies it
  handler: pipe(
    place.handler,
    (h) => drizzleHandler(h, drizzleOver),
    (h) => postgresHandler(h, observedPg),
    (h) => correlatingHandler(h),
    (h) => loggingHandler(h, log),
  ),
  commandBus,
  queryBus,
  eventStore,
}

export const plainWithoutTracer: CommandHandlerEntry = {
  ...place,
  handler: pipe(
    place.handler,
    (h) => drizzleHandler(h, drizzleOver),
    (h) => postgresHandler(h, pg),
    (h) => correlatingHandler(h),
    (h) => loggingHandler(h, log),
  ),
  commandBus,
  queryBus,
  eventStore,
}

// ---------------------------------------------------------------------------
// (d) A TRANSACTIONAL FACTORY IS NOT A QUERY BUS FACTORY.
// ---------------------------------------------------------------------------

// @ts-expect-error — "a query bus must be built from the plain unitOfWork factory…"
export const transactionalQueryBus = localQueryBus(uow)
