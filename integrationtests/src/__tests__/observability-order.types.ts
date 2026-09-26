/**
 * The TYPE test for the observability stack's ORDER, across packages. Listed
 * in the root `tsconfig.json` `files` array, so `bunx tsc --noEmit` judges it.
 *
 * What it pins, with the real wrappers:
 *
 * - The documented stack compiles and wires into an entry: an app-written
 *   `drizzleHandler` inside `postgresHandler`, inside correlation and logging,
 *   `otlpHandler` outermost.
 * - `otlpHandler` inside `correlatingHandler` or `loggingHandler` is refused
 *   with a sentence.
 * - `otlpHandler` inside `postgresHandler` is refused with a sentence: the
 *   trace would arrive too late to span a statement. With no tracer at all,
 *   `postgresHandler` demands nothing.
 * - A transactional factory is refused by `localQueryBus`.
 */
import { z } from "zod"
import {
  command,
  commandHandler,
  consoleLogger,
  correlatingHandler,
  describe,
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
import {
  postgresHandler,
  postgresUnitOfWork,
  type PostgresAdapter,
  type PostgresCapability,
} from "@kronos-ts/postgres"
import { otlpExporter, otlpHandler, type TraceCapability } from "@kronos-ts/otlp"
import { drizzle, type PostgresJsDatabase } from "drizzle-orm/postgres-js"
import type { Sql as PostgresJs } from "postgres"

declare function pipe<A, B>(a: A, ab: (a: A) => B): B
declare function pipe<A, B, C>(a: A, ab: (a: A) => B, bc: (b: B) => C): C
declare function pipe<A, B, C, D>(a: A, ab: (a: A) => B, bc: (b: B) => C, cd: (c: C) => D): D
declare function pipe<A, B, C, D, E>(a: A, ab: (a: A) => B, bc: (b: B) => C, cd: (c: C) => D, de: (d: D) => E): E
declare function pipe<A, B, C, D, E, F>(a: A, ab: (a: A) => B, bc: (b: B) => C, cd: (c: C) => D, de: (d: D) => E, ef: (e: E) => F): F

const Place = command({ name: qn("orders", "Place"), payload: z.object({ id: z.string() }) })
declare const pg: PostgresAdapter
const exporter = otlpExporter({ endpoint: "http://c:4318", serviceName: "probe" })
const log = consoleLogger()
const eventStore = inMemoryEventStore()
const uow = postgresUnitOfWork(unitOfWork, pg)
const commandBus = localCommandBus(uow)
const queryBus = localQueryBus(unitOfWork)

// The one persistence step a slice takes: Drizzle over the task's
// transaction. The APP writes it — Kronos ships no builder integration — and
// it composes with the real wrappers like any other described step.
type DrizzleCapability = { readonly db: PostgresJsDatabase }

const drizzleHandler = <M, C extends DrizzleCapability, R>(next: (message: M, context: C) => R) =>
  describe(
    (message: M, context: Omit<C, "db"> & PostgresCapability): R =>
      next(message, { ...context, db: drizzle(context.sql().unwrap<PostgresJs>()) } as unknown as C),
    { name: "drizzleHandler", supplies: ["db"], uses: ["sql"], next } as const,
  )

type Ctx = CommandHandlerContext & DrizzleCapability & LogCapability & Partial<TraceCapability>
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
    (h) => drizzleHandler(h),
    (h) => postgresHandler(h, pg),
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
  (h) => drizzleHandler(h),
  (h) => postgresHandler(h, pg),
  (h) => otlpHandler(h, exporter),
  // @ts-expect-error — "a wrapper that stamps message.metadata is inside correlatingHandler: move it outside…"
  (h) => correlatingHandler(h),
)

export const tracingInsideLogging = pipe(
  place.handler,
  (h) => drizzleHandler(h),
  (h) => postgresHandler(h, pg),
  (h) => otlpHandler(h, exporter),
  // @ts-expect-error — "a wrapper that stamps message.metadata is inside loggingHandler: move it outside…"
  (h) => loggingHandler(h, log),
)

// ---------------------------------------------------------------------------
// (c) STATEMENT SPANS NEED THE TRACER OUTSIDE — inside `postgresHandler` it is
// a sentence. With no tracer at all, postgresHandler demands nothing.
// ---------------------------------------------------------------------------

export const tracingInsidePostgres = pipe(
  place.handler,
  (h) => drizzleHandler(h),
  (h) => otlpHandler(h, exporter),
  // @ts-expect-error — "a wrapper that supplies ctx.trace is inside postgresHandler: move it outside…"
  (h) => postgresHandler(h, pg),
)

export const plainWithoutTracer: CommandHandlerEntry = {
  ...place,
  handler: pipe(
    place.handler,
    (h) => drizzleHandler(h),
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
