/**
 * The TYPE test for the CONDITIONAL demand.
 *
 * Every claim here is a compile-time one, so the test IS the typecheck: this
 * file is listed in the root `tsconfig.json` `files` array, which is not
 * subject to `exclude`, so it lives beside its runtime siblings in `__tests__`
 * (where the package build and the published `files` list already drop it) and
 * is still judged by `bunx tsc --noEmit`. A `@ts-expect-error` that stops
 * erroring turns that gate red — the only way a "this must not compile" claim
 * can be honest.
 *
 * What it pins: composing correlation is OPT-IN, and opting in is CONTAGIOUS in
 * exactly one direction. Wrap your handlers and the compiler makes you wrap
 * your unit of work; wrap neither and the concept does not appear in your
 * types at all. The previous attempt hardcoded the capability into `ctx` and
 * the bus signatures, which made the demand unconditional — and an
 * unconditional demand propagates contravariantly through every transport, so
 * every bus in the world had to know about correlation. Section (d) is the
 * assertion that this one does not.
 */
import { z } from "zod"
import { qn, command, event, type Message, type Metadata } from "../../messaging/messages.js"
import { commandHandler } from "../../command-handling/handler.js"
import { eventHandler } from "../../event-processing/handler.js"
import type { CommandHandlerContext } from "../../command-handling/context.js"
import type { EventStore } from "../../event-sourcing/event-store.js"
import type { QueryBus } from "../../query-handling/bus.js"
import { localCommandBus } from "../../command-handling/local-bus.js"
import { localQueryBus } from "../../query-handling/local-bus.js"
import { eventProcessor } from "../../event-processing/processor.js"
import { inMemoryEventStore } from "../../event-sourcing/in-memory.js"
import { inMemoryTokenStore } from "../../event-processing/token-store.js"
import type { CommandHandlerEntry, EventHandlerEntry } from "../../kronos.js"
import { unitOfWork, type UnitOfWork } from "../../unit-of-work/unit-of-work.js"
import { correlating, type CorrelatingUnitOfWork } from "../correlating.js"
import { correlatingHandler } from "../correlating-handler.js"
// The id-pair cargo, written out as any host writes it: the chain is inherited
// or seeded; the cause is the parent, unconditionally.
const correlationFrom = (parent: Message): Metadata => ({
  correlationId: String(parent.metadata.correlationId ?? parent.identifier),
  causationId: String(parent.identifier),
})

const Enroll = command({
  name: qn("probe", "Enroll"),
  payload: z.object({ studentId: z.string() }),
})
const Enrolled = event({
  name: qn("probe", "Enrolled"),
  payload: z.object({ studentId: z.string() }),
})

const eventStore = inMemoryEventStore()
const tokenStore = inMemoryTokenStore()

// ---------------------------------------------------------------------------
// (0) THE DELEGATION CHECK — `CorrelatingUnitOfWork` is DERIVED from the
// function, so nothing at the definition site catches a member that stopped
// delegating. This line does: a dropped `onCommit`, a missed `stateCache`, a
// `phase` that turned into a plain field — any of them and this stops being a
// unit of work at all.
// ---------------------------------------------------------------------------

export const stillAUnitOfWork: UnitOfWork = correlating(unitOfWork())

// ---------------------------------------------------------------------------
// (a) OPTING IN — a correlating factory yields buses and processors that ACCEPT
// correlatingHandler-wrapped handlers.
// ---------------------------------------------------------------------------

const correlatingUow = () => correlating(unitOfWork())
const correlatingCommandBus = localCommandBus(correlatingUow)
const correlatingQueryBus = localQueryBus(correlatingUow)
const correlatingProcessor = eventProcessor({
  name: "probe-correlating",
  eventStore,
  tokenStore,
  unitOfWork: correlatingUow,
})

/**
 * A slice-side handler, written exactly as an uncorrelated one is. NOTHING
 * HERE NAMES A TASK: the demand is made by the wrapper below, on its output,
 * so a handler never has to know whether somebody composed correlation around
 * it.
 */
const enroll = commandHandler(Enroll, async ({ payload }, ctx) => {
  ctx.append(Enrolled, { studentId: payload.studentId })
})

const onEnrolled = eventHandler(Enrolled, async (message, ctx) => {
  await ctx.send(Enroll, { studentId: message.payload.studentId })
})

export const correlatedCommandEntry: CommandHandlerEntry<CorrelatingUnitOfWork> = {
  ...enroll,
  handler: correlatingHandler(enroll.handler, correlationFrom),
  commandBus: correlatingCommandBus,
  queryBus: correlatingQueryBus,
  eventStore,
}

export const correlatedEventEntry: EventHandlerEntry<CorrelatingUnitOfWork> = {
  ...onEnrolled,
  handler: correlatingHandler(onEnrolled.handler, correlationFrom),
  commandBus: correlatingCommandBus,
  queryBus: correlatingQueryBus,
  processor: correlatingProcessor,
}

// ---------------------------------------------------------------------------
// (b) THE CONDITIONAL COMPILE ERROR — the same wrapped handlers against
// infrastructure built from a BARE `() => unitOfWork()`. This is the whole
// point of the parametric threading: a correlating handler needs a correlating
// task, and the factory is where a task's shape is decided.
// ---------------------------------------------------------------------------

const bareCommandBus = localCommandBus(unitOfWork)
const bareQueryBus = localQueryBus(unitOfWork)
const bareProcessor = eventProcessor({
  name: "probe-bare",
  eventStore,
  tokenStore,
  unitOfWork,
})

export const busTooPlain: CommandHandlerEntry<CorrelatingUnitOfWork> = {
  ...enroll,
  handler: correlatingHandler(enroll.handler, correlationFrom),
  // @ts-expect-error — this bus mints bare units of work; the handler needs correlating ones
  commandBus: bareCommandBus,
  queryBus: correlatingQueryBus,
  eventStore,
}

export const processorTooPlain: EventHandlerEntry<CorrelatingUnitOfWork> = {
  ...onEnrolled,
  handler: correlatingHandler(onEnrolled.handler, correlationFrom),
  commandBus: correlatingCommandBus,
  queryBus: correlatingQueryBus,
  // @ts-expect-error — this processor commits in bare units of work
  processor: bareProcessor,
}

// ---------------------------------------------------------------------------
// (c) THE DEMAND IS ON THE OUTPUT — wrapping adds it; the handler never held it.
// A handler that annotated the BARE context wraps fine, and what comes out asks
// for a correlating task: the map is something the wrapper needs, not `next`.
// ---------------------------------------------------------------------------

const uncorrelatable = commandHandler(Enroll, async ({ payload }, ctx: CommandHandlerContext) => {
  ctx.append(Enrolled, { studentId: payload.studentId })
})

export const wrapsFine: (
  message: Parameters<typeof uncorrelatable.handler>[0],
  ctx: CommandHandlerContext<EventStore, QueryBus, CorrelatingUnitOfWork>,
) => void | Promise<void> = correlatingHandler(uncorrelatable.handler, correlationFrom)

export const stillTooPlain: CommandHandlerEntry = {
  ...uncorrelatable,
  // @ts-expect-error — the wrapped handler asks for a correlating task; this entry's bus mints bare ones
  handler: correlatingHandler(uncorrelatable.handler, correlationFrom),
  commandBus: bareCommandBus,
  queryBus: bareQueryBus,
  eventStore,
}

/**
 * And the cargo function is REQUIRED. A default here would decide, for every
 * host in the world, what is worth carrying from a message to its children —
 * which is precisely the decision the mechanism refuses to make.
 */
// @ts-expect-error — `from` is not optional and is not defaulted
export const cannotWrapWithoutCargo = correlatingHandler(enroll.handler, undefined)

// ---------------------------------------------------------------------------
// (d) THE UNCORRELATED PATH — a host that never heard of the capability. Not
// one mention of correlation, not one type argument, and it compiles exactly as
// it did before any of this existed.
// ---------------------------------------------------------------------------

const plain = commandHandler(Enroll, async ({ payload }, ctx) => {
  ctx.append(Enrolled, { studentId: payload.studentId })
})

const plainReactor = eventHandler(Enrolled, async (message, ctx) => {
  await ctx.send(Enroll, { studentId: message.payload.studentId })
})

export const plainCommandEntry: CommandHandlerEntry = {
  ...plain,
  commandBus: bareCommandBus,
  queryBus: bareQueryBus,
  eventStore,
}

export const plainEventEntry: EventHandlerEntry = {
  ...plainReactor,
  commandBus: bareCommandBus,
  queryBus: bareQueryBus,
  processor: bareProcessor,
}

/**
 * A correlating bus SATISFIES a bare slot — the demand runs one way only. A
 * host that composed correlation can still hand its bus to something written
 * against the plain shape (a transport, a recorder, a health probe); what it
 * cannot do is the reverse.
 */
export const oneWay: CommandHandlerEntry = {
  ...plain,
  commandBus: correlatingCommandBus,
  queryBus: correlatingQueryBus,
  eventStore,
}
