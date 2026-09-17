/**
 * The TYPE test for correlation's (absence of) demand.
 *
 * Every claim here is a compile-time one, so the test IS the typecheck: this
 * file is listed in the root `tsconfig.json` `files` array, which is not
 * subject to `exclude`, so it lives beside its runtime siblings in `__tests__`
 * and is still judged by `bunx tsc --noEmit`. A `@ts-expect-error` that stops
 * erroring turns that gate red.
 *
 * What it pins: `correlatingHandler` is `C` in, `C` out. The cargo lives in
 * the invocation, so the wrapper needs NOTHING of the unit of work, and a
 * wrapped handler wires against exactly the buses and processors the unwrapped
 * one did — bare `unitOfWork` included. (It used to demand a
 * `CorrelatingUnitOfWork`, because the cargo was stored on the task; that map
 * is gone, and so is the demand.)
 */
import { z } from "zod"
import { qn, command, event, type Message, type Metadata } from "../../messaging/messages.js"
import { commandHandler } from "../../command-handling/handler.js"
import { eventHandler } from "../../event-processing/handler.js"
import type { CommandHandlerContext } from "../../command-handling/context.js"
import { localCommandBus } from "../../command-handling/local-bus.js"
import { localQueryBus } from "../../query-handling/local-bus.js"
import { eventProcessor } from "../../event-processing/processor.js"
import { inMemoryEventStore } from "../../event-sourcing/in-memory.js"
import { inMemoryTokenStore } from "../../event-processing/token-store.js"
import type { CommandHandlerEntry, EventHandlerEntry } from "../../kronos.js"
import { unitOfWork } from "../../unit-of-work/unit-of-work.js"
import { correlatingHandler } from "../correlating-handler.js"
import { messageOrigin } from "../message-origin.js"

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

const bareCommandBus = localCommandBus(unitOfWork)
const bareQueryBus = localQueryBus(unitOfWork)
const bareProcessor = eventProcessor({
  name: "probe-bare",
  eventStore,
  tokenStore,
  unitOfWork,
})

// ---------------------------------------------------------------------------
// (a) A wrapped handler wires against BARE infrastructure. Nothing about the
// task changes shape because a host composed correlation.
// ---------------------------------------------------------------------------

const enroll = commandHandler(Enroll, async ({ payload }, ctx) => {
  ctx.append(Enrolled, { studentId: payload.studentId })
})

const onEnrolled = eventHandler(Enrolled, async (message, ctx) => {
  await ctx.send(Enroll, { studentId: message.payload.studentId })
})

export const correlatedCommandEntry: CommandHandlerEntry = {
  ...enroll,
  handler: correlatingHandler(enroll.handler),
  commandBus: bareCommandBus,
  queryBus: bareQueryBus,
  eventStore,
}

export const correlatedEventEntry: EventHandlerEntry = {
  ...onEnrolled,
  handler: correlatingHandler(onEnrolled.handler, messageOrigin),
  commandBus: bareCommandBus,
  queryBus: bareQueryBus,
  processor: bareProcessor,
}

// ---------------------------------------------------------------------------
// (b) `C` IN, `C` OUT — the wrapped handler asks for exactly the context the
// unwrapped one asked for, no more and no less.
// ---------------------------------------------------------------------------

const annotated = commandHandler(Enroll, async ({ payload }, ctx: CommandHandlerContext) => {
  ctx.append(Enrolled, { studentId: payload.studentId })
})

export const sameContext: (
  message: Parameters<typeof annotated.handler>[0],
  ctx: CommandHandlerContext,
) => void | Promise<void> = correlatingHandler(annotated.handler)

// ---------------------------------------------------------------------------
// (c) The cargo function is OPTIONAL and typed: a host cargo is a plain
// `(message) => Metadata`, and anything else is refused.
// ---------------------------------------------------------------------------

const hostCargo = (m: Message): Metadata => ({ ...messageOrigin(m), actor: String(m.metadata.actor ?? "") })
export const withHostCargo = correlatingHandler(enroll.handler, hostCargo)

// @ts-expect-error — the cargo is a function of the message, not a record
export const cargoMustBeAFunction = correlatingHandler(enroll.handler, { correlationId: "x" })

// ---------------------------------------------------------------------------
// (d) THE UNCORRELATED PATH — a host that never heard of the capability
// compiles exactly as it did before any of this existed.
// ---------------------------------------------------------------------------

export const plainCommandEntry: CommandHandlerEntry = {
  ...enroll,
  commandBus: bareCommandBus,
  queryBus: bareQueryBus,
  eventStore,
}

export const plainEventEntry: EventHandlerEntry = {
  ...onEnrolled,
  commandBus: bareCommandBus,
  queryBus: bareQueryBus,
  processor: bareProcessor,
}
