/**
 * The TYPE test for the COMPOSITION SITE.
 *
 * Every claim here is a compile-time one, so the test IS the typecheck: this
 * file is listed in the root `tsconfig.json` `files` array, which is not subject
 * to `exclude`, so it lives beside its runtime siblings in `__tests__` (where
 * the package build and the published `files` list already drop it) and is still
 * judged by `bunx tsc --noEmit`. A `@ts-expect-error` that stops erroring turns
 * that gate red.
 *
 * What it pins, in one sentence: `validatingHandler` composes where a descriptor
 * and a handler already sit together — the ENTRY — for all three handler kinds,
 * in any order with the other function-level wrappers, and `validate` gives back
 * the descriptor's exact parsed payload type rather than `unknown`.
 */
import { z } from "zod"
import { qn, command, event, queryDescriptor, type Message, type Metadata } from "../../messaging/messages.js"
import { commandHandler } from "../../command-handling/handler.js"
import { eventHandler } from "../../event-processing/handler.js"
import { queryHandler } from "../../query-handling/handler.js"
import { correlating, type CorrelatingUnitOfWork } from "../../correlation/correlating.js"
import { correlatingHandler } from "../../correlation/correlating-handler.js"
import { kronos } from "../../kronos.js"
import type { CommandHandlerContext } from "../../command-handling/context.js"
import type { EventHandlerContext } from "../../event-processing/context.js"
import type { CommandBus } from "../../command-handling/bus.js"
import type { QueryBus } from "../../query-handling/bus.js"
import type { EventStore } from "../../event-sourcing/event-store.js"
import type { EventProcessor } from "../../event-processing/processor.js"
import { validate } from "../validate.js"
import { validatingHandler } from "../validating-handler.js"

// ---------------------------------------------------------------------------
// (a) REAL DEFINITIONS — one of each kind, as a slice writes them.
// ---------------------------------------------------------------------------

const OpenAccount = command({
  name: qn("billing", "OpenAccount"),
  payload: z.object({ accountId: z.string() }),
  result: z.object({ accountId: z.string() }),
})

const Charged = event({
  name: qn("billing", "Charged"),
  payload: z.object({ accountId: z.string(), amount: z.number() }),
  tags: { accountId: (p) => p.accountId },
})

const GetBalance = queryDescriptor({
  name: qn("billing", "GetBalance"),
  payload: z.object({ accountId: z.string() }),
})

const openAccount = commandHandler(OpenAccount, async ({ payload }) => ({
  accountId: payload.accountId,
}))
const onCharged = eventHandler(Charged, ({ payload }) => {
  void payload.amount
})
const getBalance = queryHandler(GetBalance, async ({ payload }) => payload.accountId.length)

/**
 * The same two, unannotated — a slice never names its task. The correlation
 * demand belongs to `correlatingHandler` (on its OUTPUT) and is unchanged by
 * validation: `validatingHandler` asks the context for nothing, so it neither
 * adds a demand nor satisfies one.
 */
const openAccountCorrelating = commandHandler(OpenAccount, async ({ payload }) => ({
  accountId: payload.accountId,
}))
const onChargedCorrelating = eventHandler(Charged, ({ payload }) => {
  void payload.amount
})

// ---------------------------------------------------------------------------
// (b) `validate` GIVES BACK THE DESCRIPTOR'S OWN TYPE.
//
// A descriptor with a `result` schema and an event with a `tags` extractor are
// the two shapes that used to be inexpressible: both carry functions, so both
// are checked contravariantly against a defaulted `MessageDescriptor`. The
// constraint is widened for exactly that reason, and the payload type still
// comes back exact.
// ---------------------------------------------------------------------------

declare const body: unknown

export const parsedCommand: { accountId: string } | Promise<{ accountId: string }> = validate(
  OpenAccount,
  body,
)
export const parsedEvent: { accountId: string; amount: number } | Promise<{ accountId: string; amount: number }> =
  validate(Charged, body)
export const parsedQuery: { accountId: string } | Promise<{ accountId: string }> = validate(
  GetBalance,
  body,
)

// @ts-expect-error the parse is the descriptor's payload type — not whatever the caller hoped
export const wrongParse: { nope: string } | Promise<{ nope: string }> = validate(OpenAccount, body)

// ---------------------------------------------------------------------------
// (c) THE COMPOSITION SITE — `.map((h) => ({ ...h, handler: validatingHandler(h.handler, h.descriptor) }))`
// ---------------------------------------------------------------------------

const correlationFrom = (parent: Message): Metadata => ({
  correlationId: String(parent.metadata.correlationId ?? parent.identifier),
  causationId: String(parent.identifier),
})

declare const commandBus: CommandBus<CorrelatingUnitOfWork>
declare const queryBus: QueryBus<CorrelatingUnitOfWork>
declare const eventStore: EventStore
declare const processor: EventProcessor<CorrelatingUnitOfWork>
declare const uow: () => CorrelatingUnitOfWork
void correlating
void uow

/** Validation ALONE — it demands nothing of the context, so the bare wiring compiles. */
export const validated = kronos({
  commandHandlers: [openAccount]
    .map((h) => ({ ...h, handler: validatingHandler(h.handler, h.descriptor) }))
    .map((h) => ({ ...h, commandBus, queryBus, eventStore })),
  queryHandlers: [getBalance]
    .map((h) => ({ ...h, handler: validatingHandler(h.handler, h.descriptor) }))
    .map((h) => ({ ...h, queryBus })),
  eventHandlers: [onCharged]
    .map((h) => ({ ...h, handler: validatingHandler(h.handler, h.descriptor) }))
    .map((h) => ({ ...h, commandBus, queryBus, processor })),
})

/** …and stacked with correlation, in one chain, per kind. */
export const validatedAndCorrelating = kronos({
  commandHandlers: [openAccountCorrelating]
    .map((h) => ({
      ...h,
      handler: validatingHandler(correlatingHandler(h.handler, correlationFrom), h.descriptor),
    }))
    .map((h) => ({ ...h, commandBus, queryBus, eventStore })),
  eventHandlers: [onChargedCorrelating]
    .map((h) => ({
      ...h,
      // The other order composes too — a wrapper that supplies nothing to the
      // context erases nothing, so neither of these is the "right" one.
      handler: correlatingHandler(validatingHandler(h.handler, h.descriptor), correlationFrom),
    }))
    .map((h) => ({ ...h, commandBus, queryBus, processor })),
})

// ---------------------------------------------------------------------------
// (d) THE SPREAD CARRIES THE REST OF THE ENTRY — the wrapper takes the handler
// FUNCTION, so `kind`, `descriptor` and `appendCondition` are untouched.
// ---------------------------------------------------------------------------

const wrapped = { ...openAccount, handler: validatingHandler(openAccount.handler, openAccount.descriptor) }
export const stillACommandHandler: "command-handler" = wrapped.kind
export const stillTheDescriptor: typeof OpenAccount = wrapped.descriptor
