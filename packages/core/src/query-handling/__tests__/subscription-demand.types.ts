/**
 * THE TYPE TEST FOR THE SUBSCRIPTION TIER — the third capability tier, the
 * first on a BUS, and the same four-quadrant construction as snapshotting and
 * scheduling. Judged by `bunx tsc --noEmit` through the root tsconfig `files`
 * array; a `@ts-expect-error` that stops erroring turns the gate red.
 */
import { z } from "zod"
import { qn, command, event, queryDescriptor as queryDescriptorOf } from "../../messaging/messages.js"
import { commandHandler } from "../../command-handling/handler.js"
import { eventHandler } from "../../event-processing/handler.js"
import type { CommandHandlerContext } from "../../command-handling/context.js"
import type { EventHandlerContext } from "../../event-processing/context.js"
import { localCommandBus } from "../../command-handling/local-bus.js"
import { localQueryBus } from "../../query-handling/local-bus.js"
import type { QueryBus, SubscriptionCapableQueryBus } from "../bus.js"
import type { EmitCapability } from "../emit-update.js"
import type { SnapshotCapableEventStore } from "../../event-sourcing/event-store.js"
import { subscriptionQuery } from "../subscription-query.js"
import { interceptingQueryBus } from "../../interception/intercepting-bus.js"
import { eventProcessor } from "../../event-processing/processor.js"
import { inMemoryEventStore } from "../../event-sourcing/in-memory.js"
import { inMemoryTokenStore } from "../../event-processing/token-store.js"
import type { EventHandlerEntry } from "../../kronos.js"
import { unitOfWork, type UnitOfWork } from "../../unit-of-work/unit-of-work.js"
import type { EventStore } from "../../event-sourcing/event-store.js"
import type { Intercept } from "../../interception/intercepting-bus.js"
import type { QueryMessage, Metadata } from "../../messaging/messages.js"

const Enrolled = event({ name: qn("probe", "Enrolled"), payload: z.object({ id: z.string() }) })
const Watch = queryDescriptorOf({ name: qn("probe", "Watch"), payload: z.object({ id: z.string() }) })

const eventStore = inMemoryEventStore()
const commandBus = localCommandBus(unitOfWork)
const capableBus = localQueryBus(unitOfWork)       // natively subscription-capable
declare const plainBus: QueryBus                   // the two-member seam, nothing more
const processor = eventProcessor({ name: "probe-sub", eventStore, tokenStore: inMemoryTokenStore(), unitOfWork })

// ---------------------------------------------------------------------------
// (a) THE UNASKED QUADRANT — an unannotated handler has NO emitUpdate: the
// face is structurally absent, not present-and-throwing.
// ---------------------------------------------------------------------------

export const plain = eventHandler(Enrolled, async (_m, ctx) => {
  // @ts-expect-error — property 'emitUpdate' does not exist against a bare bus
  ctx.emitUpdate(Watch, () => true, 1)
})

// ---------------------------------------------------------------------------
// (b) ASKED AND SUPPLIED — and the ASKING NAMES ONE THING. A handler demands
// the tier by INTERSECTING its face, never by restating the context's type
// parameters: parameters are positional, so naming the bus would mean naming
// the log too, and a demand names only what it needs.
// ---------------------------------------------------------------------------

export const emitting = eventHandler(
  Enrolled,
  async (_m, ctx: EventHandlerContext & EmitCapability) => {
    ctx.emitUpdate(Watch, (q: { id: string }) => q.id === "x", 1)
  },
)

// AND THE ENTRY NEEDS NO TYPE ARGUMENTS EITHER — `Q` is inferred from the bus
// this literal names, so the supply side is as unannotated as the demand side.
export const wired = {
  ...emitting,
  commandBus,
  queryBus: capableBus,
  processor,
  eventStore,
} satisfies EventHandlerEntry<UnitOfWork, EventStore, SubscriptionCapableQueryBus>

// The command context carries the same face — a decision may push an update.
export const emittingDecision = commandHandler(
  command({ name: qn("probe", "Do"), payload: z.object({}) }),
  async (_m, ctx: CommandHandlerContext & EmitCapability) => {
    ctx.emitUpdate(Watch, () => true, 1)
  },
)

// The parameter spelling still works and means the same thing — it is what the
// ENTRY threads in. Both annotations accept the same supplied context.
export const emittingByParameter = eventHandler(
  Enrolled,
  async (_m, ctx: EventHandlerContext<EventStore, SubscriptionCapableQueryBus>) => {
    ctx.emitUpdate(Watch, (q: { id: string }) => q.id === "x", 1)
  },
)

// Faces COMPOSE with the log demands — one intersection per thing used, and
// the log is named only because THIS handler reads a cached fold.
export const emittingAndSnapshotting = eventHandler(
  Enrolled,
  async (_m, ctx: EventHandlerContext<SnapshotCapableEventStore> & EmitCapability) => {
    ctx.emitUpdate(Watch, () => true, 1)
    void ctx.source
  },
)

// ---------------------------------------------------------------------------
// (c) ASKED AND NOT SUPPLIED — the emitting handler refuses an entry whose
// bus never claimed the tier. The error lands on the handler field.
// ---------------------------------------------------------------------------

// @ts-expect-error — this handler emits subscription updates; this entry's queryBus cannot serve them
export const busTooPlain: EventHandlerEntry<UnitOfWork, EventStore, QueryBus> = {
  ...emitting,
  commandBus,
  queryBus: plainBus,
  processor,
  eventStore,
}

// …and the same refusal for the parameter spelling, so the two are one demand
// said two ways rather than two demands.
// @ts-expect-error — same refusal, whichever way the handler spelled it
export const busTooPlainByParameter: EventHandlerEntry<UnitOfWork, EventStore, QueryBus> = {
  ...emittingByParameter,
  commandBus,
  queryBus: plainBus,
  processor,
  eventStore,
}

// ---------------------------------------------------------------------------
// (d) THE EDGE VERB DEMANDS THE TIER — and wrappers PRESERVE it.
// ---------------------------------------------------------------------------

declare const payload: { id: string }
export const opened = subscriptionQuery(capableBus, Watch, payload)
// @ts-expect-error — a plain bus cannot open a subscription query
export const cannotOpen = subscriptionQuery(plainBus, Watch, payload)

declare const intercept: Intercept<QueryMessage>
// Anti-laundering: interception keeps the tier — B in, B out.
export const stillCapable: SubscriptionCapableQueryBus = interceptingQueryBus(capableBus, intercept)
export const openedThroughIntercept = subscriptionQuery(
  interceptingQueryBus(capableBus, intercept),
  Watch,
  payload,
)

// A capable bus fills a plain slot; the demand runs one way only.
export const oneWay: QueryBus = capableBus
export const metadataStillTyped: Metadata | undefined = undefined
