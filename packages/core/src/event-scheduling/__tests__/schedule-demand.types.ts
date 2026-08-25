/**
 * THE TYPE TEST FOR THE SCHEDULING DEMAND.
 *
 * Every claim here is a compile-time one, so the test IS the typecheck: this
 * file is listed in the root `tsconfig.json` `files` array, which is not subject
 * to `exclude`, so it lives beside its runtime siblings in `__tests__` (where
 * the package build and the published `files` list already drop it) and is
 * still judged by `tsc --noEmit`. A `@ts-expect-error` that stops erroring turns
 * that gate red — the only way a "this must not compile" claim can be honest.
 *
 * What it pins, in one sentence: A HANDLER CANNOT SCHEDULE AGAINST A LOG THAT
 * CANNOT HOLD A FUTURE EVENT. The wiring mistake that used to be a runtime
 * `throw new Error("No event scheduler configured")` — an entry missing one
 * field, discovered by the first deadline anybody armed in production — does
 * not survive the build.
 *
 * It is the mirror of `event-sourcing/__tests__/snapshot-demand.types.ts`, one
 * capability over, and the two are meant to be read side by side.
 */
import { z } from "zod"
import { qn, command, event } from "../../messaging/messages.js"
import { commandHandler } from "../../command-handling/handler.js"
import type { CommandHandlerContext } from "../../command-handling/context.js"
import type { EventHandlerContext } from "../../event-processing/context.js"
import type { QueryHandlerContext } from "../../query-handling/context.js"
import type { CommandHandlerEntry } from "../../kronos.js"
import { localCommandBus } from "../../command-handling/local-bus.js"
import { localQueryBus } from "../../query-handling/local-bus.js"
import { unitOfWork, type UnitOfWork } from "../../unit-of-work/unit-of-work.js"
import { upcastingEventStore } from "../../upcasting/upcasting-event-store.js"
import { inMemoryEventStore } from "../../event-sourcing/in-memory.js"
import { inMemorySnapshottingEventStore } from "../../event-sourcing/in-memory-snapshotting-event-store.js"
import { inMemorySchedulingEventStore } from "../in-memory-scheduling-event-store.js"
import type { EventStore, SnapshotCapableEventStore } from "../../event-sourcing/event-store.js"
import type { ScheduleCapableEventStore } from "../scheduler.js"

const Reminded = event({
  name: qn("probe", "Reminded"),
  payload: z.object({ ticketId: z.string() }),
  tags: { ticketId: (p) => p.ticketId },
})

const Open = command({
  name: qn("probe", "Open"),
  payload: z.object({ ticketId: z.string() }),
})

// ---------------------------------------------------------------------------
// (a) THE VERBS — ALL FOUR QUADRANTS OF (log can schedule?) × (handler asks?).
// ---------------------------------------------------------------------------

/** CAPABLE + SCHEDULES ✓ — the arrangement the demand exists to require. */
export const capablePlusSchedules = commandHandler(
  Open,
  async (m, ctx: CommandHandlerContext<ScheduleCapableEventStore>) => {
    const token = await ctx.schedule(Reminded, { ticketId: m.payload.ticketId }, new Date())
    await ctx.scheduleAfter(Reminded, { ticketId: m.payload.ticketId }, 30_000)
    await ctx.cancelSchedule(token)
  },
)

/** BARE + SILENT ✓ — a project that never arms a deadline is unaffected. */
export const barePlusSilent = commandHandler(Open, async (_m, ctx: CommandHandlerContext) => {
  await ctx.load
  await ctx.source({ tags: { ticketId: "t-1" } })
})

/** CAPABLE + SILENT ✓ — the capability widens; it never narrows. */
export const capablePlusSilent = commandHandler(
  Open,
  async (_m, ctx: CommandHandlerContext<ScheduleCapableEventStore>) => {
    await ctx.source({ tags: { ticketId: "t-1" } })
  },
)

/**
 * BARE + SCHEDULES ✗ — THE HEADLINE.
 *
 * STRUCTURALLY ABSENT, NOT PRESENT-AND-COMPLAINING. `ScheduleVerbs<E>` resolves
 * to `unknown` against a bare log and vanishes from the intersection, so the
 * diagnostic is "Property 'schedule' does not exist on type 'CommandHandlerContext'"
 * — a fact about what this context IS, at the call site, rather than a
 * mismatch inside a signature nobody should have been offered.
 */
export const barePlusSchedules = commandHandler(Open, async (m, ctx: CommandHandlerContext) => {
  // @ts-expect-error — this entry's log cannot hold a future event; wrap it
  await ctx.schedule(Reminded, { ticketId: m.payload.ticketId }, new Date())
  // @ts-expect-error — and the other two verbs are absent for the same reason
  await ctx.scheduleAfter(Reminded, { ticketId: m.payload.ticketId }, 30_000)
  // @ts-expect-error — including the cancel
  await ctx.cancelSchedule({ id: "tok-1" })
})

/** An automation's schedule is a schedule — the event context shares the anchor. */
export const eventContextRefuses = async (ctx: EventHandlerContext) => {
  // @ts-expect-error — an automation arming a deadline needs a log that holds one
  await ctx.schedule(Reminded, { ticketId: "t-1" }, new Date())
}

export const eventContextAccepts = async (
  ctx: EventHandlerContext<ScheduleCapableEventStore>,
) => {
  await ctx.schedule(Reminded, { ticketId: "t-1" }, new Date())
}

/**
 * A QUERY handling has no scheduling verbs AT ALL, capable log or not — and
 * that is a different refusal from the one above. A read does not give birth
 * to anything, so `QueryHandlerContext` never had them and this tier does not
 * hand them out. The demand narrows what a context CAN have; the context still
 * decides what it WANTS.
 */
export const queryContextNeverSchedules = async (
  ctx: QueryHandlerContext<ScheduleCapableEventStore>,
) => {
  // @ts-expect-error — a query handling has no birth verbs, capable log or not
  await ctx.schedule(Reminded, { ticketId: "t-1" }, new Date())
}

// ---------------------------------------------------------------------------
// (b) THE CONTAGION, at the entry. A handler that demands a schedulable log
// does not typecheck into an entry typed for a bare one — which is what makes
// the demand reach the composition root rather than stopping at the handler.
// ---------------------------------------------------------------------------

const bareBus = localCommandBus(unitOfWork)
const bareQueries = localQueryBus(unitOfWork)

export const entryMustCarryASchedulableLog: CommandHandlerEntry<
  UnitOfWork,
  ScheduleCapableEventStore
> = {
  ...capablePlusSchedules,
  commandBus: bareBus,
  queryBus: bareQueries,
  eventStore: inMemorySchedulingEventStore(inMemoryEventStore()),
}

/**
 * The SAME handler, in an entry typed for a bare log. The handler's context
 * demands a capability the entry's `eventStore` does not have, and function
 * parameters are checked contravariantly, so the whole entry is refused.
 *
 * This is the step that carries the demand OUT of the slice: a handler that
 * arms a deadline cannot be placed until the composition root has wrapped
 * something that can hold one.
 */
// @ts-expect-error — a handler demanding a schedulable log does not fit a bare entry
export const bareEntryRejectsASchedulingHandler: CommandHandlerEntry = {
  ...capablePlusSchedules,
  commandBus: bareBus,
  queryBus: bareQueries,
  eventStore: inMemoryEventStore(),
}

// ---------------------------------------------------------------------------
// (c) WRAPPERS DO NOT LAUNDER — NOW THREE TIERS DEEP.
//
// With one capability the rule was easy to satisfy by accident. With two it is
// not: a wrapper that collapsed to its own capability would keep the one it
// adds and silently drop the other, and the failure would look like "why does
// this handler not compile against a store that obviously works".
// ---------------------------------------------------------------------------

const bare = inMemoryEventStore()

/** Each adder is additive on its own. */
export const schedulingAdds: ScheduleCapableEventStore = inMemorySchedulingEventStore(bare)
export const snapshottingAdds: SnapshotCapableEventStore = inMemorySnapshottingEventStore(bare)

/** Upcasting is identity: it hands back exactly what it was given. */
export const upcastingPreservesTheBase: EventStore = upcastingEventStore(bare, (e) => e)

/** BOTH TIERS, SCHEDULING OUTERMOST — both capabilities survive. */
const schedulingOverSnapshotting = inMemorySchedulingEventStore(
  inMemorySnapshottingEventStore(bare),
)
export const stack1Schedules: ScheduleCapableEventStore = schedulingOverSnapshotting
export const stack1Snapshots: SnapshotCapableEventStore = schedulingOverSnapshotting

/** BOTH TIERS, THE OTHER ORDER — same answer. Order is preference, not typing. */
const snapshottingOverScheduling = inMemorySnapshottingEventStore(
  inMemorySchedulingEventStore(bare),
)
export const stack2Schedules: ScheduleCapableEventStore = snapshottingOverScheduling
export const stack2Snapshots: SnapshotCapableEventStore = snapshottingOverScheduling

/** THREE DEEP, the documented arrangement: upcasting outermost over both tiers. */
const threeDeep = upcastingEventStore(
  inMemorySchedulingEventStore(inMemorySnapshottingEventStore(bare)),
  (e) => e,
)
export const threeDeepSchedules: ScheduleCapableEventStore = threeDeep
export const threeDeepSnapshots: SnapshotCapableEventStore = threeDeep

/** THREE DEEP, upcasting in the MIDDLE — a same-seam wrapper is transparent. */
const upcastSandwiched = inMemorySchedulingEventStore(
  upcastingEventStore(inMemorySnapshottingEventStore(bare), (e) => e),
)
export const sandwichedSchedules: ScheduleCapableEventStore = upcastSandwiched
export const sandwichedSnapshots: SnapshotCapableEventStore = upcastSandwiched

/**
 * AND A HANDLER DEMANDING BOTH FITS AN ENTRY BUILT FROM THE WHOLE STACK. This
 * is the claim the two probes make together and neither makes alone: the tiers
 * COMPOSE, and a slice that caches its folds AND arms deadlines needs one
 * `eventStore` on its entry, not two fields and a convention.
 */
export const demandsBoth = commandHandler(
  Open,
  async (m, ctx: CommandHandlerContext<SnapshotCapableEventStore & ScheduleCapableEventStore>) => {
    await ctx.source({ tags: { ticketId: m.payload.ticketId } }, { snapshot: `ticket:${m.payload.ticketId}` })
    await ctx.schedule(Reminded, { ticketId: m.payload.ticketId }, new Date())
  },
)

export const entryCarriesBoth: CommandHandlerEntry<
  UnitOfWork,
  SnapshotCapableEventStore & ScheduleCapableEventStore
> = {
  ...demandsBoth,
  commandBus: bareBus,
  queryBus: bareQueries,
  eventStore: threeDeep,
}

/** And a store with only ONE of them is still refused, so none of this is vacuous. */
// @ts-expect-error — snapshotting alone does not make a log schedulable
export const snapshotOnlyIsNotSchedulable: ScheduleCapableEventStore =
  inMemorySnapshottingEventStore(bare)

// @ts-expect-error — scheduling alone does not make a log snapshot-capable
export const scheduleOnlyIsNotSnapshotting: SnapshotCapableEventStore =
  inMemorySchedulingEventStore(bare)

// @ts-expect-error — and a bare store is neither
export const bareIsNotSchedulable: ScheduleCapableEventStore = bare
