/**
 * THE TYPE TEST FOR THE SNAPSHOTTING DEMAND.
 *
 * Every claim here is a compile-time one, so the test IS the typecheck: this
 * file is listed in the root `tsconfig.json` `files` array, which is not
 * subject to `exclude`, so it lives beside its runtime siblings in `__tests__`
 * (where the package build and the published `files` list already drop it) and
 * is still judged by `bunx tsc --noEmit`. A `@ts-expect-error` that stops
 * erroring turns that gate red — the only way a "this must not compile" claim
 * can be honest.
 *
 * What it pins, in one sentence: A STATE THAT DECLARES A SNAPSHOT POLICY CANNOT
 * BE LOADED THROUGH A LOG THAT CANNOT SERVE ONE. The wiring mistake that used
 * to be a silent full replay — a policy on the state, nothing wrapped at the
 * composition root, and a cache nobody ever read — is now a build error naming
 * the fix.
 *
 * And the mirror of it: the fused `ctx.source(query, { snapshot })` overload
 * does not EXIST on a context whose log is bare. Not "exists and complains" —
 * is absent, so asking for it is a no-such-signature error rather than a
 * mismatch inside a signature nobody should have been offered.
 *
 * Both faces come from ONE alias, `IfSnapshotCapable`, which is the anchor
 * anything later hangs off. See `../load.ts`.
 */
import { z } from "zod"
import { qn, command, event } from "../../messaging/messages.js"
import { commandHandler } from "../../command-handling/handler.js"
import type { CommandHandlerContext } from "../../command-handling/context.js"
import type { QueryHandlerContext } from "../../query-handling/context.js"
import type { EventHandlerContext } from "../../event-processing/context.js"
import type { CommandHandlerEntry } from "../../kronos.js"
import { localCommandBus } from "../../command-handling/local-bus.js"
import { localQueryBus } from "../../query-handling/local-bus.js"
import { unitOfWork, type UnitOfWork } from "../../unit-of-work/unit-of-work.js"
import { upcastingEventStore } from "../../upcasting/upcasting-event-store.js"
import { inMemoryEventStore } from "../in-memory.js"
import { inMemorySnapshottingEventStore } from "../in-memory-snapshotting-event-store.js"
import type { EventStore, SnapshotCapableEventStore } from "../event-store.js"
import { state } from "../state.js"
import { afterEvents } from "../snapshot.js"

const Bumped = event({
  name: qn("probe", "Bumped"),
  payload: z.object({ counterId: z.string() }),
  tags: { counterId: (p) => p.counterId },
})

const Bump = command({
  name: qn("probe", "Bump"),
  payload: z.object({ counterId: z.string() }),
})

/** A fold that CACHES — `state()` reads `Snap = true` off the config. */
const Caching = state({
  id: { counterId: z.string() },
  tags: ({ counterId }) => ({ counterId }),
  evolve: [() => ({ count: 0 }), [Bumped, (s) => ({ count: s.count + 1 })]],
  snapshot: { key: "counter-v1", when: afterEvents(100) },
})

/** The SAME fold without one — `Snap = false`, and it never meets the concept. */
const Plain = state({
  id: { counterId: z.string() },
  tags: ({ counterId }) => ({ counterId }),
  evolve: [() => ({ count: 0 }), [Bumped, (s) => ({ count: s.count + 1 })]],
})

// ---------------------------------------------------------------------------
// (0) `state()` INFERS THE THIRD PARAMETER. Everything below stands on this
// one inference, and a host writes nothing to get it.
// ---------------------------------------------------------------------------

export const cachingSaysTrue: typeof Caching extends { snapshot?: infer C }
  ? C extends { key: string } | undefined
    ? true
    : false
  : false = true

export const plainSaysUndefined: typeof Plain extends { snapshot?: infer C }
  ? [C] extends [undefined]
    ? true
    : false
  : false = true

// ---------------------------------------------------------------------------
// (a) ctx.load — ALL FOUR QUADRANTS.
// ---------------------------------------------------------------------------

/** CAPABLE + SNAPSHOTTING ✓ — the arrangement the demand exists to require. */
export const capablePlusCaching = commandHandler(
  Bump,
  async (_m, ctx: CommandHandlerContext<SnapshotCapableEventStore>) => {
    await ctx.load(Caching, { counterId: "c-1" })
  },
)

/** BARE + PLAIN ✓ — a project that never heard of snapshotting, unaffected. */
export const barePlusPlain = commandHandler(Bump, async (_m, ctx: CommandHandlerContext) => {
  await ctx.load(Plain, { counterId: "c-1" })
})

/** CAPABLE + PLAIN ✓ — the capability widens; it never narrows. */
export const capablePlusPlain = commandHandler(
  Bump,
  async (_m, ctx: CommandHandlerContext<SnapshotCapableEventStore>) => {
    await ctx.load(Plain, { counterId: "c-1" })
  },
)

/** BARE + SNAPSHOTTING ✗ — THE HEADLINE. */
export const barePlusCaching = commandHandler(Bump, async (_m, ctx: CommandHandlerContext) => {
  // @ts-expect-error — this state declares a snapshot policy; wrap the entry's eventStore
  await ctx.load(Caching, { counterId: "c-1" })
})

/** The same four hold on the other two contexts, because they share the anchor. */
export const queryContextRefuses = async (ctx: QueryHandlerContext) => {
  await ctx.load(Plain, { counterId: "c-1" })
  // @ts-expect-error — a query handling reads the same log, under the same demand
  await ctx.load(Caching, { counterId: "c-1" })
}

export const eventContextRefuses = async (ctx: EventHandlerContext) => {
  await ctx.load(Plain, { counterId: "c-1" })
  // @ts-expect-error — an automation's load is a load
  await ctx.load(Caching, { counterId: "c-1" })
}

export const eventContextAccepts = async (
  ctx: EventHandlerContext<SnapshotCapableEventStore>,
) => {
  await ctx.load(Caching, { counterId: "c-1" })
}

// ---------------------------------------------------------------------------
// (b) ctx.source — THE OVERLOAD IS ABSENT, NOT BROKEN.
// ---------------------------------------------------------------------------

export const plainSourceAlwaysWorks = async (ctx: CommandHandlerContext) => {
  const events = await ctx.source({ tags: { counterId: "c-1" } })
  return events.length
}

export const fusedSourceNeedsCapability = async (
  ctx: CommandHandlerContext<SnapshotCapableEventStore>,
) => {
  const { snapshot, events, position } = await ctx.source(
    { tags: { counterId: "c-1" } },
    { snapshot: "counter:c-1" },
  )
  return { snapshot, events, position }
}

export const fusedSourceRefusedOnBareLog = async (ctx: CommandHandlerContext) => {
  // @ts-expect-error — a bare log has no fused read; this call takes ONE argument
  await ctx.source({ tags: { counterId: "c-1" } }, { snapshot: "counter:c-1" })
}

/** And the WRITE half is reachable from the same object, on a capable log only. */
export const writeHalfIsOnTheLog = async (store: SnapshotCapableEventStore) => {
  await store.storeSnapshot("counter:c-1", { state: { count: 1 }, position: 3n })
}

export const writeHalfAbsentOnBareLog = async (store: EventStore) => {
  // @ts-expect-error — the base contract mentions snapshots nowhere
  await store.storeSnapshot("counter:c-1", { state: { count: 1 }, position: 3n })
}

// ---------------------------------------------------------------------------
// (c) THE CONTAGION, at the entry. A handler that demands a capable log does
// not typecheck into an entry typed for a bare one — which is what makes the
// demand reach the composition root rather than stopping at the handler.
// ---------------------------------------------------------------------------

const bareBus = localCommandBus(unitOfWork)
const bareQueries = localQueryBus(unitOfWork)

export const entryMustCarryACapableLog: CommandHandlerEntry<
  UnitOfWork,
  SnapshotCapableEventStore
> = {
  ...capablePlusCaching,
  commandBus: bareBus,
  queryBus: bareQueries,
  eventStore: inMemorySnapshottingEventStore(inMemoryEventStore()),
}

/**
 * The SAME handler, in an entry typed for a bare log. The handler's context
 * demands a capability the entry's `eventStore` does not have, and function
 * parameters are checked contravariantly, so the whole entry is refused.
 *
 * This is the step that carries the demand OUT of the slice: a handler cannot
 * be placed until the composition root has wrapped something.
 */
// @ts-expect-error — a handler demanding a capable log does not fit a bare entry
export const bareEntryRejectsACachingHandler: CommandHandlerEntry = {
  ...capablePlusCaching,
  commandBus: bareBus,
  queryBus: bareQueries,
  eventStore: inMemoryEventStore(),
}

// ---------------------------------------------------------------------------
// (d) WRAPPERS DO NOT LAUNDER. A wrapper whose input and output are the same
// seam must be a GENERIC IDENTITY, and a capability adder must be ADDITIVE —
// otherwise the runtime keeps a capability the type threw away, and the demand
// above rejects a configuration that works perfectly.
// ---------------------------------------------------------------------------

const bare = inMemoryEventStore()

/** The adder is additive: what comes back satisfies the capability. */
export const inMemoryAdds: SnapshotCapableEventStore = inMemorySnapshottingEventStore(bare)

/** Upcasting is identity: it hands back exactly what it was given. */
export const upcastingPreservesTheBase: EventStore = upcastingEventStore(bare, (e) => e)

/** SNAPSHOTTING INSIDE UPCASTING — the documented order — keeps both. */
export const upcastOutermost: SnapshotCapableEventStore = upcastingEventStore(
  inMemorySnapshottingEventStore(bare),
  (e) => e,
)

/** UPCASTING INSIDE SNAPSHOTTING — the other order — keeps both too. */
export const upcastInnermost: SnapshotCapableEventStore = inMemorySnapshottingEventStore(
  upcastingEventStore(bare, (e) => e),
)

/** And a bare store is still refused, so the probe above is not vacuous. */
// @ts-expect-error — nothing added the capability
export const bareIsNotCapable: SnapshotCapableEventStore = bare
