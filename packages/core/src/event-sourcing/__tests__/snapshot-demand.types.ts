/**
 * THE TYPE TEST FOR SNAPSHOTTING — and what it pins is an ABSENCE.
 *
 * Snapshotting is a STORE tier with no context capability. A handler has
 * nothing new to call: `state({ snapshot })` says a state wants caching, the
 * wrapped log serves it through `ctx.load`, and nothing on any context changes
 * shape when a host wraps the log. So the claims here are:
 *
 *   - a BARE `CommandHandlerContext` loads a snapshot-policy state without a
 *     type argument — the demand that used to force
 *     `CommandHandlerContext<SnapshotCapableEventStore>` is gone;
 *   - `ctx.source` has ONE signature everywhere;
 *   - the store-side tier still exists, is additive, and wrappers do not
 *     launder it.
 *
 * Judged by `bunx tsc --noEmit` through the root tsconfig `files` array; a
 * `@ts-expect-error` that stops erroring turns the gate red. The runtime half
 * of the story — a policy loaded through a bare log throws, loudly, on the
 * first load — is `capableOrThrow` in `repository.ts`, covered by
 * `repository-snapshots.test.ts`.
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
import { unitOfWork } from "../../unit-of-work/unit-of-work.js"
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

const Caching = state({
  id: { counterId: z.string() },
  tags: ({ counterId }) => ({ counterId }),
  evolve: [() => ({ count: 0 }), [Bumped, (s) => ({ count: s.count + 1 })]],
  snapshot: { key: "counter-v1", when: afterEvents(100) },
})

const Plain = state({
  id: { counterId: z.string() },
  tags: ({ counterId }) => ({ counterId }),
  evolve: [() => ({ count: 0 }), [Bumped, (s) => ({ count: s.count + 1 })]],
})

// ---------------------------------------------------------------------------
// (0) `state()` STILL INFERS THE THIRD PARAMETER. The repository reads it at
// runtime to decide whether to ask the log for a snapshot.
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
// (a) ctx.load — NO DEMAND. A bare context loads either state; the store the
// entry wires decides at runtime whether the policy is served.
// ---------------------------------------------------------------------------

export const bareLoadsCaching = commandHandler(Bump, async (_m, ctx: CommandHandlerContext) => {
  await ctx.load(Caching, { counterId: "c-1" })
  await ctx.load(Plain, { counterId: "c-1" })
})

export const queryContextLoadsCaching = async (ctx: QueryHandlerContext) => {
  await ctx.load(Caching, { counterId: "c-1" })
}

export const eventContextLoadsCaching = async (ctx: EventHandlerContext) => {
  await ctx.load(Caching, { counterId: "c-1" })
}

// The parameter spelling is still legal — `E` is the supply side the entry
// threads in — and means nothing more than the bare one for this tier.
export const parameterSpellingIsHarmless = commandHandler(
  Bump,
  async (_m, ctx: CommandHandlerContext<SnapshotCapableEventStore>) => {
    await ctx.load(Caching, { counterId: "c-1" })
  },
)

// ---------------------------------------------------------------------------
// (b) ctx.source — ONE SIGNATURE, capable log or not.
// ---------------------------------------------------------------------------

export const plainSourceAlwaysWorks = async (ctx: CommandHandlerContext) => {
  const events = await ctx.source({ tags: { counterId: "c-1" } })
  return events.length
}

export const noFusedOverloadOnAnyContext = async (
  ctx: CommandHandlerContext<SnapshotCapableEventStore>,
) => {
  // @ts-expect-error — a context has no fused read; the tier lives on the store
  await ctx.source({ tags: { counterId: "c-1" } }, { snapshot: "counter:c-1" })
}

// ---------------------------------------------------------------------------
// (c) NO CONTAGION. A handler that loads a caching state fits a BARE entry —
// which store it actually runs against is the composition root's business.
// ---------------------------------------------------------------------------

const bareBus = localCommandBus(unitOfWork)
const bareQueries = localQueryBus(unitOfWork)

export const bareEntryAcceptsACachingHandler: CommandHandlerEntry = {
  ...bareLoadsCaching,
  commandBus: bareBus,
  queryBus: bareQueries,
  eventStore: inMemoryEventStore(),
}

export const capableEntryAcceptsItToo: CommandHandlerEntry<typeof unitOfWork extends () => infer U ? U : never, SnapshotCapableEventStore> = {
  ...bareLoadsCaching,
  commandBus: bareBus,
  queryBus: bareQueries,
  eventStore: inMemorySnapshottingEventStore(inMemoryEventStore()),
}

// ---------------------------------------------------------------------------
// (d) THE STORE TIER IS REAL, ADDITIVE, AND NOT LAUNDERED. This is where the
// snapshotting capability lives, and the only place a type says so.
// ---------------------------------------------------------------------------

const bare = inMemoryEventStore()

export const inMemoryAdds: SnapshotCapableEventStore = inMemorySnapshottingEventStore(bare)

export const writeHalfIsOnTheLog = async (store: SnapshotCapableEventStore) => {
  await store.storeSnapshot("counter:c-1", { state: { count: 1 }, position: 3n })
}

export const writeHalfAbsentOnBareLog = async (store: EventStore) => {
  // @ts-expect-error — the base contract mentions snapshots nowhere
  await store.storeSnapshot("counter:c-1", { state: { count: 1 }, position: 3n })
}

export const upcastingPreservesTheBase: EventStore = upcastingEventStore(bare, (e) => e)

export const upcastOutermost: SnapshotCapableEventStore = upcastingEventStore(
  inMemorySnapshottingEventStore(bare),
  (e) => e,
)

export const upcastInnermost: SnapshotCapableEventStore = inMemorySnapshottingEventStore(
  upcastingEventStore(bare, (e) => e),
)

// @ts-expect-error — nothing added the capability
export const bareIsNotCapable: SnapshotCapableEventStore = bare
