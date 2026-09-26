/**
 * THE APPEND CONDITION OF A DECISION OVER TWO STATES, against any store.
 *
 * A command handler loads `Left`, then `Right`, then appends. Between the two
 * loads a hook may write straight to the store — another writer, landing while
 * the decision is half-read. Each store family runs the same three scenarios:
 *
 * 1. Sequential, no interference, the two states' newest events at different
 *    positions: the decision commits. (A single boundary taken from the
 *    EARLIEST read refuses this on a store whose marker is the last matching
 *    event, on every attempt.)
 * 2. `Left` changes between the loads, and `Right` changes after it: the
 *    decision is refused. (A single boundary taken from the LATEST read lets
 *    it through — a decision committed on a stale `Left`.)
 * 3. Only `Right` changes between the loads: its own read saw that change, so
 *    it is not a conflict. A store that checks each read against its own
 *    marker commits; a store with one boundary for the whole condition refuses
 *    — a retry, never a wrong decision.
 */
import assert from "node:assert/strict"
import { afterEach, it } from "bun:test"
import { z } from "zod"
import {
  command,
  commandHandler,
  correlation,
  emptyMetadata,
  event,
  interceptingCommandBus,
  interceptingQueryBus,
  kronos,
  localCommandBus,
  localQueryBus,
  qn,
  send,
  state,
  tagsOf,
  unitOfWork,
  type App,
  type EventDescriptor,
  type EventStore,
  type UnitOfWork,
} from "@kronos-ts/core"

const LeftBumped = event({
  name: qn("markers", "LeftBumped"),
  payload: z.object({ leftId: z.string() }),
  tags: { leftId: (p) => p.leftId },
})
const RightBumped = event({
  name: qn("markers", "RightBumped"),
  payload: z.object({ rightId: z.string() }),
  tags: { rightId: (p) => p.rightId },
})
const Decided = event({
  name: qn("markers", "Decided"),
  payload: z.object({ leftId: z.string(), rightId: z.string() }),
  tags: { leftId: (p) => p.leftId, rightId: (p) => p.rightId },
})
const Decide = command({
  name: qn("markers", "Decide"),
  payload: z.object({ leftId: z.string(), rightId: z.string() }),
})

const Left = state({
  id: { leftId: z.string() },
  tags: (id) => ({ leftId: id.leftId }),
  evolve: [() => ({ bumps: 0 }), [LeftBumped, (s) => ({ bumps: s.bumps + 1 })]],
})
const Right = state({
  id: { rightId: z.string() },
  tags: (id) => ({ rightId: id.rightId }),
  evolve: [() => ({ bumps: 0 }), [RightBumped, (s) => ({ bumps: s.bumps + 1 })]],
})

type Ids = { readonly leftId: string; readonly rightId: string }

export type AppendConditionTarget = {
  /** The store every scenario reads and writes. Called once per scenario. */
  readonly eventStore: () => EventStore
  /** The command seam's unit-of-work factory; the plain one when absent. */
  readonly unitOfWork?: () => () => UnitOfWork
  /** Whether the store checks each read against its own marker. */
  readonly exact: boolean
}

let sequence = 0
function freshIds(): Ids {
  const run = `${Date.now().toString(36)}-${++sequence}-${Math.random().toString(36).slice(2, 8)}`
  return { leftId: `left-${run}`, rightId: `right-${run}` }
}

async function write<P extends z.ZodType>(
  store: EventStore,
  descriptor: EventDescriptor<P>,
  payload: z.output<P>,
): Promise<void> {
  await store.append([
    {
      kind: "event",
      identifier: crypto.randomUUID(),
      name: descriptor.name,
      version: descriptor.version,
      payload,
      metadata: emptyMetadata(),
      timestamp: Date.now(),
      tags: tagsOf(descriptor, payload, emptyMetadata()),
    },
  ])
}

function isConflict(err: unknown): boolean {
  const text = `${(err as Error)?.name ?? ""} ${(err as Error)?.message ?? ""}`
  return /append ?condition|conflict|consistency/i.test(text)
}

export function appendConditionScenarios(target: AppendConditionTarget): void {
  let between: (store: EventStore, ids: Ids) => Promise<void> = async () => {}
  let app: App | undefined

  afterEach(async () => {
    between = async () => {}
    await app?.stop()
    app = undefined
  })

  function wire(): { eventStore: EventStore; decide: (ids: Ids) => Promise<unknown> } {
    const eventStore = target.eventStore()
    const decide = commandHandler(Decide, async ({ payload }, ctx) => {
      await ctx.load(Left, { leftId: payload.leftId })
      await between(eventStore, payload)
      await ctx.load(Right, { rightId: payload.rightId })
      ctx.append(Decided, payload)
    })
    const commandBus = interceptingCommandBus(localCommandBus(target.unitOfWork?.() ?? unitOfWork), correlation)
    const queryBus = interceptingQueryBus(localQueryBus(unitOfWork), correlation)
    app = kronos({ commandHandlers: [{ ...decide, eventStore, commandBus, queryBus }] })
    return { eventStore, decide: (ids) => send(commandBus, Decide, ids) }
  }

  it("commits a decision over two states whose newest events sit at different positions", async () => {
    const { eventStore, decide } = wire()
    const ids = freshIds()
    await write(eventStore, LeftBumped, { leftId: ids.leftId })
    await write(eventStore, RightBumped, { rightId: ids.rightId })

    await decide(ids)
  })

  it("refuses a decision when the first state changed between the two loads", async () => {
    const { eventStore, decide } = wire()
    const ids = freshIds()
    await write(eventStore, LeftBumped, { leftId: ids.leftId })
    await write(eventStore, RightBumped, { rightId: ids.rightId })
    between = async (store, { leftId, rightId }) => {
      await write(store, LeftBumped, { leftId })
      await write(store, RightBumped, { rightId })
    }

    await assert.rejects(decide(ids), isConflict)
  })

  it(
    target.exact
      ? "commits when only the second state changed between the loads — its own read saw it"
      : "refuses when only the second state changed between the loads — one boundary for the whole condition",
    async () => {
      const { eventStore, decide } = wire()
      const ids = freshIds()
      await write(eventStore, LeftBumped, { leftId: ids.leftId })
      between = async (store, { rightId }) => {
        await write(store, RightBumped, { rightId })
      }

      if (target.exact) await decide(ids)
      else await assert.rejects(decide(ids), isConflict)
    },
  )
}
