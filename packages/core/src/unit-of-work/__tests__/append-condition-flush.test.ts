/**
 * The append condition a decision over several reads flushes with, and how the
 * in-memory store checks it.
 *
 * - The whole condition is the union of the reads' queries under the EARLIEST
 *   marker, so every read's unseen events are covered.
 * - `reads` carries each read with its own marker, for a store that can check
 *   exactly.
 * - A handler's `appendCondition` override replaces the query, so the per-read
 *   form is dropped.
 */
import { describe, expect, it } from "bun:test"
import { z } from "zod"
import {
  command,
  commandHandler,
  emptyMetadata,
  event,
  inMemoryEventStore,
  kronos,
  localCommandBus,
  localQueryBus,
  qn,
  send,
  state,
  tagsOf,
  unitOfWork,
  AppendConditionError,
  type AppendCondition,
  type EventDescriptor,
  type EventStore,
} from "../../index.js"

const LeftBumped = event({
  name: qn("flush", "LeftBumped"),
  payload: z.object({ leftId: z.string() }),
  tags: { leftId: (p) => p.leftId },
})
const RightBumped = event({
  name: qn("flush", "RightBumped"),
  payload: z.object({ rightId: z.string() }),
  tags: { rightId: (p) => p.rightId },
})
const Decided = event({
  name: qn("flush", "Decided"),
  payload: z.object({ leftId: z.string(), rightId: z.string() }),
  tags: { leftId: (p) => p.leftId, rightId: (p) => p.rightId },
})
const Decide = command({
  name: qn("flush", "Decide"),
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

async function write<P extends z.ZodType>(store: EventStore, descriptor: EventDescriptor<P>, payload: z.output<P>) {
  await store.append([
    {
      kind: "event",
      identifier: crypto.randomUUID(),
      name: descriptor.name,
      version: descriptor.version,
      payload,
      metadata: emptyMetadata(),
      timestamp: 0,
      tags: tagsOf(descriptor, payload, emptyMetadata()),
    },
  ])
}

/** An in-memory store that remembers the last condition it was handed. */
function capturing() {
  const store = inMemoryEventStore()
  const seen: { condition?: AppendCondition } = {}
  const eventStore: EventStore = {
    ...store,
    append: (events, condition, uow) => {
      seen.condition = condition
      return store.append(events, condition, uow)
    },
  }
  return { store, eventStore, seen }
}

const ids = { leftId: "l-1", rightId: "r-1" }

describe("the flushed append condition", () => {
  it("is the union under the earliest marker, with each read under its own", async () => {
    const { store, eventStore, seen } = capturing()
    let between = async () => {}
    const decide = commandHandler(Decide, async ({ payload }, ctx) => {
      await ctx.load(Left, { leftId: payload.leftId })
      await between()
      await ctx.load(Right, { rightId: payload.rightId })
      ctx.append(Decided, payload)
    })
    const commandBus = localCommandBus(unitOfWork)
    const app = kronos({ commandHandlers: [{ ...decide, eventStore, commandBus, queryBus: localQueryBus(unitOfWork) }] })
    try {
      await write(store, LeftBumped, { leftId: "l-1" }) // position 0
      between = () => write(store, RightBumped, { rightId: "r-1" }) // position 1, between the loads
      await send(commandBus, Decide, ids)

      const condition = seen.condition!
      expect(condition.marker.position).toBe(0n)
      expect(condition.reads?.map((r) => r.marker.position)).toEqual([0n, 1n])
      expect(condition.reads?.map((r) => r.query)).toEqual([Left.query({ leftId: "l-1" }), Right.query({ rightId: "r-1" })])
    } finally {
      await app.stop()
    }
  })

  it("drops the per-read form when the handler overrides the query", async () => {
    const { store, eventStore, seen } = capturing()
    const decide = commandHandler(Decide, {
      handler: async ({ payload }, ctx) => {
        await ctx.load(Left, { leftId: payload.leftId })
        await ctx.load(Right, { rightId: payload.rightId })
        ctx.append(Decided, payload)
      },
      appendCondition: () => ({ tags: { leftId: "l-1" } }),
    })
    const commandBus = localCommandBus(unitOfWork)
    const app = kronos({ commandHandlers: [{ ...decide, eventStore, commandBus, queryBus: localQueryBus(unitOfWork) }] })
    try {
      await write(store, LeftBumped, { leftId: "l-1" })
      await write(store, RightBumped, { rightId: "r-1" })
      await send(commandBus, Decide, ids)

      const condition = seen.condition!
      expect(condition.query).toEqual({ tags: { leftId: "l-1" } })
      expect(condition.reads).toBeUndefined()
      expect(condition.marker.position).toBe(1n)
    } finally {
      await app.stop()
    }
  })
})

describe("the in-memory store's check of `reads`", () => {
  const onLeft = { tags: { leftId: "l-1" } }
  const onRight = { tags: { rightId: "r-1" } }
  const decision = (store: EventStore, condition: AppendCondition) =>
    store.append(
      [
        {
          kind: "event",
          identifier: crypto.randomUUID(),
          name: Decided.name,
          version: Decided.version,
          payload: ids,
          metadata: emptyMetadata(),
          timestamp: 0,
          tags: tagsOf(Decided, ids, emptyMetadata()),
        },
      ],
      condition,
    )

  it("accepts an event only a later read's marker covers", async () => {
    const store = inMemoryEventStore()
    await write(store, LeftBumped, { leftId: "l-1" }) // 0
    await write(store, RightBumped, { rightId: "r-1" }) // 1 — the Right read saw it
    await decision(store, {
      query: [onLeft, onRight],
      marker: { position: 0n },
      reads: [
        { query: onLeft, marker: { position: 0n } },
        { query: onRight, marker: { position: 1n } },
      ],
    })
  })

  it("refuses an event after its own read's marker", async () => {
    const store = inMemoryEventStore()
    await write(store, LeftBumped, { leftId: "l-1" }) // 0
    await write(store, LeftBumped, { leftId: "l-1" }) // 1 — after the Left read
    await write(store, RightBumped, { rightId: "r-1" }) // 2
    let error: unknown
    try {
      await decision(store, {
        query: [onLeft, onRight],
        marker: { position: 0n },
        reads: [
          { query: onLeft, marker: { position: 0n } },
          { query: onRight, marker: { position: 2n } },
        ],
      })
    } catch (err) {
      error = err
    }
    expect(error).toBeInstanceOf(AppendConditionError)
  })

  it("without `reads`, checks the whole query against the one marker", async () => {
    const store = inMemoryEventStore()
    await write(store, LeftBumped, { leftId: "l-1" }) // 0
    await write(store, RightBumped, { rightId: "r-1" }) // 1
    let error: unknown
    try {
      await decision(store, { query: [onLeft, onRight], marker: { position: 0n } })
    } catch (err) {
      error = err
    }
    expect(error).toBeInstanceOf(AppendConditionError)
  })
})
