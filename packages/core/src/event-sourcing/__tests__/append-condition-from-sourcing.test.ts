/**
 * Covers the append-condition flow derived from sourced state — the query +
 * marker that the framework attaches to append() so the event store can reject
 * stale-state writes.
 *
 * Composition: `kronos` takes a plain `Components` record, so a
 * probe-wrapped event store is passed in directly — it is the `eventStore`
 * field, not a slot factory registered under a string key.
 */
import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, emptyMetadata, command, event, type EventMessage } from "../../messaging/messages.js"
import { commandHandler } from "../../command-handling/handler.js"
import { state } from "../state.js"
import { kronos } from "../../kronos.js"
import { send, correlation, interceptingCommandBus, interceptingQueryBus, unitOfWork, localCommandBus, localQueryBus } from "../../index.js"
import { inMemoryEventStore } from "../in-memory.js"
import type { EventStore } from "../event-store.js"
import type { AppendCondition } from "../append-condition.js"

/**
 * The three things `kronos` needs that are not modules. The UoW runner is named
 * once and handed to BOTH `localCommandBus` (which captures it at construction)
 * and `kronos` — writing them on adjacent lines is what makes that checkable.
 */
function inMemoryBuses(uow: () => UnitOfWork = unitOfWork) {
  return {
    commandBus: interceptingCommandBus(localCommandBus(uow), correlation),
    queryBus: interceptingQueryBus(localQueryBus(uow), correlation),
  }
}


// ─── Domain ─────────────────────────────────────────────────────────────────

const TouchThing = command({
  name: qn("ac", "TouchThing"),
  payload: z.object({ id: z.string() }),
})
const ThingTouched = event({
  name: qn("ac", "ThingTouched"),
  payload: z.object({ id: z.string() }),
  tags: { id: (p) => p.id },
})
const Thing = state({
  id: { id: z.string() },
  tags: ({ id }) => ({ id: id }),
  evolve: [() => ({ touched: false }), [ThingTouched, (s) => ({ ...s, touched: true })]],
})
const touchThing = commandHandler(TouchThing, async ({ payload: cmd }, ctx) => {
  await ctx.load(Thing, { id: cmd.id })
  ctx.append(ThingTouched, { id: cmd.id })
})

// ─── Probe wrapper ──────────────────────────────────────────────────────────

type AppendRecord = {
  events: ReadonlyArray<EventMessage>
  condition: AppendCondition | undefined
}

function probeEventStore(): EventStore & { records: AppendRecord[] } {
  const inner = inMemoryEventStore()
  const records: AppendRecord[] = []
  const wrapped: EventStore & { records: AppendRecord[] } = {
    records,
    source: inner.source.bind(inner),
    open: inner.open.bind(inner),
    appendEvents: inner.appendEvents.bind(inner),
    firstToken: inner.firstToken.bind(inner),
    latestToken: inner.latestToken.bind(inner),
    getHeadPosition: inner.getHeadPosition.bind(inner),
    async append(events, condition) {
      records.push({ events, condition })
      return inner.append(events, condition)
    },
  }
  return wrapped
}

// ─── Tests ──────────────────────────────────────────────────────────────────

describe("Append condition derived from sourced state — probe event store as a component", () => {
  it("appendCondition captures the sourcing query from the loaded entity", async () => {
    const probe = probeEventStore()
    const buses = inMemoryBuses()
    const app = kronos({
      commandHandlers: [{ ...touchThing, ...buses, eventStore: probe }],
    })
    try {
      await send(
        buses.commandBus,
        TouchThing,
        { id: "ac-1" },
        emptyMetadata(),
      )
      // The framework derived an AppendCondition from the sourced state of
      // AcThing { id: "ac-1" }. The query must mention the entity's tag.
      expect(probe.records.length).toBeGreaterThan(0)
      const last = probe.records[probe.records.length - 1]!
      expect(last.condition).toBeDefined()
      const query = last.condition!.query
      // The query is either one item or a list of them; both carry the tag.
      const json = JSON.stringify(query)
      expect(json).toContain("ac-1")
    } finally {
      await app.stop()
    }
  })

  it("detects concurrent modification via append condition (probe surfaces the marker)", async () => {
    const probe = probeEventStore()
    const buses = inMemoryBuses()
    const app = kronos({
      commandHandlers: [{ ...touchThing, ...buses, eventStore: probe }],
    })
    try {
      // First touch — should record a condition with a marker (position-based).
      await send(
        buses.commandBus,
        TouchThing,
        { id: "ac-2" },
        emptyMetadata(),
      )
      expect(probe.records.length).toBeGreaterThan(0)
      const recorded = probe.records[probe.records.length - 1]!
      expect(recorded.condition).toBeDefined()
      // The condition has a marker attached (the framework derived it from
      // the sourcing result, which carries the highest known position).
      expect(recorded.condition!.marker).toBeDefined()
    } finally {
      await app.stop()
    }
  })
})
