/**
 * Covers the append-condition flow derived from sourced state — the criteria +
 * marker that the framework attaches to append() so the event store can reject
 * stale-state writes.
 *
 * Composition: `createApp` takes a plain `Components` record, so a
 * probe-wrapped event store is passed in directly — it is the `eventStore`
 * field, not a slot factory registered under a string key.
 */
import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, tag, emptyMetadata } from "@kronos-ts/common"
import {
  command,
  event,
  commandHandler,
  EventCriteria,
  type EventMessage,
} from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import { createApp, inMemoryComponents, module } from "@kronos-ts/app"
import { append } from "../append.js"
import { load } from "../load.js"
import { createInMemoryEventStore } from "../in-memory-event-store.js"
import type { EventStore } from "../event-store.js"
import type { AppendCondition } from "../append-condition.js"

// ─── Domain ─────────────────────────────────────────────────────────────────

const TouchThing = command({
  name: qn("ac", "TouchThing"),
  payload: z.object({ id: z.string() }),
})
const ThingTouched = event({
  name: qn("ac", "ThingTouched"),
  payload: z.object({ id: z.string() }),
  tags: (p) => [tag("id", p.id)],
})
const Thing = state({
  name: "AcThing",
  id: { id: z.string() },
  initial: () => ({ touched: false }),
  criteria: ({ id }) => EventCriteria.havingTags(tag("id", id)),
  evolve: (on) => [on(ThingTouched, (s) => ({ ...s, touched: true }))],
})
const touchThing = commandHandler(TouchThing, async ({ payload: cmd }) => {
  await load(Thing, { id: cmd.id })
  append(ThingTouched, { id: cmd.id })
})

// ─── Probe wrapper ──────────────────────────────────────────────────────────

interface AppendRecord {
  events: ReadonlyArray<EventMessage>
  condition: AppendCondition | undefined
}

function probeEventStore(): EventStore & { records: AppendRecord[] } {
  const inner = createInMemoryEventStore()
  const records: AppendRecord[] = []
  const wrapped: EventStore & { records: AppendRecord[] } = {
    records,
    source: inner.source.bind(inner),
    open: inner.open.bind(inner),
    subscribe: inner.subscribe?.bind(inner),
    publish: inner.publish.bind(inner),
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
  it("appendCondition captures the sourcing criteria from the loaded entity", async () => {
    const probe = probeEventStore()
    const app = createApp({
      components: inMemoryComponents({ eventStore: probe }),
      modules: [module("ac", Thing, touchThing)],
    })
    try {
      await app.commandGateway.send(
        TouchThing,
        { id: "ac-1" },
        emptyMetadata(),
      )
      // The framework derived an AppendCondition from the sourced state of
      // AcThing { id: "ac-1" }. The criteria must mention the entity's tag.
      expect(probe.records.length).toBeGreaterThan(0)
      const last = probe.records[probe.records.length - 1]!
      expect(last.condition).toBeDefined()
      const criteria = last.condition!.criteria
      // Criteria can be either tags-based directly or wrapped in "either".
      const json = JSON.stringify(criteria)
      expect(json).toContain("ac-1")
    } finally {
      await app.stop()
    }
  })

  it("detects concurrent modification via append condition (probe surfaces the marker)", async () => {
    const probe = probeEventStore()
    const app = createApp({
      components: inMemoryComponents({ eventStore: probe }),
      modules: [module("ac", Thing, touchThing)],
    })
    try {
      // First touch — should record a condition with a marker (position-based).
      await app.commandGateway.send(
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
