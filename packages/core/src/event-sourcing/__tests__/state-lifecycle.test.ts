import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, emptyMetadata, event, type EventMessage } from "../../messaging/messages.js"
import { generateIdentifier } from "../../messaging/identifier.js"
import { state } from "../state.js"
import { inMemoryEventStore } from "../in-memory.js"
import { eventSourcedRepository } from "../repository.js"

// -- Fixtures --

const ItemCreated = event({
  name: qn("test", "ItemCreated"),
  payload: z.object({ itemId: z.string(), name: z.string() }),
  tags: { itemId: (p) => p.itemId },
})

const ItemRenamed = event({
  name: qn("test", "ItemRenamed"),
  payload: z.object({ itemId: z.string(), name: z.string() }),
  tags: { itemId: (p) => p.itemId },
})

const ItemDeleted = event({
  name: qn("test", "ItemDeleted"),
  payload: z.object({ itemId: z.string() }),
  tags: { itemId: (p) => p.itemId },
})

type ItemState = { created: boolean; name: string; deleted: boolean }

function eventMsg(descriptor: any, payload: any): EventMessage {
  const tags = descriptor.tags ? descriptor.tags(payload) : []
  return {
    identifier: generateIdentifier(),
    name: qn(descriptor.name.namespace, descriptor.name.name),
    version: descriptor.version ?? "1.0",
    payload,
    metadata: emptyMetadata(),
    timestamp: Date.now(),
    tags,
  }
}

// -- Tests --

describe("Entity Lifecycle Hooks", () => {
  describe("onCreate", () => {
    it("fires when first event creates the entity", async () => {
      // given
      const created: Array<{ state: ItemState; id: { itemId: string } }> = []

      const Item = state({
        id: { itemId: z.string() },
        tags: (id) => ({ itemId: id.itemId }),
        evolve: [
          () => ({ created: false, name: "", deleted: false }) as ItemState,
          [ItemCreated, (state, { payload: e }) => ({ ...state, created: true, name: e.name })],
        ],
        lifecycle: {
          onCreate: (state, id) => { created.push({ state: { ...state }, id }) },
        },
      })

      const eventStore = inMemoryEventStore()
      await eventStore.append([
        eventMsg(ItemCreated, { itemId: "i-1", name: "Widget" }),
      ])

      const repo = eventSourcedRepository(Item, eventStore)

      // when
      await repo.load({ itemId: "i-1" })

      // then
      expect(created).toHaveLength(1)
      expect(created[0]!.state.name).toBe("Widget")
      expect(created[0]!.id).toEqual({ itemId: "i-1" })
    })
  })

  describe("onStateChange", () => {
    it("fires after each state-changing event", async () => {
      // given
      const changes: Array<{ from: string; to: string }> = []

      const Item = state({
        id: { itemId: z.string() },
        tags: (id) => ({ itemId: id.itemId }),
        evolve: [
          () => ({ created: false, name: "", deleted: false }) as ItemState,
          [ItemCreated, (state, { payload: e }) => ({ ...state, created: true, name: e.name })],
          [ItemRenamed, (state, { payload: e }) => ({ ...state, name: e.name })],
        ],
        lifecycle: {
          onStateChange: (from, to) => {
            changes.push({ from: from.name, to: to.name })
          },
        },
      })

      const eventStore = inMemoryEventStore()
      await eventStore.append([
        eventMsg(ItemCreated, { itemId: "i-1", name: "Widget" }),
        eventMsg(ItemRenamed, { itemId: "i-1", name: "Gadget" }),
      ])

      const repo = eventSourcedRepository(Item, eventStore)

      // when
      await repo.load({ itemId: "i-1" })

      // then
      expect(changes).toHaveLength(2)
      expect(changes[0]).toEqual({ from: "", to: "Widget" })
      expect(changes[1]).toEqual({ from: "Widget", to: "Gadget" })
    })
  })

  describe("onDelete", () => {
    it("fires when isDeleted transitions from false to true", async () => {
      // given
      const deleted: Array<{ itemId: string }> = []

      const Item = state({
        id: { itemId: z.string() },
        tags: (id) => ({ itemId: id.itemId }),
        evolve: [
          () => ({ created: false, name: "", deleted: false }) as ItemState,
          [ItemCreated, (state, { payload: e }) => ({ ...state, created: true, name: e.name })],
          [ItemDeleted, (state) => ({ ...state, deleted: true })],
        ],
        lifecycle: {
          isDeleted: (state) => state.deleted,
          onDelete: (_state, id) => { deleted.push(id) },
        },
      })

      const eventStore = inMemoryEventStore()
      await eventStore.append([
        eventMsg(ItemCreated, { itemId: "i-1", name: "Widget" }),
        eventMsg(ItemDeleted, { itemId: "i-1" }),
      ])

      const repo = eventSourcedRepository(Item, eventStore)

      // when
      await repo.load({ itemId: "i-1" })

      // then
      expect(deleted).toEqual([{ itemId: "i-1" }])
    })

    it("does not fire when already deleted", async () => {
      // given
      const deleted: Array<{ itemId: string }> = []

      const Item = state({
        id: { itemId: z.string() },
        tags: (id) => ({ itemId: id.itemId }),
        evolve: [
          () => ({ created: false, name: "", deleted: false }) as ItemState,
          [ItemCreated, (state, { payload: e }) => ({ ...state, created: true, name: e.name })],
          [ItemDeleted, (state) => ({ ...state, deleted: true })],
        ],
        lifecycle: {
          isDeleted: (state) => state.deleted,
          onDelete: (_state, id) => { deleted.push(id) },
        },
      })

      const eventStore = inMemoryEventStore()
      await eventStore.append([
        eventMsg(ItemCreated, { itemId: "i-1", name: "Widget" }),
        eventMsg(ItemDeleted, { itemId: "i-1" }),
        eventMsg(ItemDeleted, { itemId: "i-1" }), // duplicate delete
      ])

      const repo = eventSourcedRepository(Item, eventStore)

      // when
      await repo.load({ itemId: "i-1" })

      // then — only fired once
      expect(deleted).toEqual([{ itemId: "i-1" }])
    })
  })

  describe("no lifecycle", () => {
    it("works without lifecycle hooks", async () => {
      // given
      const Item = state({
        id: { itemId: z.string() },
        tags: (id) => ({ itemId: id.itemId }),
        evolve: [
          () => ({ created: false, name: "", deleted: false }) as ItemState,
          [ItemCreated, (state, { payload: e }) => ({ ...state, created: true, name: e.name })],
        ],
        // no lifecycle
      })

      const eventStore = inMemoryEventStore()
      await eventStore.append([
        eventMsg(ItemCreated, { itemId: "i-1", name: "Widget" }),
      ])

      const repo = eventSourcedRepository(Item, eventStore)

      // when
      const result = await repo.load({ itemId: "i-1" })

      // then
      expect(result.state.name).toBe("Widget")
    })
  })
})
