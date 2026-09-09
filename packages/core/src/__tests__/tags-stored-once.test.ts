import { describe, expect, it } from "bun:test"
import { z } from "zod"
import {
  commandHandler,
  inMemoryEventStore,
  kronos,
  localCommandBus,
  localQueryBus,
  send,
  sourcingCondition,
  unitOfWork,
  withNamespace,
} from "@kronos-ts/core"

const ns = withNamespace("tags-once")
const Borrow = ns.command("Borrow", { payload: z.object({ copyId: z.string(), memberId: z.string() }) })
const Borrowed = ns.event("Borrowed", {
  payload: z.object({ copyId: z.string(), memberId: z.string() }),
  tags: { copyId: (p) => p.copyId, memberId: (p) => p.memberId },
})

describe("an appended event's tags are stored exactly once", () => {
  it("a two-tag descriptor yields two stored tags, not four", async () => {
    const borrow = commandHandler(Borrow, async ({ payload }, ctx) => {
      ctx.append(Borrowed, payload)
    })
    const eventStore = inMemoryEventStore()
    const commandBus = localCommandBus(unitOfWork)
    const queryBus = localQueryBus(unitOfWork)
    const app = kronos({ commandHandlers: [{ ...borrow, eventStore, commandBus, queryBus }] })
    try {
      await send(commandBus, Borrow, { copyId: "c-1", memberId: "ada" })
      const { events } = await eventStore.source(sourcingCondition({}))
      expect(events[0]!.tags).toEqual([
        { key: "copyId", value: "c-1" },
        { key: "memberId", value: "ada" },
      ])
    } finally {
      await app.stop()
    }
  })
})
