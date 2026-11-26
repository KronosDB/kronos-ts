import { describe, expect, it } from "bun:test"
import { z } from "zod"
import {
  commandHandler,
  inMemoryEventStore,
  kronos,
  localCommandBus,
  localQueryBus,
  send,
  type TaggedEvent,
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
    const appended: TaggedEvent[] = []
    const log = inMemoryEventStore()
    const eventStore = {
      ...log,
      async append(...args: Parameters<typeof log.append>) {
        appended.push(...args[0])
        return log.append(...args)
      },
    }
    const commandBus = localCommandBus(unitOfWork)
    const queryBus = localQueryBus(unitOfWork)
    const app = kronos({ commandHandlers: [{ ...borrow, eventStore, commandBus, queryBus }] })
    try {
      await send(commandBus, Borrow, { copyId: "c-1", memberId: "ada" })
      expect(appended[0]!.tags).toEqual([
        { key: "copyId", value: "c-1" },
        { key: "memberId", value: "ada" },
      ])
    } finally {
      await app.stop()
    }
  })
})
