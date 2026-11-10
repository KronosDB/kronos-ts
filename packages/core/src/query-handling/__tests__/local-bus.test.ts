import { describe, expect, it } from "bun:test"
import { emptyMetadata, qn } from "../../messaging/messages.js"
import { isNoHandler } from "../../messaging/failures.js"
import { unitOfWork } from "../../unit-of-work/unit-of-work.js"
import { localQueryBus } from "../local-bus.js"

describe("localQueryBus", () => {
  it("fails a query nothing handles with core's no-handler kind", async () => {
    const failure = await localQueryBus(unitOfWork)
      .query({ kind: "query", identifier: "q-1", name: qn("test", "Unknown"), payload: {}, metadata: emptyMetadata(), timestamp: 0 })
      .then(() => undefined, (e) => e)
    expect(isNoHandler(failure)).toBe(true)
    expect(failure.message).toBe('No handler registered for query "test.Unknown"')
  })
})
