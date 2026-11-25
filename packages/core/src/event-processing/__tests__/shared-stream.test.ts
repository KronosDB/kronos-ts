import { describe, expect, it } from "bun:test"
import { sharedStream } from "../shared-stream.js"
import { inMemoryEventStore } from "../../event-sourcing/in-memory.js"
import { qn, type EventMessage } from "../../messaging/messages.js"

function thing(n: number): EventMessage {
  return {
    kind: "event",
    identifier: `thing-${n}`,
    name: qn("shared", "Thing"),
    version: "1.0",
    payload: { n },
    metadata: {},
    timestamp: 1_700_000_000_000,
    tags: [],
  }
}

describe("sharedStream and the function applied to each event", () => {
  it("reads an event again for every view when the function threw on it", async () => {
    const log = inMemoryEventStore()
    await log.append([thing(0)])

    let failures = 1
    const shared = sharedStream(log, {
      readAhead: 10,
      each(event) {
        if (event.identifier === "thing-1" && failures-- > 0) throw new Error("cannot prepare thing-1")
        return event
      },
    })
    const first = shared.open({ position: 0n }, () => true)
    let second = shared.open({ position: 0n }, () => true)
    const taken = (stream: typeof first) => {
      const seen: string[] = []
      for (let e = stream.next(); e; e = stream.next()) seen.push(e.event.identifier)
      return seen
    }
    expect(taken(first)).toEqual(["thing-0"])
    expect(taken(second)).toEqual(["thing-0"])

    // The second view's next read reaches the event the function throws on.
    await log.append([thing(1), thing(2)])
    expect(() => second.next()).toThrow("cannot prepare thing-1")
    // It reopens where it had got to, as a processor does after a failed poll.
    second.close()
    second = shared.open({ position: 1n }, () => true)

    expect(taken(second)).toEqual(["thing-1", "thing-2"])
    expect(taken(first)).toEqual(["thing-1", "thing-2"])
    first.close()
    second.close()
  })
})
