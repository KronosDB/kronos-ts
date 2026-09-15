import { flowControlledSender as kronosSender } from "../../kronosdb/src/flow-controlled-sender.js"
import { flowControlledSender as axonSender } from "../../axon-server/src/flow-controlled-sender.js"
import { describe, expect, it } from "bun:test"
import {
  messagingAdmission,
  messagingDeadline,
  withMessagingTimeout,
} from "./messaging-reliability.js"
import { outboundStream as kronosStream } from "../../kronosdb/src/outbound-stream.js"
import { outboundStream as axonStream } from "../../axon-server/src/outbound-stream.js"
import { localQueryBus, qn, unitOfWork, updateHandler } from "./index.js"

const message = {
  kind: "query" as const,
  identifier: "sub",
  name: qn("qa", "Watch"),
  payload: {},
  metadata: {},
}

describe("messaging resource contracts", () => {
  it("rejects excess work without queuing, reports peaks, and releases exactly once", () => {
    const gate = messagingAdmission("test", 1, () => {
      throw new Error("observer failed")
    })
    const first = gate.enter()
    expect(() => gate.enter()).toThrow(/overloaded/)
    first.end()
    first.end()
    const next = gate.enter()
    next.end()
    expect(gate.snapshot).toMatchObject({
      active: 0,
      peak: 1,
      accepted: 2,
      rejected: 1,
      completed: 2,
    })
  })
  it("rejects invalid resource limits", () => {
    for (const invalid of [0, -1, NaN, Infinity, 1.5])
      expect(() => messagingAdmission("test", invalid)).toThrow()
  })
  it("bounds shutdown waits and observes a late rejection", async () => {
    let reject!: (error: Error) => void
    const work = new Promise<void>((_, r) => {
      reject = r
    })
    await expect(withMessagingTimeout(work, 5, "shutdown")).rejects.toThrow(/shutdown timed out/)
    reject(new Error("late failure"))
    await Promise.resolve()
  })
  it("cancels a real signal on deadline and clears the timer on completion", async () => {
    const expires = messagingDeadline(5)
    await new Promise((r) => setTimeout(r, 10))
    expect(expires.signal.aborted).toBe(true)
    expires.close()
    const completed = messagingDeadline(5)
    completed.close()
    await new Promise((r) => setTimeout(r, 10))
    expect(completed.signal.aborted).toBe(false)
  })
  it("never strands the first subscription read when a concurrent read is attempted", async () => {
    const handler = updateHandler(message)
    const iterator = handler.iterable[Symbol.asyncIterator]()
    const first = iterator.next()
    await expect(iterator.next()).rejects.toThrow(/Concurrent/)
    handler.offer(7)
    expect((await first).value).toBe(7)
    await iterator.return!()
  })
  it("releases local subscriptions on initial failure and iterator return", async () => {
    const bus = localQueryBus(unitOfWork)
    let fail = true
    bus.subscribe("qa.Watch", async () => {
      if (fail) throw new Error("failed initial")
      return 1
    })
    const failed = bus.subscriptionQuery(message)
    await expect(failed.initialResult).rejects.toThrow("failed initial")
    await expect(failed.updates[Symbol.asyncIterator]().next()).rejects.toThrow("failed initial")
    fail = false
    const good = bus.subscriptionQuery(message)
    expect(await good.initialResult).toBe(1)
    await good.updates[Symbol.asyncIterator]().return!()
    const reopened = bus.subscribeToUpdates(message)
    reopened.close()
  })
})

for (const [name, create] of [
  ["kronos", kronosStream],
  ["axon", axonStream],
] as const) {
  describe(`${name} bounded outbound stream`, () => {
    it("bounds the buffer and preserves falsy messages and queued data during close", async () => {
      const stream = create<any>(2)
      stream.send(false)
      stream.send(0)
      expect(() => stream.send(3)).toThrow(/overflow/)
      stream.close()
      expect(() => stream.send(4)).toThrow(/closed/)
      const iterator = stream.iterable[Symbol.asyncIterator]()
      expect((await iterator.next()).value).toBe(false)
      expect((await iterator.next()).value).toBe(0)
      expect((await iterator.next()).done).toBe(true)
      expect(stream.buffered).toBe(0)
    })
    it("cleans up a waiting reader and refuses multiple consumers", async () => {
      const stream = create()
      const iterator = stream.iterable[Symbol.asyncIterator]()
      const pending = iterator.next()
      await expect(iterator.next()).rejects.toThrow(/Concurrent/)
      expect(() => stream.iterable[Symbol.asyncIterator]()).toThrow(/one consumer/)
      await iterator.return!()
      expect((await pending).done).toBe(true)
    })
  })
}

for (const [name, create] of [
  ["kronos", kronosSender],
  ["axon", axonSender],
] as const) {
  it(`${name}: buffered sender failure terminates and surfaces the error`, () => {
    const errors: Error[] = []
    const sender = create(
      () => {
        throw new Error("wire broken")
      },
      undefined,
      (e) => errors.push(e),
    )
    expect(sender.offer(1)).toBe(true)
    expect(() => sender.addPermits(1)).toThrow("wire broken")
    expect(sender.active).toBe(false)
    expect(sender.offer(2)).toBe(false)
    expect(errors).toHaveLength(1)
  })
  it(`${name}: completion follows buffered updates once credit arrives`, () => {
    const delivered: unknown[] = []
    const sender = create(
      (value) => delivered.push(value),
      () => delivered.push("complete"),
    )
    sender.offer(1)
    sender.offer(2)
    sender.complete()
    expect(delivered).toEqual([])
    expect(sender.offer(3)).toBe(false)
    sender.addPermits(1)
    expect(delivered).toEqual([1])
    sender.addPermits(1)
    sender.complete()
    expect(delivered).toEqual([1, 2, "complete"])
  })
}

it("closing a local subscription settles an initial handler that is still awaiting work", async () => {
  const bus = localQueryBus(unitOfWork)
  let release!: () => void
  bus.subscribe("qa.Watch", async () => {
    await new Promise<void>((r) => {
      release = r
    })
    return 1
  })
  const sub = bus.subscriptionQuery(message)
  await new Promise((r) => setTimeout(r, 0))
  sub.close()
  try {
    await expect(sub.initialResult).rejects.toThrow(/closed before initial/)
  } finally {
    release()
  }
})
