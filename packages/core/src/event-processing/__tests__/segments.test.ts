import { describe, expect, it } from "bun:test"
import { qn, emptyMetadata, type EventMessage } from "../../messaging/messages.js"
import { batched, concurrent, eventProcessor, polling, segmented, type RunningProcessor } from "../processor.js"
import { runEventProcessor } from "../running-processor.js"
import type { EventStore } from "../../event-sourcing/event-store.js"
import { inMemoryTokenStore, type TokenStore } from "../token-store.js"
import { unitOfWork } from "../../unit-of-work/unit-of-work.js"
import type { SequencedEvent } from "../source.js"
import type { EventHandler } from "../handler.js"
import { inSegment, segmentsOf, sequenceHash, splitSegment, mergeableSegmentId } from "../segment.js"
import { globalSequenceToken, isMergedToken, mergedToken, advanceTokenTo, serializeToken, deserializeToken } from "../tracking-token.js"

// ---------------------------------------------------------------------------
// A log that grows: events are pushed, and every open stream is woken.
// ---------------------------------------------------------------------------

const Counted = qn("test", "Counted")

type Payload = { account: string; n: number }

function growingLog() {
  const events: SequencedEvent[] = []
  const waiting = new Set<() => void>()
  const store = {
    push(...payloads: Payload[]) {
      for (const payload of payloads) {
        const sequence = BigInt(events.length)
        events.push({
          sequence,
          event: {
            kind: "event",
            identifier: `e-${sequence}`,
            name: Counted,
            version: "1.0",
            payload,
            metadata: emptyMetadata(),
            timestamp: 0,
            tags: [{ key: "account", value: payload.account }],
          },
        })
      }
      for (const wake of waiting) wake()
    },
    open(condition: { position: bigint }) {
      let cursor = condition.position
      let callback: (() => void) | null = null
      const wake = () => callback?.()
      waiting.add(wake)
      return {
        next() {
          const found = events.find((e) => e.sequence >= cursor)
          if (found) cursor = found.sequence + 1n
          return found
        },
        peek: () => events.find((e) => e.sequence >= cursor),
        hasNextAvailable: () => events.some((e) => e.sequence >= cursor),
        isCompleted: () => false,
        error: () => undefined,
        setCallback(cb: () => void) {
          callback = cb
        },
        close() {
          callback = null
          waiting.delete(wake)
        },
      }
    },
    async getHeadPosition() {
      return BigInt(events.length)
    },
  }
  return store as unknown as EventStore & { push(...payloads: Payload[]): void }
}

const byAccount = (e: EventMessage) => (e.payload as Payload).account
const accounts = (n: number) => Array.from({ length: n }, (_, i) => `acc-${i}`)

/** Every event handled, by which node, in order. */
function recorder() {
  const seen: Array<{ node: string; account: string; n: number }> = []
  const handler = (node: string): EventHandler<any> => ({
    kind: "event-handler",
    descriptor: { kind: "event", name: Counted, version: "1.0", payload: {} as any },
    handler: ({ payload }: { payload: Payload }) => {
      seen.push({ node, ...payload })
    },
  })
  return { seen, handler }
}

function node(
  label: string,
  log: EventStore,
  tokens: TokenStore,
  handler: (node: string) => EventHandler<any>,
  options: { segments?: number; renewEveryMs?: number } = {},
): RunningProcessor {
  // The processor paces its claim rounds off `claimTimeoutMs`; the store
  // expires claims by its own. Telling this node a different timeout makes it
  // renew every `renewEveryMs` (default 20, so tests see claims move quickly)
  // without changing when the store lets a claim lapse.
  const view: TokenStore = { ...tokens, claimTimeoutMs: (options.renewEveryMs ?? 20) * 2 }
  let processor = batched(eventProcessor("balances", log, view, unitOfWork), 10)
  if (options.segments !== undefined) processor = segmented(concurrent(processor, byAccount), options.segments, byAccount)
  return runEventProcessor({
    processor: polling(processor, 5),
    handlers: [{ definition: handler(label) }],
  })
}

async function until(done: () => boolean, what: string, timeoutMs = 3000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!done() && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5))
  if (!done()) throw new Error(`timed out waiting for ${what}`)
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

/** Each account's events in the order they were handled — must be 0, 1, 2, … */
function inOrderPerAccount(seen: ReadonlyArray<{ account: string; n: number }>) {
  const last = new Map<string, number>()
  for (const { account, n } of seen) {
    expect(n).toBe((last.get(account) ?? -1) + 1)
    last.set(account, n)
  }
}

function pushRound(log: { push(...p: Payload[]): void }, names: string[], n: number) {
  log.push(...names.map((account) => ({ account, n })))
}

// ---------------------------------------------------------------------------

describe("the segment math", () => {
  it("splits into halves that cover the segment, and merges them back", () => {
    const [a, b] = splitSegment({ id: 1, mask: 1 })
    expect(a).toEqual({ id: 1, mask: 3 })
    expect(b).toEqual({ id: 3, mask: 3 })
    expect(mergeableSegmentId(a)).toBe(3)
    expect(mergeableSegmentId(b)).toBe(1)
  })

  it("works every mask out from the ids alone", () => {
    expect(segmentsOf([0])).toEqual([{ id: 0, mask: 0 }])
    expect(segmentsOf([0, 1, 2, 3])).toEqual([0, 1, 2, 3].map((id) => ({ id, mask: 3 })))
    // 1 split again into 1 and 5: the others keep their masks.
    expect(segmentsOf([0, 1, 2, 3, 5])).toEqual([
      { id: 0, mask: 3 },
      { id: 1, mask: 7 },
      { id: 2, mask: 3 },
      { id: 3, mask: 3 },
      { id: 5, mask: 7 },
    ])
  })

  it("puts every sequence in exactly one segment", () => {
    const segments = segmentsOf([0, 1, 2, 3, 5])
    for (const account of accounts(200)) {
      expect(segments.filter((s) => inSegment(s, sequenceHash(account)))).toHaveLength(1)
    }
  })

  it("hashes the same way every time", () => {
    expect(sequenceHash("acc-1")).toBe(sequenceHash("acc-1"))
    expect(sequenceHash("")).toBe(0x811c9dc5)
  })
})

describe("a merged token", () => {
  it("reads from the half that is behind and becomes plain once both have got to one place", () => {
    const merged = mergedToken(1, [
      { segment: 0, token: globalSequenceToken(10n) },
      { segment: 1, token: globalSequenceToken(4n) },
    ])
    expect(isMergedToken(merged)).toBe(true)
    expect(merged.position()).toBe(4n)
    const halfway = advanceTokenTo(merged, globalSequenceToken(7n))
    expect(isMergedToken(halfway)).toBe(true)
    expect(advanceTokenTo(halfway, globalSequenceToken(10n)).kind).toBe("global-sequence")
  })

  it("survives the token row", () => {
    const merged = mergedToken(1, [
      { segment: 0, token: globalSequenceToken(10n) },
      { segment: 1, token: globalSequenceToken(4n) },
    ])
    const { type, data } = serializeToken(merged)
    expect(type).toBe("MergedToken")
    const back = deserializeToken(type, data)!
    expect(isMergedToken(back)).toBe(true)
    expect(back.position()).toBe(4n)
  })
})

describe("one segment, two nodes", () => {
  it("is handled by one node at a time: every event once", async () => {
    const log = growingLog()
    const tokens = inMemoryTokenStore()
    const { seen, handler } = recorder()
    const a = node("a", log, tokens, handler)
    const b = node("b", log, tokens, handler)
    await a.start()
    await b.start()
    for (let n = 0; n < 20; n++) pushRound(log, ["x", "y"], n)
    await until(() => seen.length >= 40, "every event")
    await sleep(50)
    await Promise.all([a.stop(), b.stop()])

    expect(seen).toHaveLength(40)
    expect(new Set(seen.map((s) => s.node))).toEqual(new Set(["a"]))
    expect(b.segments()).toEqual([])
  })

  it("moves to the other node when the first stops", async () => {
    const log = growingLog()
    const tokens = inMemoryTokenStore()
    const { seen, handler } = recorder()
    const a = node("a", log, tokens, handler)
    const b = node("b", log, tokens, handler)
    await a.start()
    await b.start()
    pushRound(log, ["x"], 0)
    await until(() => seen.length === 1, "the first event")
    await a.stop()
    pushRound(log, ["x"], 1)
    await until(() => seen.length === 2, "the second event, on b")
    await b.stop()

    expect(seen.map((s) => s.node)).toEqual(["a", "b"])
  })

  it("fences a node that lost its claim: its batch fails and the other node handles the event", async () => {
    const log = growingLog()
    // A claim lapses after 30 ms, and node a renews only every 10 s — so b can take it.
    const tokens = inMemoryTokenStore(30)
    const { seen, handler } = recorder()
    const a = node("a", log, tokens, handler, { renewEveryMs: 10_000 })
    await a.start()
    pushRound(log, ["x"], 0)
    await until(() => seen.length === 1, "a's first event")
    await sleep(60)
    const b = node("b", log, tokens, handler)
    await b.start()
    expect(b.segments().map((s) => s.segment)).toEqual([0])

    pushRound(log, ["x"], 1)
    await until(() => seen.some((s) => s.node === "b" && s.n === 1), "b to handle the next event")
    await until(() => a.segments().length === 0, "a to let go")
    await Promise.all([a.stop(), b.stop()])

    // a may have run its handler for n=1 before its write was refused; b's is the one that counted.
    expect((await tokens.get("balances", 0))!.position()).toBe(2n)
  })
})

describe("a node that lost its claim while stuck on a failing handler", () => {
  it("finds out when it next renews, and stops handling the segment", async () => {
    const log = growingLog()
    const tokens = inMemoryTokenStore(30)
    const failing: EventHandler<any> = {
      kind: "event-handler",
      descriptor: { kind: "event", name: Counted, version: "1.0", payload: {} as any },
      handler: () => {
        throw new Error("down")
      },
    }
    const a = node("a", log, tokens, () => failing, { renewEveryMs: 100 })
    await a.start()
    pushRound(log, ["x"], 0)
    await until(() => a.status().error !== undefined, "a to fail")
    await sleep(60)

    const { seen, handler } = recorder()
    const b = node("b", log, tokens, handler)
    await b.start()
    await until(() => seen.length === 1, "b to handle the event a could not")
    await until(() => a.segments().length === 0, "a to find out")
    await Promise.all([a.stop(), b.stop()])
  })
})

describe("segmented", () => {
  it("starts with the segments it was given, and keeps each sequence in order", async () => {
    const log = growingLog()
    const tokens = inMemoryTokenStore()
    const { seen, handler } = recorder()
    const a = node("a", log, tokens, handler, { segments: 4 })
    await a.start()
    for (let n = 0; n < 10; n++) pushRound(log, accounts(20), n)
    await until(() => seen.length >= 200, "every event")
    await a.stop()

    expect(seen).toHaveLength(200)
    expect(await tokens.fetchSegments("balances")).toEqual([0, 1, 2, 3])
    inOrderPerAccount(seen)
  })

  it("hands a released segment to another node, which takes only that segment's sequences", async () => {
    const log = growingLog()
    const tokens = inMemoryTokenStore()
    const { seen, handler } = recorder()
    const a = node("a", log, tokens, handler, { segments: 4 })
    const b = node("b", log, tokens, handler, { segments: 4 })
    await a.start()
    await b.start()
    expect(a.segments().map((s) => s.segment)).toEqual([0, 1, 2, 3])

    await a.releaseSegment(2, 10_000)
    await until(() => b.segments().length === 1, "b to claim segment 2")
    for (let n = 0; n < 5; n++) pushRound(log, accounts(20), n)
    await until(() => seen.length >= 100, "every event")
    await sleep(30)
    await Promise.all([a.stop(), b.stop()])

    expect(seen).toHaveLength(100)
    const two = segmentsOf([0, 1, 2, 3]).find((s) => s.id === 2)!
    for (const { node: who, account } of seen) {
      expect(who).toBe(inSegment(two, sequenceHash(account)) ? "b" : "a")
    }
    inOrderPerAccount(seen)
  })

  it("splits a segment while running, with nothing handled twice or missed", async () => {
    const log = growingLog()
    const tokens = inMemoryTokenStore()
    const { seen, handler } = recorder()
    const a = node("a", log, tokens, handler, { segments: 1 })
    await a.start()
    for (let n = 0; n < 5; n++) pushRound(log, accounts(10), n)
    await until(() => seen.length >= 50, "the first rounds")

    await a.splitSegment(0)
    expect(a.segments().map((s) => [s.segment, s.mask])).toEqual([
      [0, 1],
      [1, 1],
    ])
    for (let n = 5; n < 10; n++) pushRound(log, accounts(10), n)
    await until(() => seen.length >= 100, "every event")
    await sleep(30)
    await a.stop()

    expect(seen).toHaveLength(100)
    expect(await tokens.fetchSegments("balances")).toEqual([0, 1])
    inOrderPerAccount(seen)
  })

  it("merges two halves that had got to different places, handing the one behind only its own events", async () => {
    const log = growingLog()
    const tokens = inMemoryTokenStore()
    for (let n = 0; n < 4; n++) pushRound(log, accounts(10), n)
    // Two halves: segment 0 has handled everything (40), segment 1 only up to 20.
    await tokens.initializeSegments("balances", 2)
    await tokens.store("balances", 0, globalSequenceToken(40n))
    await tokens.store("balances", 1, globalSequenceToken(20n))

    const { seen, handler } = recorder()
    const a = node("a", log, tokens, handler, { segments: 2 })
    await a.mergeSegment(1)
    expect(await tokens.fetchSegments("balances")).toEqual([0])
    expect(isMergedToken((await tokens.get("balances", 0))!)).toBe(true)

    await a.start()
    await until(() => a.position === 40n, "the merged segment to catch up")
    await a.stop()

    const one = { id: 1, mask: 1 }
    expect(seen.length).toBeGreaterThan(0)
    for (const { account, n } of seen) {
      expect(inSegment(one, sequenceHash(account))).toBe(true)
      expect(n).toBeGreaterThanOrEqual(2)
    }
    expect((await tokens.get("balances", 0))!.kind).toBe("global-sequence")
  })

  it("resets every segment, and refuses while another node holds one", async () => {
    const log = growingLog()
    const tokens = inMemoryTokenStore()
    const { seen, handler } = recorder()
    const a = node("a", log, tokens, handler, { segments: 4 })
    const b = node("b", log, tokens, handler, { segments: 4 })
    await a.start()
    for (let n = 0; n < 3; n++) pushRound(log, accounts(10), n)
    await until(() => seen.length >= 30, "the first pass")

    await expect(b.resetTokens(0n)).rejects.toThrow(/held by another node/)
    await a.stop()
    await a.resetTokens(0n)
    expect(a.replaying).toBe(true)

    seen.length = 0
    await a.start()
    await until(() => seen.length >= 30, "the replay")
    await until(() => !a.replaying, "the replay to end")
    await a.stop()
    inOrderPerAccount(seen)
  })
})
