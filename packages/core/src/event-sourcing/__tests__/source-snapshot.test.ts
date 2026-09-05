/**
 * THE RAW SNAPSHOTTING LAYER — `ctx.source(query, { snapshot })` plus an
 * `eventStore.storeSnapshot(key, …)` call, which together ARE the capability.
 *
 * BOTH HALVES COME OFF ONE OBJECT now: the read verb grew an overload, and the
 * write is a member of the log the slice already holds.
 *
 * Everything `state({ snapshot })` does is these four lines with the key
 * composition and the policy written for you. That is the layering, and it is
 * why this file comes before the sugar's.
 */
import { describe, expect, it } from "bun:test"
import { qn, emptyMetadata, type EventMessage } from "../../messaging/messages.js"
import { generateIdentifier } from "../../messaging/identifier.js"
import { inMemoryEventStore } from "../in-memory.js"
import { sourceFunction } from "../load.js"
import { unitOfWork } from "../../unit-of-work/unit-of-work.js"
import type { EventStore } from "../event-store.js"



function bumped(counterId: string, by = 1): EventMessage {
  return {
    kind: "event",
    identifier: generateIdentifier(),
    name: qn("counting", "Bumped"),
    version: "1.0",
    payload: { counterId, by },
    metadata: emptyMetadata(),
    timestamp: Date.now(),
    tags: [{ key: "counterId", value: counterId }],
  } as EventMessage
}

/** The site a handler's `ctx.source` is built from. */
function site(eventStore: EventStore) {
  const uow = unitOfWork()
  return { uow, source: sourceFunction({ uow, eventStore }) }
}

async function appendBumps(store: EventStore, counterId: string, n: number) {
  for (let i = 0; i < n; i++) await store.append([bumped(counterId)])
}

/** THE RAW FOLD, written by hand — `is()` + `reduce`, exactly as documented. */

describe("ctx.source(query) — the plain read is untouched", () => {
  it("still answers with the events array and nothing else", async () => {
    const log = inMemoryEventStore()
    await appendBumps(log, "c-1", 3)

    const events = await site(log).source({ tags: { counterId: "c-1" } })

    expect(Array.isArray(events)).toBe(true)
    expect(events.length).toBe(3)
  })

  it("carries no snapshot key onto the condition", async () => {
    const seen: unknown[] = []
    const log = inMemoryEventStore()
    const spy: EventStore = {
      ...log,
      async source(condition) { seen.push(condition.snapshot); return log.source(condition) },
    }
    await site(spy).source({ tags: { counterId: "c-x" } })
    expect(seen).toEqual([undefined])
  })
})

/** A log with the capability on it — the one line of client-side wiring. */
