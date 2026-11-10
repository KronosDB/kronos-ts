/**
 * THE ERROR CONTRACT OF AN EVENT STORE, against any store.
 *
 * Every store family runs these, straight on the store, without a handler in
 * between: an append whose condition another writer has violated fails with a
 * conflict that core's `isConflict` recognises, whatever the store runs on, and
 * writes nothing.
 */
import assert from "node:assert/strict"
import { it } from "bun:test"
import { emptyMetadata, isConflict, qn, type EventMessage, type EventQuery, type EventStore } from "@kronos-ts/core"

export type EventStoreContractTarget = {
  /** The store every check appends to. Called once per check. */
  readonly eventStore: () => EventStore
}

let sequence = 0

function freshSubject(): { query: EventQuery; event: () => EventMessage } {
  const subject = `contract-${Date.now().toString(36)}-${++sequence}-${Math.random().toString(36).slice(2, 8)}`
  return {
    query: { tags: { subject } },
    event: () => ({
      kind: "event",
      identifier: crypto.randomUUID(),
      name: qn("contract", "Bumped"),
      version: "1",
      payload: { subject },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
      tags: [{ key: "subject", value: subject }],
    }),
  }
}

export function eventStoreContract(target: EventStoreContractTarget): void {
  it("refuses an append on a stale condition with a conflict, and writes nothing", async () => {
    const store = target.eventStore()
    const { query, event } = freshSubject()
    await store.append([event()])
    const read = await store.source({ query })
    await store.append([event()]) // another writer, after the read

    const refusal = await store.append([event()], { query, marker: read.marker }).then(
      () => undefined,
      (error: unknown) => error,
    )

    assert.ok(isConflict(refusal), `expected a conflict, got ${String(refusal)}`)
    assert.equal((await store.source({ query })).events.length, 2)
  })

  it("lets one of several racing appends on the same condition land, and refuses the rest with conflicts", async () => {
    const store = target.eventStore()
    const { query, event } = freshSubject()
    await store.append([event()])
    const read = await store.source({ query })

    const outcomes = await Promise.allSettled(
      Array.from({ length: 4 }, () => store.append([event()], { query, marker: read.marker })),
    )

    const landed = outcomes.filter((o) => o.status === "fulfilled")
    const refused = outcomes.flatMap((o) => (o.status === "rejected" ? [o.reason] : []))
    assert.equal(landed.length, 1)
    for (const reason of refused) assert.ok(isConflict(reason), `expected a conflict, got ${String(reason)}`)
    assert.equal((await store.source({ query })).events.length, 2)
  })
}
