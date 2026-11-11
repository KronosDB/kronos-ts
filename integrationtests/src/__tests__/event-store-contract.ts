/**
 * THE ERROR CONTRACT OF AN EVENT STORE, against any store.
 *
 * Every store family runs these, straight on the store, without a handler in
 * between: an append whose condition another writer has violated fails with a
 * conflict that core's `isConflict` recognises, whatever the store runs on, and
 * writes nothing. A store that cannot reach its server, never or no longer,
 * fails its reads and appends as unavailable (`isUnavailable`), in bounded time.
 */
import assert from "node:assert/strict"
import { it } from "bun:test"
import { createServer } from "node:net"
import { emptyMetadata, isConflict, isUnavailable, qn, type EventMessage, type EventQuery, type EventStore } from "@kronos-ts/core"

export type EventStoreContractTarget = {
  /** The store every check appends to. Called once per check. */
  readonly eventStore: () => EventStore
  /** A store of this family pointed at `port`, where nothing listens. Absent for a store with no server. */
  readonly unreachable?: (port: number) => Promise<EventStore>
  /**
   * A store of this family whose server answered an append and was then
   * stopped. Absent for a store with no server.
   */
  readonly stopped?: () => Promise<EventStore>
}

/** A port nothing listens on: one the system handed out and that was closed again. */
export async function closedPort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer()
    server.once("error", reject)
    server.listen(0, "127.0.0.1", () => {
      const address = server.address()
      server.close(() => (typeof address === "object" && address ? resolve(address.port) : reject(new Error("no port"))))
    })
  })
}

async function failsUnavailable(what: string, work: Promise<unknown>): Promise<void> {
  const failure = await Promise.race([
    work.then(
      () => assert.fail(`${what}: expected a failure`),
      (error: unknown) => error,
    ),
    new Promise((resolve) => setTimeout(() => resolve(new Error(`${what}: no answer within 20 s`)), 20_000)),
  ])
  assert.ok(isUnavailable(failure), `${what}: expected unavailable, got ${String(failure)}`)
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

  if (target.unreachable) {
    const unreachable = target.unreachable
    it("fails reads and appends as unavailable when the server cannot be reached", async () => {
      const store = await unreachable(await closedPort())
      const { query, event } = freshSubject()
      await failsUnavailable("source", store.source({ query }))
      await failsUnavailable("append", store.append([event()]))
    }, 60_000)
  }

  if (target.stopped) {
    const stopped = target.stopped
    it("fails reads and appends as unavailable once the server has stopped", async () => {
      const store = await stopped()
      const { query, event } = freshSubject()
      await failsUnavailable("source", store.source({ query }))
      await failsUnavailable("append", store.append([event()]))
    }, 120_000)
  }
}
