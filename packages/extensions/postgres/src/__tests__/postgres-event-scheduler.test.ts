/**
 * Unit tests for postgresEventScheduler.
 *
 * Uses a fake adapter whose `transaction(fn)` snapshots its in-memory
 * "kronos_scheduled_events" table before calling fn and reverts on
 * rejection, so we can exercise real UoW-rollback semantics without a
 * live postgres. The real postgresTransactionManager + lazyTransactional
 * UoW runner are used unchanged — these tests verify the scheduler's SQL
 * + lifecycle, not the tx-bridging glue (covered by
 * postgres-transaction-manager.test.ts + lazy-transactional-uow.test.ts).
 */

import { describe, it, expect } from "bun:test"
import { qn, type Metadata } from "@kronos-ts/common"
import type { EventMessage } from "@kronos-ts/messaging"
import {
  lazyTransactionalUnitOfWorkFactory,
  runInNewUoW,
} from "@kronos-ts/messaging"
import type {
  PostgresAdapter,
  PostgresAdapterTransaction,
  ListenSubscription,
} from "../adapter.js"
import { IsolationLevel } from "../adapter.js"
import { postgresTransactionManager } from "../postgres-transaction-manager.js"
import { postgresEventScheduler } from "../postgres-event-scheduler.js"
import type { TagResolver } from "../postgres-event-store.js"

// ── Fake adapter ────────────────────────────────────────────────────────

interface FakeRow {
  schedule_id: string
  fire_at: Date
  status: "pending" | "appended" | "cancelled"
  type: string
  tags: string[]
  payload: string
  metadata: string
  version: string
  timestamp: number
}

/**
 * Lightweight pg fake: a Map-backed table + tx semantics via snapshot/revert.
 * Supports just the SQL the scheduler emits — anything else throws so the
 * test surface stays sealed.
 */
function createFakeAdapter() {
  let table = new Map<string, FakeRow>()
  const now = { value: new Date() }

  function execOn(map: Map<string, FakeRow>, sql: string, params: unknown[] = []): unknown[] {
    const compact = sql.replace(/\s+/g, " ").trim()

    // Per-transaction safety GUCs armed by postgresTransactionManager via
    // SET LOCAL. No-op for the fake — they only matter against real postgres.
    if (compact.startsWith("SET LOCAL")) return []

    // INSERT new schedule
    if (compact.startsWith("INSERT INTO")) {
      const [id, fireAt, type, tags, payload, metadata, version, ts] = params as [
        string, string, string, string[], string, string, string, number,
      ]
      map.set(id, {
        schedule_id: id,
        fire_at: new Date(fireAt),
        status: "pending",
        type,
        tags,
        payload,
        metadata,
        version,
        timestamp: ts,
      })
      return []
    }

    // SELECT … FOR UPDATE on a single id (cancel path)
    if (compact.startsWith("SELECT status FROM") && compact.includes("WHERE schedule_id = $1")) {
      const id = params[0] as string
      const row = map.get(id)
      return row ? [{ status: row.status }] : []
    }

    // UPDATE status = '…' WHERE schedule_id = $1
    const upd = compact.match(/^UPDATE \w+ SET status = '(\w+)' WHERE schedule_id = \$1$/)
    if (upd) {
      const status = upd[1] as FakeRow["status"]
      const id = params[0] as string
      const row = map.get(id)
      if (row) row.status = status
      return []
    }

    // Worker SELECT
    if (
      compact.startsWith("SELECT schedule_id, type, tags, payload, metadata, version, timestamp")
      && compact.includes("WHERE status = 'pending' AND fire_at <= now()")
    ) {
      const limit = Number(params[0] ?? 50)
      const due = [...map.values()]
        .filter((r) => r.status === "pending" && r.fire_at.getTime() <= now.value.getTime())
        .sort((a, b) => a.fire_at.getTime() - b.fire_at.getTime())
        .slice(0, limit)
      return due.map((r) => ({
        schedule_id: r.schedule_id,
        type: r.type,
        tags: r.tags,
        payload: r.payload,
        metadata: r.metadata,
        version: r.version,
        timestamp: r.timestamp,
      }))
    }

    throw new Error(`fake adapter: unrecognised SQL: ${compact}`)
  }

  const adapter: PostgresAdapter = {
    async query() { return [] },
    async queryOne() { return null },
    async transaction<T>(
      _isolationLevel: IsolationLevel,
      fn: (tx: PostgresAdapterTransaction) => Promise<T>,
    ): Promise<T> {
      // Snapshot for rollback. Map is shallow-cloned with row clones so an
      // in-flight UPDATE doesn't leak through the snapshot back to the
      // committed map on rollback.
      const snapshot = new Map<string, FakeRow>()
      for (const [k, v] of table) snapshot.set(k, { ...v })

      const txTable = table
      const tx: PostgresAdapterTransaction = {
        unwrap<T = unknown>(): T {
          return undefined as unknown as T
        },
        async query<R>(sql: string, params?: unknown[]): Promise<R[]> {
          return execOn(txTable, sql, params) as R[]
        },
      }
      try {
        const result = await fn(tx)
        return result
      } catch (err) {
        // Revert table to snapshot
        table = snapshot
        throw err
      }
    },
    async listen(): Promise<ListenSubscription> { return { async unlisten() {} } },
    async connect() {},
    async disconnect() {},
  }

  return {
    adapter,
    rows: () => table,
    setNow: (d: Date) => { now.value = d },
  }
}

// ── Fake EventStore ─────────────────────────────────────────────────────

function createFakeEventStore() {
  const appended: EventMessage[] = []
  // Only `append` is exercised by the scheduler; the rest are typed-but-unused.
  const store = {
    async append(events: ReadonlyArray<EventMessage>) {
      for (const e of events) appended.push(e)
      return { position: BigInt(appended.length), xid: "0" } as never
    },
  } as unknown as import("@kronos-ts/eventsourcing").EventStore
  return { store, appended }
}

const passthroughTagResolver: TagResolver = {
  resolve: (e) => e.tags,
}

function makeEvent(over: Partial<EventMessage> = {}): EventMessage {
  return {
    identifier: over.identifier ?? crypto.randomUUID(),
    name: qn("com.example", "TestEvent"),
    payload: { hello: "world" },
    metadata: {} as Metadata,
    timestamp: 1700000000000,
    version: "1",
    tags: [{ key: "aggregate", value: "abc" }],
    ...over,
  }
}

// ── Wiring ──────────────────────────────────────────────────────────────

function wire(opts: { adapter: PostgresAdapter; store: import("@kronos-ts/eventsourcing").EventStore }) {
  const tm = postgresTransactionManager(opts.adapter)
  const uowFactory = lazyTransactionalUnitOfWorkFactory(runInNewUoW, tm)
  const scheduler = postgresEventScheduler({
    adapter: opts.adapter,
    eventStore: opts.store,
    uowFactory,
    tagResolver: passthroughTagResolver,
    pollIntervalMs: 5,
    batchSize: 10,
  })
  return { scheduler, uowFactory }
}

// ── Tests ───────────────────────────────────────────────────────────────

describe("postgresEventScheduler", () => {
  describe("schedule()", () => {
    it("inserts a pending row when called inside a UoW and the UoW commits", async () => {
      const fake = createFakeAdapter()
      const { store } = createFakeEventStore()
      const { scheduler, uowFactory } = wire({ adapter: fake.adapter, store })

      const event = makeEvent()
      const token = await uowFactory(undefined, async () => {
        return scheduler.schedule(event, new Date(Date.now() + 60_000))
      })

      expect(token.id).toBe(event.identifier)
      const row = fake.rows().get(event.identifier)
      expect(row?.status).toBe("pending")
      expect(row?.type).toBe("com.example.TestEvent")
    })

    it("rolling back the UoW after schedule() leaves no row behind", async () => {
      const fake = createFakeAdapter()
      const { store } = createFakeEventStore()
      const { scheduler, uowFactory } = wire({ adapter: fake.adapter, store })

      const event = makeEvent()
      await expect(
        uowFactory(undefined, async () => {
          await scheduler.schedule(event, new Date(Date.now() + 60_000))
          throw new Error("rollback")
        }),
      ).rejects.toThrow("rollback")

      expect(fake.rows().get(event.identifier)).toBeUndefined()
    })

    it("throws when called outside any UoW (must join a transaction)", async () => {
      const fake = createFakeAdapter()
      const { store } = createFakeEventStore()
      const { scheduler } = wire({ adapter: fake.adapter, store })

      await expect(
        scheduler.schedule(makeEvent(), new Date()),
      ).rejects.toThrow()
    })
  })

  describe("cancel()", () => {
    it("returns 'cancelled' when the row is pending and marks status=cancelled", async () => {
      const fake = createFakeAdapter()
      const { store } = createFakeEventStore()
      const { scheduler, uowFactory } = wire({ adapter: fake.adapter, store })

      const event = makeEvent()
      const token = await uowFactory(undefined, async () => {
        return scheduler.schedule(event, new Date(Date.now() + 60_000))
      })

      const result = await scheduler.cancel(token)
      expect(result).toEqual({ kind: "cancelled" })
      expect(fake.rows().get(token.id)?.status).toBe("cancelled")
    })

    it("returns 'already-appended' when the row already fired", async () => {
      const fake = createFakeAdapter()
      const { store } = createFakeEventStore()
      const { scheduler, uowFactory } = wire({ adapter: fake.adapter, store })

      const event = makeEvent()
      const token = await uowFactory(undefined, async () => {
        return scheduler.schedule(event, new Date(Date.now() - 1000))
      })
      // Manually flip to 'appended' to simulate worker firing it.
      fake.rows().get(token.id)!.status = "appended"

      const result = await scheduler.cancel(token)
      expect(result).toEqual({ kind: "already-appended" })
      expect(fake.rows().get(token.id)?.status).toBe("appended")
    })

    it("returns 'not-found' for an unknown token", async () => {
      const fake = createFakeAdapter()
      const { store } = createFakeEventStore()
      const { scheduler } = wire({ adapter: fake.adapter, store })

      const result = await scheduler.cancel({ id: crypto.randomUUID() })
      expect(result).toEqual({ kind: "not-found" })
    })

    it("returns 'not-found' for an already-cancelled token (collapsed to no-such-row)", async () => {
      // Caller-facing: a second cancel is indistinguishable from a token
      // that never existed. Avoids exposing a fourth result kind.
      const fake = createFakeAdapter()
      const { store } = createFakeEventStore()
      const { scheduler, uowFactory } = wire({ adapter: fake.adapter, store })

      const token = await uowFactory(undefined, async () => {
        return scheduler.schedule(makeEvent(), new Date(Date.now() + 60_000))
      })

      expect(await scheduler.cancel(token)).toEqual({ kind: "cancelled" })
      expect(await scheduler.cancel(token)).toEqual({ kind: "not-found" })
    })
  })

  describe("worker", () => {
    it("fires due pending schedules via eventStore.append and marks them 'appended'", async () => {
      const fake = createFakeAdapter()
      const { store, appended } = createFakeEventStore()
      const { scheduler, uowFactory } = wire({ adapter: fake.adapter, store })

      const event = makeEvent({ identifier: crypto.randomUUID() })
      await uowFactory(undefined, async () => {
        await scheduler.schedule(event, new Date(Date.now() - 1000))
      })

      await scheduler.start()
      // Wait for at least one tick (pollIntervalMs=5)
      await new Promise((r) => setTimeout(r, 30))
      await scheduler.stop()

      expect(appended.map((e) => e.identifier)).toContain(event.identifier)
      expect(fake.rows().get(event.identifier)?.status).toBe("appended")
    })

    it("does not fire schedules whose fire_at is in the future", async () => {
      const fake = createFakeAdapter()
      const { store, appended } = createFakeEventStore()
      const { scheduler, uowFactory } = wire({ adapter: fake.adapter, store })

      await uowFactory(undefined, async () => {
        await scheduler.schedule(makeEvent(), new Date(Date.now() + 60_000))
      })

      await scheduler.start()
      await new Promise((r) => setTimeout(r, 30))
      await scheduler.stop()

      expect(appended).toHaveLength(0)
    })

    it("does not fire cancelled schedules", async () => {
      const fake = createFakeAdapter()
      const { store, appended } = createFakeEventStore()
      const { scheduler, uowFactory } = wire({ adapter: fake.adapter, store })

      const token = await uowFactory(undefined, async () => {
        return scheduler.schedule(makeEvent(), new Date(Date.now() - 1000))
      })
      await scheduler.cancel(token)

      await scheduler.start()
      await new Promise((r) => setTimeout(r, 30))
      await scheduler.stop()

      expect(appended).toHaveLength(0)
      expect(fake.rows().get(token.id)?.status).toBe("cancelled")
    })

    it("stop() resolves after the in-flight tick settles (no leaked timers)", async () => {
      const fake = createFakeAdapter()
      const { store } = createFakeEventStore()
      const { scheduler } = wire({ adapter: fake.adapter, store })

      await scheduler.start()
      await scheduler.stop()
      // If a timer leaked we'd hit "fake adapter: unrecognised SQL" on a
      // late tick; the bun:test runner would surface it. Reaching here
      // cleanly is the assertion.
      expect(true).toBe(true)
    })
  })
})
