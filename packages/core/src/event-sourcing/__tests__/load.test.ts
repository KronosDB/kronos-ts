import { describe, it, expect } from "bun:test"
import { z } from "zod"
import { qn, event } from "../../messaging/messages.js"
import { unitOfWork, NoActiveUnitOfWork } from "../../unit-of-work/unit-of-work.js"
import { inMemoryEventStore } from "../in-memory.js"
import { state } from "../state.js"
import { loadFunction } from "../load.js"
import type { EventStore } from "../event-store.js"

// ---------------------------------------------------------------------------
// A real log, counted. Nothing is registered anywhere: `ctx.load` is handed the
// STATE at the call site and the LOG on the deps, and that pair is the fold.
// ---------------------------------------------------------------------------

const TicketOpened = event({
  name: qn("support", "TicketOpened"),
  payload: z.object({ ticketId: z.string(), subject: z.string() }),
  tags: { ticketId: (p) => p.ticketId },
})

function countingStore(): EventStore & { sourced: number } {
  const inner = inMemoryEventStore()
  let sourced = 0
  return {
    ...inner,
    get sourced() {
      return sourced
    },
    async source(condition) {
      sourced++
      return inner.source(condition)
    },
  } as EventStore & { sourced: number }
}

const Ticket = state({
  id: { ticketId: z.string() },
  tags: ({ ticketId }) => ({ ticketId }),
  evolve: [
    () => ({ subject: "" }),
    [TicketOpened, (s, { payload }) => ({ ...s, subject: payload.subject })],
  ],
})

/** A second definition over the same events — a distinct `identity`. */
const TicketAudit = state({
  id: { ticketId: z.string() },
  tags: ({ ticketId }) => ({ ticketId }),
  evolve: [() => ({ opens: 0 }), [TicketOpened, (s) => ({ opens: s.opens + 1 })]],
})

async function seed(store: EventStore, ticketId: string, subject: string): Promise<void> {
  await store.append([
    {
      kind: "event",
      identifier: `e-${ticketId}`,
      name: TicketOpened.name,
      version: TicketOpened.version,
      payload: { ticketId, subject },
      metadata: {},
      timestamp: 1_700_000_000_000,
      tags: [{ key: "ticketId", value: ticketId }],
    },
  ])
}

describe("load", () => {
  it("throws NoActiveUnitOfWork once its unit of work has closed", async () => {
    const eventStore = countingStore()
    let load!: ReturnType<typeof loadFunction>
    await unitOfWork().execute(async (uow) => {
      load = loadFunction({ uow, eventStore })
    })
    await expect(load(Ticket, { ticketId: "t1" })).rejects.toThrow(NoActiveUnitOfWork)
  })

  it("throws naming the state (by its process identity) when no event store was attached", async () => {
    await unitOfWork().execute(async (uow) => {
      const load = loadFunction({ uow })
      await expect(load(Ticket, { ticketId: "t1" })).rejects.toThrow(
        /ctx\.load\(state#\d+.*needs a log/s,
      )
    })
  })

  it("folds the log the site named and returns the state", async () => {
    const eventStore = countingStore()
    await seed(eventStore, "t1", "Printer on fire")
    await unitOfWork().execute(async (uow) => {
      const load = loadFunction({ uow, eventStore })
      expect(await load(Ticket, { ticketId: "t1" })).toEqual({ subject: "Printer on fire" })
      expect(eventStore.sourced).toBe(1)
    })
  })

  it("caches the state promise — second load() call is a cache hit", async () => {
    const eventStore = countingStore()
    await seed(eventStore, "t1", "Intro")
    await unitOfWork().execute(async (uow) => {
      const load = loadFunction({ uow, eventStore })
      const r1 = await load(Ticket, { ticketId: "t1" })
      const r2 = await load(Ticket, { ticketId: "t1" })
      expect(r1).toEqual({ subject: "Intro" })
      expect(r2).toEqual({ subject: "Intro" })
      expect(eventStore.sourced).toBe(1)
    })
  })

  it("does NOT collide two different OBJECT ids of the same state within one UoW (gotcha #7)", async () => {
    // Regression: the cache key used String(id), so {ticketId:"A"} and
    // {ticketId:"B"} both stringified to "[object Object]" and shared one
    // entry — the second load returned the first ticket's state.
    const eventStore = countingStore()
    await seed(eventStore, "A", "first")
    await seed(eventStore, "B", "second")

    await unitOfWork().execute(async (uow) => {
      const load = loadFunction({ uow, eventStore })
      const a = await load(Ticket, { ticketId: "A" })
      const b = await load(Ticket, { ticketId: "B" })

      expect(a.subject).toBe("first")
      expect(b.subject).toBe("second") // would be "first" under the old String(id) key
      expect(eventStore.sourced).toBe(2) // distinct cache keys → two real loads
    })
  })

  it("key order in an object id does not change the cache key", async () => {
    const eventStore = countingStore()
    const TwoKeyed = state({
      id: { a: z.string(), b: z.string() },
      tags: ({ a, b }) => ({ ticketId: `${a}-${b}` }),
      evolve: [() => ({}), [TicketOpened, (s) => s]],
    })
    await unitOfWork().execute(async (uow) => {
      const load = loadFunction({ uow, eventStore })
      await load(TwoKeyed, { a: "1", b: "2" })
      await load(TwoKeyed, { b: "2", a: "1" }) // same id, other construction order → cache hit
      expect(eventStore.sourced).toBe(1)
    })
  })

  it("populates the state cache, module map and sourcing infos on first load", async () => {
    const eventStore = countingStore()
    await unitOfWork().execute(async (uow) => {
      const load = loadFunction({ uow, eventStore })
      await load(Ticket, { ticketId: "t1" })

      const key = `${Ticket.identity}:{"ticketId":"t1"}`
      expect(uow.stateCache.entries.has(key)).toBe(true)
      expect(uow.stateCache.modules.has(key)).toBe(true)
      expect(uow.events.sourcingInfos).toHaveLength(1)
      // An empty log: the marker sits below the origin, which is what the
      // append condition needs to mean "nothing was there when I read".
      expect(uow.events.sourcingInfos[0]!.markerPosition).toBe(-1n)
    })
  })

  it("sourcing infos accumulate one entry per load() call (per unique state-id)", async () => {
    const eventStore = countingStore()
    await unitOfWork().execute(async (uow) => {
      const load = loadFunction({ uow, eventStore })
      await load(Ticket, { ticketId: "t1" })
      await load(TicketAudit, { ticketId: "t1" })
      // Two distinct state-id pairs → two sourcing infos
      expect(uow.events.sourcingInfos).toHaveLength(2)
    })
  })

  it("does NOT have a phase guard — load works from inside onPrepareCommit", async () => {
    const eventStore = countingStore()
    await seed(eventStore, "t1", "Intro")
    let result: unknown = null
    let caughtError: unknown = null

    await unitOfWork().execute(async (uow) => {
      const load = loadFunction({ uow, eventStore })
      uow.onPrepareCommit(async () => {
        try {
          result = await load(Ticket, { ticketId: "t1" })
        } catch (e) {
          caughtError = e
        }
      })
    })

    // Must NOT throw WrongUoWPhase — load is read-only
    expect(caughtError).toBeNull()
    expect(result).toEqual({ subject: "Intro" })
  })

  it("the per-site repository cache is a CACHE — two units of work, one fold built", async () => {
    const eventStore = countingStore()
    await seed(eventStore, "t1", "Intro")
    await unitOfWork().execute(async (uow) => {
      await loadFunction({ uow, eventStore })(Ticket, { ticketId: "t1" })
    })
    await unitOfWork().execute(async (uow) => {
      await loadFunction({ uow, eventStore })(Ticket, { ticketId: "t1" })
    })
    // The REPOSITORY is remembered across tasks; the ANSWER is not — the
    // per-task dedupe lives on `uow.stateCache`, so each task really sourced.
    expect(eventStore.sourced).toBe(2)
  })
})
