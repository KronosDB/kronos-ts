import { describe, expect, it } from "bun:test"
import { emptyMetadata, qn } from "../messaging/messages.js"
import { inMemoryEventStore } from "../event-sourcing/in-memory.js"
import { inMemoryDeadLetterQueue } from "../event-processing/dead-letter-queue.js"
import { type TagResolver } from "../event-sourcing/tag-resolver.js"
import {
  command,
  commandHandler,
  event,
  eventHandler,
  eventProcessor,
  correlation,
  interceptingCommandBus,
  interceptingQueryBus,
  inMemoryTokenStore,
  send,
  unitOfWork,
  localCommandBus,
  localQueryBus,
  type UnitOfWork,
} from "../index.js"
import { state } from "../event-sourcing/state.js"
import { z } from "zod"
import { kronos } from "../kronos.js"
// ===========================================================================
// The DOMAIN — identical for both composition styles. Nothing below this line
// changes between the two roots; only the assembly does.
// ===========================================================================

const BillOpened = event({
  name: qn("billing", "BillOpened"),
  payload: z.object({ billId: z.string() }),
  tags: { billId: (p) => p.billId },
})

const LineBilled = event({
  name: qn("billing", "LineBilled"),
  payload: z.object({ billId: z.string(), amount: z.number() }),
  tags: { billId: (p) => p.billId },
})

const OpenBill = command({ name: qn("billing", "OpenBill"), payload: z.object({ billId: z.string() }) })
const BillLine = command({
  name: qn("billing", "BillLine"),
  payload: z.object({ billId: z.string(), amount: z.number() }),
})

const Bill = state({
  id: { billId: z.string() },
  tags: ({ billId }) => ({ billId }),
  evolve: [
    () => ({ open: false, total: 0 }),
    [BillOpened, (s) => ({ ...s, open: true })],
    [LineBilled, (s, { payload }) => ({ ...s, total: s.total + payload.amount })],
  ],
})

/** A ledger the slice writes to — its one dependency. */
type Ledger = {
  lines: string[]
}
const newLedger = (): Ledger => ({ lines: [] })

// ===========================================================================
// Composition: dependencies are closure arguments, the event store is a bare
// property attached at the composition root — not inside the slice.
// ===========================================================================

const openBill = commandHandler(OpenBill, async ({ payload }, ctx) => {
  ctx.append(BillOpened, { billId: payload.billId })
})

const billLine = (ledger: Ledger) =>
  commandHandler(BillLine, async ({ payload }, ctx) => {
    const bill = await ctx.load(Bill, { billId: payload.billId })
    if (!bill.open) return
    ledger.lines.push(`${payload.billId}:${payload.amount}`)
    ctx.append(LineBilled, { billId: payload.billId, amount: payload.amount })
  })

/** A slice is a state plus its handlers. Deps are closure arguments. */
const billLinesSlice = (ledger: Ledger) => ({
  commandHandlers: [openBill, billLine(ledger)],
})

// ===========================================================================

describe("billing", () => {
  it("boots and bills a line against its own event store", async () => {
    const ledger = newLedger()
    const persistence = inMemoryEventStore()

    // --- composition root ---
    const slice = billLinesSlice(ledger)
    const buses = inMemoryBuses()
    const app = kronos({
      commandHandlers: slice.commandHandlers.map((h) => ({ ...h, ...buses, eventStore: persistence })),
    })
    // -------------------------

    await send(buses.commandBus, OpenBill, { billId: "b-1" }, emptyMetadata())
    await send(buses.commandBus, BillLine, { billId: "b-1", amount: 250 }, emptyMetadata())

    expect(ledger.lines).toEqual(["b-1:250"])
    await app.stop()
  })

  it("two slices, two event stores, one bus", async () => {
    const billingLedger = newLedger()
    const billingPersistence = inMemoryEventStore()
    const orderingPersistence = inMemoryEventStore()

    // A second slice with its OWN command + event, so the shared bus stays valid.
    const OrderPlaced = event({
      name: qn("ordering", "OrderPlaced"),
      payload: z.object({ orderId: z.string() }),
      tags: { orderId: (p) => p.orderId },
    })
    const PlaceOrder = command({
      name: qn("ordering", "PlaceOrder"),
      payload: z.object({ orderId: z.string() }),
    })
    const placeOrder = commandHandler(PlaceOrder, async ({ payload }, ctx) => {
      ctx.append(OrderPlaced, { orderId: payload.orderId })
    })

    const billingSlice = billLinesSlice(billingLedger)
    const buses = inMemoryBuses()
    const app = kronos({
      commandHandlers: [
        ...billingSlice.commandHandlers.map((h) => ({ ...h, ...buses, eventStore: billingPersistence })),
        { ...placeOrder, ...buses, eventStore: orderingPersistence },
      ],
    })

    await send(buses.commandBus, OpenBill, { billId: "b-9" }, emptyMetadata())
    await send(buses.commandBus, BillLine, { billId: "b-9", amount: 40 }, emptyMetadata())
    await send(buses.commandBus, PlaceOrder, { orderId: "o-1" }, emptyMetadata())

    // Each slice resolved its own store — no scope machinery, just object identity.
    expect(billingLedger.lines).toEqual(["b-9:40"])

    // And the stores really are separate.
    const billingEvents = await billingPersistence.source({
      query: { tags: { billId: "b-9" } },
    } as never)
    expect((billingEvents as { events: unknown[] }).events.length).toBeGreaterThan(0)
    await app.stop()
  })

  // NOTE ON TEST INTENT: the old API let a module override ANY component —
  // including its OWN command bus, so its handlers were unreachable from the
  // app-level gateway. `kronos` no longer has a component registry; the only
  // thing a handler can override is what the host attaches to it (eventStore /
  // tagResolver) — the command/query buses are strictly
  // app-level now. The closest surviving case is `tagResolver`: it is NOT a
  // persistence concern, so it demonstrates the same "not just persistence"
  // point within what a sited entry can still express.
  it("a handler can override tagResolver too, not just persistence", async () => {
    const ledger = newLedger()
    const eventStore = inMemoryEventStore()
    const seenEventNames: string[] = []
    const stampingTagResolver: TagResolver = (evt) => {
      seenEventNames.push(evt.name.name)
      return [{ key: "stamped", value: "yes" }]
    }

    const slice = billLinesSlice(ledger)
    const tagResolver = stampingTagResolver
    const buses = inMemoryBuses()
    const app = kronos({
      commandHandlers: slice.commandHandlers.map((h) => ({ ...h, ...buses, eventStore, tagResolver })),
    })

    await send(buses.commandBus, OpenBill, { billId: "x" }, emptyMetadata())

    expect(seenEventNames).toContain("BillOpened")
    const { events } = await eventStore.source({
      query: { tags: { billId: "x" } },
    } as never)
    expect(events[0]!.tags).toEqual(
      expect.arrayContaining([{ key: "stamped", value: "yes" }]),
    )
    await app.stop()
  })
})

// ---------------------------------------------------------------------------
// Component resolution order — the silent-transaction-loss regression
// ---------------------------------------------------------------------------

/**
 * The two things `kronos` needs that are not handlers. The UoW runner is
 * named once and handed to `localCommandBus` (which captures it at
 * construction) — writing it on an adjacent line is what makes that checkable.
 */
function inMemoryBuses(uow: () => UnitOfWork = unitOfWork) {
  return {
    commandBus: interceptingCommandBus(localCommandBus(uow), correlation),
    queryBus: interceptingQueryBus(localQueryBus(uow), correlation),
  }
}

describe("component resolution", () => {
  it("a supplied unitOfWork factory reaches the default command bus", async () => {
    // The trap: localCommandBus captures the UoW factory when BUILT.
    // Spreading a fully-built record under a backend used to leave the bus on
    // the bare factory while the supplied one said otherwise — handlers then ran
    // outside the transaction and a rollback silently kept its row.
    let ranThrough = 0
    const countingUoW: typeof unitOfWork = ((metadata: never, fn: never) => {
      ranThrough++
      return (unitOfWork as (m: never, f: never) => unknown)(metadata, fn)
    }) as typeof unitOfWork

    const ledger = newLedger()
    const eventStore = inMemoryEventStore()
    const slice = billLinesSlice(ledger)
    // The bus captures THIS UoW factory, because the factory is named before
    // the bus is built and both are handed over on adjacent lines.
    const buses = inMemoryBuses(countingUoW)
    const app = kronos({
      commandHandlers: slice.commandHandlers.map((h) => ({ ...h, ...buses, eventStore })),
    })

    await send(buses.commandBus, OpenBill, { billId: "uow-1" }, emptyMetadata())
    expect(ranThrough).toBeGreaterThan(0)
    await app.stop()
  })

  it("exposes live processor instances, not just descriptors", async () => {
    const ledger = newLedger()
    const eventStore = inMemoryEventStore()
    const slice = billLinesSlice(ledger)
    const buses = inMemoryBuses()
    const app = kronos({
      commandHandlers: slice.commandHandlers.map((h) => ({ ...h, ...buses, eventStore })),
    })
    // No processors here, but the surface exists for control planes
    // (Axon/KronosDB) that need real instances to honour pause/split/merge.
    expect(app.processors).toBeInstanceOf(Map)
    await app.stop()
  })
})

// ---------------------------------------------------------------------------
// correlation — end-to-end through the DEFAULTS, nothing seeded by hand
// ---------------------------------------------------------------------------

const BillClosed = event({
  name: qn("billing", "BillClosed"),
  payload: z.object({ billId: z.string() }),
  tags: { billId: (p) => p.billId },
})

describe("correlation", () => {
  it("a command sent from a handler inherits the incoming command's correlation", async () => {
    const CloseBill = command({ name: qn("billing", "CloseBill"), payload: z.object({ billId: z.string() }) })
    const seen: Array<Record<string, unknown>> = []

    const closeBill = commandHandler(CloseBill, async ({ payload, metadata }, ctx) => {
      seen.push(metadata as Record<string, unknown>)
      ctx.append(BillClosed, { billId: payload.billId })
    })
    const openAndClose = commandHandler(OpenBill, async ({ payload }, ctx) => {
      ctx.append(BillOpened, { billId: payload.billId })
      await ctx.send(CloseBill, { billId: payload.billId })   // nested dispatch
    })

    const eventStore = inMemoryEventStore()
    const buses = inMemoryBuses()
    const app = kronos({
      commandHandlers: [
        { ...openAndClose, ...buses, eventStore },
        { ...closeBill, ...buses, eventStore },
      ],
    })
    await send(buses.commandBus, OpenBill, { billId: "lin-1" }, emptyMetadata())

    // The nested command carries the ORIGIN of the first — extract (invocation)
    // + apply (dispatch interceptor), both running on the framework defaults.
    expect(seen).toHaveLength(1)
    expect(seen[0]!.correlationId).toBeDefined()
    expect(seen[0]!.causationId).toBeDefined()
    await app.stop()
  })
})

// ---------------------------------------------------------------------------
// Boot errors — persistence is attached at composition, and kronos says so
// ---------------------------------------------------------------------------

describe("boot errors", () => {
  const onOpened = eventHandler(BillOpened, async () => {})

  it("a dead-letter queue without a sequence is rejected where the processor is built", () => {
    // Parking is a LANE operation — the queue parks a failed event AND
    // everything behind it in the same lane — so a queue with no lane is not a
    // configuration the framework can honour.
    expect(() =>
      eventProcessor({
        name: "bill-projection",
        eventStore: inMemoryEventStore(),
        tokenStore: inMemoryTokenStore(),
        unitOfWork,
        deadLetterQueue: inMemoryDeadLetterQueue(),
      }),
    ).toThrow(/deadLetterQueue was given without a sequence/)
  })

  it("an event handler with no processor is named, not silently undelivered", () => {
    const buses = inMemoryBuses()
    expect(() =>
      kronos({ eventHandlers: [{ ...onOpened, ...buses } as never] }),
    ).toThrow(/has no processor/)
  })

  it("two handlers naming one processor share ONE delivery", async () => {
    const buses = inMemoryBuses()
    const eventStore = inMemoryEventStore()
    const projection = eventProcessor({
      name: "bill-projection",
      eventStore,
      tokenStore: inMemoryTokenStore(),
      unitOfWork,
    })
    const onLine = eventHandler(LineBilled, async () => {})
    const app = kronos({
      eventHandlers: [
        { ...onOpened, ...buses, processor: projection },
        { ...onLine, ...buses, processor: projection },
      ],
    })
    expect(app.processors.size).toBe(1)
    expect(app.processors.get("bill-projection")).toBeDefined()
    await app.stop()
  })

  it("the same NAME with an equal config is still one delivery — name is the durable identity", async () => {
    const buses = inMemoryBuses()
    const eventStore = inMemoryEventStore()
    const tokenStore = inMemoryTokenStore()
    const config = { name: "bill-projection", eventStore, tokenStore, unitOfWork } as const
    const app = kronos({
      eventHandlers: [
        { ...onOpened, ...buses, processor: eventProcessor({ ...config }) },
        { ...eventHandler(LineBilled, async () => {}), ...buses, processor: eventProcessor({ ...config }) },
      ],
    })
    expect(app.processors.size).toBe(1)
    await app.stop()
  })

  it("the same NAME with a CONFLICTING config is a boot error naming both entries", () => {
    const buses = inMemoryBuses()
    const tokenStore = inMemoryTokenStore()
    const shared = { name: "bill-projection", tokenStore, unitOfWork }
    expect(() =>
      kronos({
        eventHandlers: [
          {
            ...onOpened,
            ...buses,
            name: "opened-view",
            processor: eventProcessor({ ...shared, eventStore: inMemoryEventStore() }),
          },
          {
            ...eventHandler(LineBilled, async () => {}),
            ...buses,
            name: "lines-view",
            processor: eventProcessor({ ...shared, eventStore: inMemoryEventStore() }),
          },
        ],
      }),
    ).toThrow(/disagree on eventStore[\s\S]*opened-view[\s\S]*lines-view/)
  })

  it("a command handler with no event store is named", () => {
    const buses = inMemoryBuses()
    expect(() =>
      kronos({ commandHandlers: [{ ...openBill, ...buses }] }),
    ).toThrow(/no event store was attached/)
  })
})
