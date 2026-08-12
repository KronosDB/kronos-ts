import { describe, expect, it } from "bun:test"
import { emptyMetadata, qn } from "@kronos-ts/common"
import { createInMemoryEventStore } from "@kronos-ts/eventsourcing"
import { command, commandHandler, EventCriteria, event } from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import { z } from "zod"
import { createApp, inMemoryComponents, module } from "../create-app.js"
import { createInMemoryTokenStore, createSimpleCommandBus } from "@kronos-ts/messaging"

// ===========================================================================
// The DOMAIN — identical for both composition styles. Nothing below this line
// changes between the two roots; only the assembly does.
// ===========================================================================

const BillOpened = event({
  name: qn("billing", "BillOpened"),
  payload: z.object({ billId: z.string() }),
  tags: (p) => [{ key: "billId", value: p.billId }],
})

const LineBilled = event({
  name: qn("billing", "LineBilled"),
  payload: z.object({ billId: z.string(), amount: z.number() }),
  tags: (p) => [{ key: "billId", value: p.billId }],
})

const OpenBill = command({ name: qn("billing", "OpenBill"), payload: z.object({ billId: z.string() }) })
const BillLine = command({
  name: qn("billing", "BillLine"),
  payload: z.object({ billId: z.string(), amount: z.number() }),
})

const Bill = state({
  name: "Bill",
  id: { billId: z.string() },
  initial: () => ({ open: false, total: 0 }),
  criteria: ({ billId }) => EventCriteria.havingTags({ billId }),
  evolve: (on) => [
    on(BillOpened, (s) => ({ ...s, open: true })),
    on(LineBilled, (s, { payload }) => ({ ...s, total: s.total + payload.amount })),
  ],
})

/** A ledger the module writes to — the module's one dependency. */
interface Ledger {
  lines: string[]
}
const newLedger = (): Ledger => ({ lines: [] })

// ===========================================================================
// Composition: dependencies are closure arguments, the event store is a field.
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

/** Local test convenience — the framework deliberately ships no such bundle. */
const inMemoryStores = () => ({
  eventStore: createInMemoryEventStore(),
  tokenStore: createInMemoryTokenStore(),
})

/** A slice is a LIST of registrations. Deps are closure arguments. */
const billLinesSlice = (ledger: Ledger) => [Bill, openBill, billLine(ledger)]

const billingModule = (ledger: Ledger, persistence: ReturnType<typeof inMemoryStores>) =>
  module("billing", persistence, ...billLinesSlice(ledger))

// ===========================================================================

describe("billing", () => {
  it("boots and bills a line against its own event store", async () => {
    const ledger = newLedger()
    const persistence = inMemoryStores()

    // --- composition root (functional) ---
    const app = createApp({
      components: inMemoryComponents(),
      modules: [billingModule(ledger, persistence)],
    })
    // -------------------------------------

    await app.commandGateway.send(OpenBill, { billId: "b-1" }, emptyMetadata())
    await app.commandGateway.send(BillLine, { billId: "b-1", amount: 250 }, emptyMetadata())

    expect(ledger.lines).toEqual(["b-1:250"])
    await app.stop()
  })

  it("two modules, two event stores, one bus", async () => {
    const billingLedger = newLedger()
    const billingPersistence = inMemoryStores()
    const orderingPersistence = inMemoryStores()

    // A second module with its OWN command + event, so the shared bus stays valid.
    const OrderPlaced = event({
      name: qn("ordering", "OrderPlaced"),
      payload: z.object({ orderId: z.string() }),
      tags: (p) => [{ key: "orderId", value: p.orderId }],
    })
    const PlaceOrder = command({
      name: qn("ordering", "PlaceOrder"),
      payload: z.object({ orderId: z.string() }),
    })
    const placeOrder = commandHandler(PlaceOrder, async ({ payload }, ctx) => {
      ctx.append(OrderPlaced, { orderId: payload.orderId })
    })

    const app = createApp({
      components: inMemoryComponents(),
      modules: [
        billingModule(billingLedger, billingPersistence),
        module("ordering", orderingPersistence, placeOrder),
      ],
    })

    await app.commandGateway.send(OpenBill, { billId: "b-9" }, emptyMetadata())
    await app.commandGateway.send(BillLine, { billId: "b-9", amount: 40 }, emptyMetadata())
    await app.commandGateway.send(PlaceOrder, { orderId: "o-1" }, emptyMetadata())

    // Each module resolved its own store — no scope machinery, just `??`.
    expect(billingLedger.lines).toEqual(["b-9:40"])
    expect(app.stateManagers.get("billing")).not.toBe(app.stateManagers.get("ordering"))

    // And the stores really are separate.
    const billingEvents = await billingPersistence.eventStore.source({
      criteria: EventCriteria.havingTags({ billId: "b-9" }),
    } as never)
    expect((billingEvents as { events: unknown[] }).events.length).toBeGreaterThan(0)
    await app.stop()
  })

  it("a module can override ANY component, not just persistence", async () => {
    const ledger = newLedger()
    // This module runs on its own command bus as well as its own store, so its
    // handlers are NOT reachable from the app-level gateway. Nothing about the
    // override mechanism privileges persistence.
    const ownBus = createSimpleCommandBus()
    const app = createApp({
      components: inMemoryComponents(),
      modules: [
        module("billing", { ...inMemoryStores(), commandBus: ownBus }, ...billLinesSlice(ledger)),
      ],
    })

    // The app gateway has no handler for it — the module took its own bus.
    await expect(
      app.commandGateway.send(OpenBill, { billId: "x" }, emptyMetadata()),
    ).rejects.toThrow()

    // Dispatched on the module's OWN bus, it works.
    await ownBus.dispatch({
      kind: "command",
      identifier: "c-1",
      name: OpenBill.name,
      payload: { billId: "x" },
      metadata: emptyMetadata(),
      timestamp: 1,
    })
    await ownBus.dispatch({
      kind: "command",
      identifier: "c-2",
      name: BillLine.name,
      payload: { billId: "x", amount: 7 },
      metadata: emptyMetadata(),
      timestamp: 2,
    })
    expect(ledger.lines).toEqual(["x:7"])
    await app.stop()
  })
})

// ---------------------------------------------------------------------------
// Component resolution order — the silent-transaction-loss regression
// ---------------------------------------------------------------------------

import { runInNewUoW } from "@kronos-ts/messaging"

describe("component resolution", () => {
  it("a supplied unitOfWorkFactory reaches the default command bus", async () => {
    // The trap: createSimpleCommandBus captures the UoW factory when BUILT.
    // Spreading a fully-built record under a backend used to leave the bus on
    // runInNewUoW while unitOfWorkFactory said otherwise — handlers then ran
    // outside the transaction and a rollback silently kept its row.
    let ranThrough = 0
    const countingUoW: typeof runInNewUoW = ((metadata: never, fn: never) => {
      ranThrough++
      return (runInNewUoW as (m: never, f: never) => unknown)(metadata, fn)
    }) as typeof runInNewUoW

    const ledger = newLedger()
    const app = createApp({
      // PARTIAL record — createApp resolves the bus after the merge.
      components: { unitOfWorkFactory: countingUoW },
      modules: [module("billing", inMemoryStores(), ...billLinesSlice(ledger))],
    })

    await app.commandGateway.send(OpenBill, { billId: "uow-1" }, emptyMetadata())
    expect(ranThrough).toBeGreaterThan(0)
    await app.stop()
  })

  it("exposes live processor instances, not just descriptors", async () => {
    const ledger = newLedger()
    const app = createApp({
      modules: [module("billing", inMemoryStores(), ...billLinesSlice(ledger))],
    })
    // No processors registered here, but the surface exists for control planes
    // (Axon/KronosDB) that need real instances to honour pause/split/merge.
    expect(app.processors).toBeInstanceOf(Map)
    await app.stop()
  })
})
