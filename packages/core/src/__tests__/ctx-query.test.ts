import { describe, expect, it } from "bun:test"
import { qn } from "../messaging/messages.js"
import { inMemoryEventStore } from "../event-sourcing/in-memory.js"
import {
  command,
  commandHandler,
  event,
  eventHandler,
  eventProcessor,
  inMemoryTokenStore,
  query,
  queryHandler,
  send,
  correlation,
  interceptingCommandBus,
  interceptingQueryBus,
  unitOfWork,
  localCommandBus,
  localQueryBus,
} from "../index.js"
import { state } from "../event-sourcing/state.js"
import { z } from "zod"
import { kronos } from "../kronos.js"
/**
 * The three things `kronos` needs that are not modules. The UoW runner is named
 * once and handed to BOTH `localCommandBus` (which captures it at construction)
 * and `kronos` — writing them on adjacent lines is what makes that checkable.
 */
function inMemoryBuses(uow: () => UnitOfWork = unitOfWork) {
  return {
    commandBus: interceptingCommandBus(localCommandBus(uow), correlation),
    queryBus: interceptingQueryBus(localQueryBus(uow), correlation),
  }
}


// ---------------------------------------------------------------------------
// ctx.query — the in-handler consult, across modules. Pricing owns a query
// handler; ordering's COMMAND handler consults it before appending, and an
// AUTOMATION consults it before sending. The AF5 analogue is injecting the
// query gateway into any handler.
// ---------------------------------------------------------------------------

const PriceOf = query({
  name: qn("pricing", "PriceOf"),
  payload: z.object({ sku: z.string() }),
  result: z.object({ amount: z.number() }),
})

const OrderPlaced = event({
  name: qn("ordering", "OrderPlaced"),
  payload: z.object({ orderId: z.string(), sku: z.string(), amount: z.number() }),
  tags: { orderId: (p) => p.orderId },
})

const PlaceOrder = command({
  name: qn("ordering", "PlaceOrder"),
  payload: z.object({ orderId: z.string(), sku: z.string() }),
  result: z.object({ amount: z.number() }),
})

const RecordQuote = command({
  name: qn("ordering", "RecordQuote"),
  payload: z.object({ orderId: z.string(), amount: z.number() }),
})

const Order = state({
  id: { orderId: z.string() },
  tags: ({ orderId }) => ({ orderId }),
  evolve: [() => ({ placed: false }), [OrderPlaced, (s) => ({ ...s, placed: true })]],
})

const priceOf = queryHandler(PriceOf, async ({ payload }) => ({ amount: payload.sku === "espresso" ? 4 : 2 }))

const QuoteFor = query({
  name: qn("ordering", "QuoteFor"),
  payload: z.object({ sku: z.string() }),
  result: z.object({ quoted: z.number() }),
})

/** A read composing another module's read — ctx.query from a QUERY handler. */
const quoteFor = queryHandler(QuoteFor, async ({ payload }, ctx) => {
  const price = await ctx.query(PriceOf, { sku: payload.sku })
  return { quoted: price.amount * 2 }
})

async function waitFor(check: () => boolean, timeoutMs = 5000): Promise<void> {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    if (check()) return
    await new Promise((r) => setTimeout(r, 10))
  }
  throw new Error("Timed out")
}

describe("ctx.query", () => {
  it("lets a command handler consult another module's query handler in-UoW", async () => {
    const placeOrder = commandHandler(PlaceOrder, async ({ payload }, ctx) => {
      const price = await ctx.query(PriceOf, { sku: payload.sku })
      ctx.append(OrderPlaced, { orderId: payload.orderId, sku: payload.sku, amount: price.amount })
      return price
    })

    const pricingLog = inMemoryEventStore()
    const orderingLog = inMemoryEventStore()
    const buses = inMemoryBuses()
    const app = kronos({
      queryHandlers: [{ ...priceOf, ...buses, eventStore: pricingLog }],
      commandHandlers: [{ ...placeOrder, ...buses, eventStore: orderingLog }],
    })

    const result = (await send(buses.commandBus, PlaceOrder, { orderId: "o1", sku: "espresso" })) as {
      amount: number
    }
    expect(result.amount).toBe(4)
    await app.stop()
  })

  it("lets a query handler compose another module's query handler", async () => {
    const buses = inMemoryBuses()
    const app = kronos({
      queryHandlers: [
        { ...priceOf, ...buses, eventStore: inMemoryEventStore() },
        { ...quoteFor, ...buses, eventStore: inMemoryEventStore() },
      ],
    })

    const result = (await query(buses.queryBus, QuoteFor, { sku: "espresso" })) as { quoted: number }
    expect(result.quoted).toBe(8)
    await app.stop()
  })

  it("lets an automation consult before sending its command", async () => {
    const quotes: number[] = []
    const recordQuote = commandHandler(RecordQuote, async ({ payload }) => {
      quotes.push(payload.amount)
    })

    const onPlaced = eventHandler(OrderPlaced, async ({ payload }, ctx) => {
      const price = await ctx.query(PriceOf, { sku: payload.sku })
      await ctx.send(RecordQuote, { orderId: payload.orderId, amount: price.amount })
    })

    const placeOrder = commandHandler(PlaceOrder, async ({ payload }, ctx) => {
      ctx.append(OrderPlaced, { orderId: payload.orderId, sku: payload.sku, amount: 0 })
      return { amount: 0 }
    })

    const orderingLog = inMemoryEventStore()
    const buses = inMemoryBuses()
    const app = kronos({
      queryHandlers: [{ ...priceOf, ...buses, eventStore: inMemoryEventStore() }],
      commandHandlers: [
        { ...placeOrder, ...buses, eventStore: orderingLog },
        { ...recordQuote, ...buses, eventStore: orderingLog },
      ],
      eventHandlers: [
        {
          ...onPlaced,
          ...buses,
          processor: eventProcessor({
            name: "ctx-query-quotes",
            eventStore: orderingLog,
            tokenStore: inMemoryTokenStore(),
            unitOfWork,
          }),
        },
      ],
    })
    await send(buses.commandBus, PlaceOrder, { orderId: "o2", sku: "espresso" })
    // Delivery is TRACKED now — there is no on-commit lane — so the assertion
    // waits for the processor to catch up rather than assuming synchrony.
    await waitFor(() => quotes.length > 0)
    expect(quotes).toEqual([4])
    await app.stop()
  })
})
