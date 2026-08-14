import { describe, expect, it } from "bun:test"
import { qn } from "@kronos-ts/common"
import { inMemoryEventStore } from "@kronos-ts/eventsourcing"
import {
  command,
  commandHandler,
  EventCriteria,
  event,
  eventHandler,
  inMemoryTokenStore,
  query,
  queryHandler,
  subscribingProcessor,
} from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import { z } from "zod"
import { inMemoryComponents, kronos, module } from "../kronos.js"

// ---------------------------------------------------------------------------
// ctx.query — the in-handler consult, across modules. Pricing owns a query
// handler; ordering's COMMAND handler consults it before appending, and an
// AUTOMATION consults it before sending. The AF5 analogue is injecting the
// QueryGateway into any handler.
// ---------------------------------------------------------------------------

const PriceOf = query({
  name: qn("pricing", "PriceOf"),
  payload: z.object({ sku: z.string() }),
  result: z.object({ amount: z.number() }),
})

const OrderPlaced = event({
  name: qn("ordering", "OrderPlaced"),
  payload: z.object({ orderId: z.string(), sku: z.string(), amount: z.number() }),
  tags: (p) => [{ key: "orderId", value: p.orderId }],
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
  name: "CtxQueryOrder",
  id: { orderId: z.string() },
  initial: () => ({ placed: false }),
  criteria: ({ orderId }) => EventCriteria.havingTags({ orderId }),
  evolve: (on) => [on(OrderPlaced, (s) => ({ ...s, placed: true }))],
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

describe("ctx.query", () => {
  it("lets a command handler consult another module's query handler in-UoW", async () => {
    const placeOrder = commandHandler(PlaceOrder, async ({ payload }, ctx) => {
      const price = await ctx.query(PriceOf, { sku: payload.sku })
      ctx.append(OrderPlaced, { orderId: payload.orderId, sku: payload.sku, amount: price.amount })
      return price
    })

    const app = kronos({
      components: inMemoryComponents(),
      modules: [
        module("pricing", { eventStore: inMemoryEventStore(), tokenStore: inMemoryTokenStore() }, priceOf),
        module("ordering", { eventStore: inMemoryEventStore(), tokenStore: inMemoryTokenStore() }, Order, placeOrder),
      ],
    })

    const result = (await app.commandGateway.send(PlaceOrder, { orderId: "o1", sku: "espresso" })) as {
      amount: number
    }
    expect(result.amount).toBe(4)
    await app.stop()
  })

  it("lets a query handler compose another module's query handler", async () => {
    const app = kronos({
      components: inMemoryComponents(),
      modules: [
        module("pricing", { eventStore: inMemoryEventStore(), tokenStore: inMemoryTokenStore() }, priceOf),
        module("ordering", { eventStore: inMemoryEventStore(), tokenStore: inMemoryTokenStore() }, quoteFor),
      ],
    })

    const result = (await app.queryGateway.query(QuoteFor, { sku: "espresso" })) as { quoted: number }
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

    const automation = subscribingProcessor("ctx-query-quotes").eventHandlers(onPlaced).build()

    const app = kronos({
      components: inMemoryComponents(),
      modules: [
        module("pricing", { eventStore: inMemoryEventStore(), tokenStore: inMemoryTokenStore() }, priceOf),
        module(
          "ordering",
          { eventStore: inMemoryEventStore(), tokenStore: inMemoryTokenStore() },
          Order,
          placeOrder,
          recordQuote,
          automation,
        ),
      ],
    })
    await app.commandGateway.send(PlaceOrder, { orderId: "o2", sku: "espresso" })
    // subscribing processor delivers synchronously on commit
    expect(quotes).toEqual([4])
    await app.stop()
  })
})
