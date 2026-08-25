/**
 * SUBSCRIPTION QUERIES RUN THE DISPATCH CHAIN. `interceptingQueryBus` wraps
 * `subscriptionQuery` and `subscribeToUpdates` with the same intercept the
 * primary `query` gets — so a correlation intercept stamps subscription
 * queries too, and they cross a transport carrying their metadata.
 *
 * This test retires a stale KNOWN-GAP comment that claimed the opposite.
 */
import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, queryDescriptor } from "../../messaging/messages.js"
import { localQueryBus } from "../../query-handling/local-bus.js"
import { subscriptionQuery } from "../../query-handling/subscription-query.js"
import { interceptingQueryBus, type Intercept } from "../intercepting-bus.js"
import { unitOfWork } from "../../unit-of-work/unit-of-work.js"
import type { QueryMessage } from "../../messaging/messages.js"

const Watch = queryDescriptor({
  name: qn("probe", "Watch"),
  payload: z.object({ id: z.string() }),
})

const stamp: Intercept<QueryMessage> = (m) => ({
  ...m,
  metadata: { ...m.metadata, correlationId: String(m.metadata.correlationId ?? m.identifier) },
})

describe("interceptingQueryBus — the subscription tier runs the intercept", () => {
  it("subscriptionQuery reaches the delegate with the intercept applied", async () => {
    const seen: QueryMessage[] = []
    const inner = localQueryBus(unitOfWork)
    const spied = {
      ...inner,
      subscriptionQuery: (m: QueryMessage, n?: number) => {
        seen.push(m)
        return inner.subscriptionQuery(m, n)
      },
    }
    const bus = interceptingQueryBus(spied, stamp)

    inner.subscribe("probe.Watch", async () => 1)
    const result = subscriptionQuery(bus, Watch, { id: "x" })
    await result.initialResult
    result.close()

    expect(seen).toHaveLength(1)
    expect(seen[0]!.metadata.correlationId).toBe(seen[0]!.identifier)
  })

  it("subscribeToUpdates reaches the delegate with the intercept applied", () => {
    const seen: QueryMessage[] = []
    const inner = localQueryBus(unitOfWork)
    const spied = {
      ...inner,
      subscribeToUpdates: (m: QueryMessage, n?: number) => {
        seen.push(m)
        return inner.subscribeToUpdates(m, n)
      },
    }
    const bus = interceptingQueryBus(spied, stamp)

    const stream = bus.subscribeToUpdates(
      { identifier: "q-1", kind: "query", name: qn("probe", "Watch"), payload: { id: "x" }, metadata: {} },
      8,
    )
    stream.close()

    expect(seen).toHaveLength(1)
    expect(seen[0]!.metadata.correlationId).toBe("q-1")
  })
})
