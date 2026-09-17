import { describe as suite, expect, it } from "bun:test"
import { chainError, chainOf, chainProblems, describe, descriptionOf } from "../describe.js"
import { correlatingHandler } from "../../correlation/correlating-handler.js"
import { loggingHandler } from "../../logging/logging-handler.js"
import { consoleLogger } from "../../logging/console-logger.js"
import { kronos } from "../../kronos.js"
import { localCommandBus } from "../../command-handling/local-bus.js"
import { localQueryBus } from "../../query-handling/local-bus.js"
import { inMemoryEventStore } from "../../event-sourcing/in-memory.js"
import { unitOfWork } from "../../unit-of-work/unit-of-work.js"
import { transactional } from "../../unit-of-work/transactional.js"
import { command, commandHandler, qn } from "../../index.js"
import { z } from "zod"

const noop = async () => {}
const logger = consoleLogger({ write: () => {} })

/** A stand-in tracing wrapper: supplies `trace`, stamps the message. */
const tracing = <H extends (...args: never[]) => unknown>(next: H) =>
  describe(((...args: never[]) => next(...args)) as unknown as H, {
    name: "tracingHandler",
    supplies: ["trace"],
    stamps: ["message.metadata"],
    next,
  })

/** A stand-in adapter wrapper that USES `trace` (an observed handle). */
const observedDb = <H extends (...args: never[]) => unknown>(next: H) =>
  describe(((...args: never[]) => next(...args)) as unknown as H, {
    name: "drizzleHandler",
    supplies: ["db"],
    uses: ["trace"],
    hints: { trace: "Put otlpHandler outside drizzleHandler, or use a plain handle instead of observed(db) if tracing is not wanted." },
    next,
  })

suite("describe — a wrapper that says what it does", () => {
  it("marks the very function it is given, without changing it", async () => {
    const fresh = async () => {}
    const wrapped = describe(fresh, { name: "x", next: undefined })
    expect(wrapped).toBe(fresh)
    expect(descriptionOf(wrapped)?.name).toBe("x")
    expect(Object.keys(wrapped)).toEqual([])
    expect(descriptionOf(noop)).toBeUndefined()
  })

  it("walks a chain outermost first and skips undescribed links", () => {
    const inner = correlatingHandler(noop as never)
    const anonymous = (...args: never[]) => (inner as never as (...a: never[]) => unknown)(...args)
    const outer = tracing(anonymous)
    // `anonymous` breaks the walk: it carries no description, so nothing behind it is seen.
    expect(chainOf(outer).map((d) => d.name)).toEqual(["tracingHandler"])
    // Described all the way down, the walk sees every link.
    expect(chainOf(tracing(loggingHandler(inner as never, logger))).map((d) => d.name)).toEqual([
      "tracingHandler",
      "loggingHandler",
      "correlatingHandler",
    ])
  })
})

suite("chainProblems — the three rules", () => {
  it("accepts a chain in the right order", () => {
    const chain = chainOf(tracing(loggingHandler(correlatingHandler(observedDb(noop as never)) as never, logger)))
    expect(chainProblems(chain)).toEqual([])
  })

  it("refuses a wrapper that uses a capability nothing outside it supplies, with the wrapper's own hint", () => {
    const chain = chainOf(observedDb(tracing(noop)))
    expect(chainProblems(chain)).toEqual([
      'drizzleHandler uses "trace", but nothing outside it supplies it.\n' +
        "  → Put otlpHandler outside drizzleHandler, or use a plain handle instead of observed(db) if tracing is not wanted.",
    ])
  })

  it("refuses a capability supplied twice", () => {
    const chain = chainOf(tracing(observedDb(observedDb(noop))))
    expect(chainProblems(chain)).toEqual([
      '"db" is supplied twice (drizzleHandler, then drizzleHandler); the inner drizzleHandler shadows the outer one.\n' +
        "  → Remove one of them.",
    ])
  })

  it("refuses a message stamp inside the wrapper that reads it", () => {
    const chain = chainOf(correlatingHandler(tracing(noop) as never))
    expect(chainProblems(chain)).toEqual([
      "tracingHandler stamps message.metadata, but correlatingHandler reads it outside tracingHandler, " +
        "so correlatingHandler sees the message before the stamp.\n" +
        "  → Move tracingHandler outside correlatingHandler.",
    ])
  })
})

suite("kronos() — refuses a chain that cannot work, before subscribing anything", () => {
  const Place = command({ name: qn("orders", "Place"), payload: z.object({ id: z.string() }) })
  const place = commandHandler(Place, async () => {})
  const eventStore = inMemoryEventStore()
  const commandBus = localCommandBus(unitOfWork)
  const queryBus = localQueryBus(unitOfWork)

  it("names the entry, prints the chain, says what is wrong and how to fix it", () => {
    const handler = correlatingHandler(tracing(place.handler as never) as never)
    expect(() =>
      kronos({ commandHandlers: [{ ...place, handler: handler as never, eventStore, commandBus, queryBus }] }),
    ).toThrow(
      'kronos: entry "orders.Place" is not configured properly.\n' +
        "  chain: correlatingHandler → tracingHandler\n" +
        "  tracingHandler stamps message.metadata, but correlatingHandler reads it outside tracingHandler, " +
        "so correlatingHandler sees the message before the stamp.\n" +
        "  → Move tracingHandler outside correlatingHandler.",
    )
  })

  it("boots a chain in the right order", async () => {
    const handler = tracing(correlatingHandler(place.handler as never))
    const app = kronos({ commandHandlers: [{ ...place, handler: handler as never, eventStore, commandBus, queryBus }] })
    await app.stop()
  })
})

suite("localQueryBus — refuses a transactional factory at construction", () => {
  it("throws a sentence naming the fix", () => {
    const family = transactional(unitOfWork)
    expect(() => localQueryBus(family as never)).toThrow(/plain `unitOfWork` factory/)
  })

  it("accepts the plain factory", () => {
    expect(() => localQueryBus(unitOfWork)).not.toThrow()
  })
})
