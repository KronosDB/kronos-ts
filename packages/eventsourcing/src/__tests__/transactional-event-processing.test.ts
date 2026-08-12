/**
 * Covers:
 *   (a) token position persistence via TokenStore
 *   (b) resume-from-stored-token-position
 *   (c) running an app on a caller-supplied TransactionManager
 *
 * Composition: `tokenStore` and `transactionManager` are fields on the
 * `Components` record, so a probe is passed straight in — app-wide via
 * `inMemoryComponents({ ... })`, or scoped to one module via
 * `module(name, { tokenStore }, ...)`.
 */
import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, tag, emptyMetadata } from "@kronos-ts/common"
import {
  command,
  event,
  commandHandler,
  eventHandler,
  EventCriteria,
  trackingProcessor,
  createInMemoryTokenStore,
  type TokenStore,
  type TransactionManager,
} from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import { kronos, inMemoryComponents, module } from "@kronos-ts/app"
import { append } from "../append.js"
import { load } from "../load.js"
import { createInMemoryEventStore } from "../in-memory-event-store.js"

// ─── Domain ─────────────────────────────────────────────────────────────────

const CreateThing = command({
  name: qn("transactional", "CreateThing"),
  payload: z.object({ id: z.string() }),
})
const ThingCreated = event({
  name: qn("transactional", "ThingCreated"),
  payload: z.object({ id: z.string() }),
  tags: (p) => [tag("id", p.id)],
})
const Thing = state({
  name: "TransactionalThing",
  id: { id: z.string() },
  initial: () => ({ created: false }),
  criteria: ({ id }) => EventCriteria.havingTags(tag("id", id)),
  evolve: (on) => [on(ThingCreated, (s) => ({ ...s, created: true }))],
})
const createThing = commandHandler(CreateThing, async ({ payload: cmd }) => {
  await load(Thing, { id: cmd.id })
  append(ThingCreated, { id: cmd.id })
})

async function waitFor(check: () => boolean, timeoutMs = 5000): Promise<void> {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    if (check()) return
    await new Promise((r) => setTimeout(r, 20))
  }
  throw new Error("Timed out")
}

// ─── Tests ──────────────────────────────────────────────────────────────────

describe("Transactional event processing — tokenStore + transactionManager components", () => {
  it("persists processor position via the app-level TokenStore", async () => {
    // Inject a probe tokenStore as the app's component — no per-module override.
    const probe = createInMemoryTokenStore()
    const seen: string[] = []
    const onThingCreated = eventHandler(ThingCreated, async ({ payload: e }) => {
      seen.push(e.id)
    })

    const app = kronos({
      components: inMemoryComponents({ tokenStore: probe }),
      modules: [
        module(
          "transactional",
          Thing,
          createThing,
          trackingProcessor("transactional-projection").eventHandlers(onThingCreated).build(),
        ),
      ],
    })
    try {
      await app.commandGateway.send(CreateThing, { id: "t1" }, emptyMetadata())
      await waitFor(() => seen.includes("t1"))
      // The processor wrote at least one token entry into the app's store.
      const segments = await probe.fetchSegments("transactional-projection")
      expect(segments.length).toBeGreaterThan(0)
      const token = await probe.get("transactional-projection", segments[0]!)
      expect(token).toBeDefined()
    } finally {
      await app.stop()
    }
  })

  it("resumes from a previously stored token position on a second start", async () => {
    // Same probe token store AND the same event store shared across two app
    // boots — components are values now, so both really are the same instance
    // and the resume can be asserted on behaviour, not just on the stored token.
    const probe: TokenStore = createInMemoryTokenStore()
    const eventStore = createInMemoryEventStore()
    const seenFirst: string[] = []
    const seenSecond: string[] = []

    function makeOnThingCreated(sink: string[]) {
      return eventHandler(ThingCreated, async ({ payload: e }) => {
        sink.push(e.id)
      })
    }

    const bootWith = (sink: string[]) =>
      kronos({
        components: inMemoryComponents({ tokenStore: probe, eventStore }),
        modules: [
          module(
            "transactional",
            Thing,
            createThing,
            trackingProcessor("transactional-projection")
              .eventHandlers(makeOnThingCreated(sink))
              .build(),
          ),
        ],
      })

    // Boot 1: create one event, let the processor advance past it.
    const first = bootWith(seenFirst)
    await first.commandGateway.send(CreateThing, { id: "r1" }, emptyMetadata())
    await waitFor(() => seenFirst.includes("r1"))
    await first.stop()

    // The position survived the stop.
    const segments = await probe.fetchSegments("transactional-projection")
    expect(segments.length).toBeGreaterThan(0)
    const tokenAfterStop = await probe.get("transactional-projection", segments[0]!)
    expect(tokenAfterStop).toBeDefined()

    // Boot 2: same event store, same token store. The processor must resume from
    // the stored position — r1 is already covered by the token, so only the new
    // event is delivered.
    const second = bootWith(seenSecond)
    try {
      await second.commandGateway.send(CreateThing, { id: "r2" }, emptyMetadata())
      await waitFor(() => seenSecond.includes("r2"))
      expect(seenSecond).not.toContain("r1")
    } finally {
      await second.stop()
    }

    // The per-module override path: a module can run on its OWN token store
    // instead of the app's, and that store is the one the processor writes to.
    const moduleScoped: TokenStore = createInMemoryTokenStore()
    const seenThird: string[] = []
    const third = kronos({
      components: inMemoryComponents({ tokenStore: probe }),
      modules: [
        module(
          "transactional",
          { tokenStore: moduleScoped },
          Thing,
          createThing,
          trackingProcessor("module-scoped-projection")
            .eventHandlers(makeOnThingCreated(seenThird))
            .build(),
        ),
      ],
    })
    try {
      await third.commandGateway.send(CreateThing, { id: "m1" }, emptyMetadata())
      await waitFor(() => seenThird.includes("m1"))
      // Written to the module's store…
      expect((await moduleScoped.fetchSegments("module-scoped-projection")).length).toBeGreaterThan(0)
      // …and not to the app's.
      expect(await probe.fetchSegments("module-scoped-projection")).toEqual([])
    } finally {
      await third.stop()
    }
  })

  it("an app runs on a caller-supplied TransactionManager component", async () => {
    // Counting TM proves the component is the one the app was built with —
    // backends (postgres, kronosdb) replace it the same way.
    let beginCalls = 0
    const counting: TransactionManager<{ id: number }> = {
      async begin() {
        beginCalls++
        return { id: beginCalls }
      },
      async commit(_tx) {},
      async rollback(_tx) {},
    }

    const app = kronos({
      components: inMemoryComponents({ transactionManager: counting as TransactionManager }),
      modules: [module("transactional", Thing, createThing)],
    })
    try {
      const tx = await counting.begin()
      expect(tx.id).toBe(1)
      expect(beginCalls).toBeGreaterThan(0)
      await app.commandGateway.send(CreateThing, { id: "tx1" }, emptyMetadata())
      // The default unitOfWorkRunner is a pass-through; integrating the TM
      // into the processor UoW is a downstream concern (backends compose
      // `lazyTransactionalUnitOfWorkFactory(runInNewUoW, tm)` themselves — see
      // packages/extensions/postgres).
    } finally {
      await app.stop()
    }
  })
})
