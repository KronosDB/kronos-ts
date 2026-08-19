/**
 * Covers:
 *   (a) token position persistence via TokenStore
 *   (b) resume-from-stored-token-position
 *   (c) running an app on a caller-supplied TransactionManager
 *
 * Composition: `tokenStore` and `transactionManager` are fields on the
 * `Components` record, so a probe is passed straight in — app-wide via
 * the entry's own bare properties, or scoped to one module via
 * `module(name, { tokenStore }, ...)`.
 */
import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn } from "../../primitives/qualified-name.js"
import { emptyMetadata } from "../../primitives/metadata.js"
import { command, event, commandHandler, eventHandler, eventProcessor, inMemoryTokenStore, send, type TokenStore } from "../../index.js"
import { state } from "../../state/state.js"
import { kronos } from "../../assembly/kronos.js"
import { lineage, interceptingCommandBus, interceptingQueryBus, unitOfWork, simpleCommandBus, simpleQueryBus, type UnitOfWork } from "../../index.js"
import { inMemoryEventStore } from "../../stores/in-memory-event-store.js"

/**
 * The three things `kronos` needs that are not modules. The UoW runner is named
 * once and handed to BOTH `simpleCommandBus` (which captures it at construction)
 * and `kronos` — writing them on adjacent lines is what makes that checkable.
 */
function inMemoryBuses(uow: () => UnitOfWork = unitOfWork) {
  return {
    commandBus: interceptingCommandBus(simpleCommandBus(uow), lineage),
    queryBus: interceptingQueryBus(simpleQueryBus(uow), lineage),
  }
}


// ─── Domain ─────────────────────────────────────────────────────────────────

const CreateThing = command({
  name: qn("transactional", "CreateThing"),
  payload: z.object({ id: z.string() }),
})
const ThingCreated = event({
  name: qn("transactional", "ThingCreated"),
  payload: z.object({ id: z.string() }),
  tags: { id: (p) => p.id },
})
const Thing = state({
  name: "TransactionalThing",
  id: { id: z.string() },
  initial: () => ({ created: false }),
  tags: ({ id }) => ({ id: id }),
  evolve: [[ThingCreated, (s) => ({ ...s, created: true })]],
})
const createThing = commandHandler(CreateThing, async ({ payload: cmd }, ctx) => {
  await ctx.load(Thing, { id: cmd.id })
  ctx.append(ThingCreated, { id: cmd.id })
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
    // A probe token store, attached to the processor entry at composition.
    const probe = inMemoryTokenStore()
    const seen: string[] = []
    const onThingCreated = eventHandler(ThingCreated, async ({ payload: e }) => {
      seen.push(e.id)
    })

    const eventStore = inMemoryEventStore()
    const buses = inMemoryBuses()
    const app = kronos({
      states: [{ ...Thing, eventStore }],
      commandHandlers: [{ ...createThing, eventStore, ...buses }],
      eventHandlers: [
        {
          ...onThingCreated,
          ...buses,
          processor: eventProcessor({
            name: "transactional-projection",
            eventStore,
            tokenStore: probe,
            unitOfWork,
          }),
        },
      ],
    })
    try {
      await send(buses.commandBus, CreateThing, { id: "t1" }, emptyMetadata())
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
    const probe: TokenStore = inMemoryTokenStore()
    const eventStore = inMemoryEventStore()
    const seenFirst: string[] = []
    const seenSecond: string[] = []

    function makeOnThingCreated(sink: string[]) {
      return eventHandler(ThingCreated, async ({ payload: e }) => {
        sink.push(e.id)
      })
    }

    const bootWith = (sink: string[]) => {
      const buses = inMemoryBuses()
      const app = kronos({
        states: [{ ...Thing, eventStore }],
        commandHandlers: [{ ...createThing, eventStore, ...buses }],
        eventHandlers: [
          {
            ...makeOnThingCreated(sink),
            ...buses,
            processor: eventProcessor({
              name: "transactional-projection",
              eventStore,
              tokenStore: probe,
              unitOfWork,
            }),
          },
        ],
      })
      return { app, ...buses }
    }

    // Boot 1: create one event, let the processor advance past it.
    const first = bootWith(seenFirst)
    await send(first.commandBus, CreateThing, { id: "r1" }, emptyMetadata())
    await waitFor(() => seenFirst.includes("r1"))
    await first.app.stop()

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
      await send(second.commandBus, CreateThing, { id: "r2" }, emptyMetadata())
      await waitFor(() => seenSecond.includes("r2"))
      expect(seenSecond).not.toContain("r1")
    } finally {
      await second.app.stop()
    }

    // The per-processor path: one processor can run on its OWN token store, and
    // that store is the one it writes to — the difference is one property in the
    // `.map()`, not a different kind of composition.
    const moduleScoped: TokenStore = inMemoryTokenStore()
    const seenThird: string[] = []
    const thirdLog = inMemoryEventStore()
    const thirdBuses = inMemoryBuses()
    const third = kronos({
      states: [{ ...Thing, eventStore: thirdLog }],
      commandHandlers: [{ ...createThing, eventStore: thirdLog, ...thirdBuses }],
      eventHandlers: [
        {
          ...makeOnThingCreated(seenThird),
          ...thirdBuses,
          processor: eventProcessor({
            name: "module-scoped-projection",
            eventStore: thirdLog,
            tokenStore: moduleScoped,
            unitOfWork,
          }),
        },
      ],
    })
    try {
      await send(thirdBuses.commandBus, CreateThing, { id: "m1" }, emptyMetadata())
      await waitFor(() => seenThird.includes("m1"))
      // Written to the module's store…
      expect((await moduleScoped.fetchSegments("module-scoped-projection")).length).toBeGreaterThan(0)
      // …and not to the app's.
      expect(await probe.fetchSegments("module-scoped-projection")).toEqual([])
    } finally {
      await third.stop()
    }
  })

})
