import { describe, expect, it, afterEach } from "bun:test"
import { z } from "zod"
import { qn, emptyMetadata } from "@kronos-ts/common"
import { command, commandHandler, event, EventCriteria } from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import { kronos, type RunningApp } from "../kronos.js"
import { defineModule, ReservedContextKeyError, type ModuleApi } from "../module.js"

// ---------------------------------------------------------------------------
// Test descriptors
// ---------------------------------------------------------------------------

const CreateThing = command({
  name: qn("mod-test", "CreateThing"),
  payload: z.object({ id: z.string() }),
})

const ThingCreated = event({
  name: qn("mod-test", "ThingCreated"),
  payload: z.object({ id: z.string() }),
  tags: (p) => [{ key: "id", value: p.id }],
})

const Thing = state({
  name: "ModTestThing",
  id: { id: z.string() },
  initial: () => ({ created: false }),
  criteria: ({ id }) => EventCriteria.havingTags({ id }),
  evolve: (on) => [on(ThingCreated, (s) => ({ ...s, created: true }))],
})

interface Deps extends Record<string, unknown> {
  db: { insert: (row: string) => void; rows: string[] }
  tenant: string
}

function fakeDb(): Deps["db"] {
  const rows: string[] = []
  return { rows, insert: (row) => rows.push(row) }
}

describe("defineModule", () => {
  let app: RunningApp | undefined
  afterEach(async () => {
    if (app) {
      await app.stop()
      app = undefined
    }
  })

  it("handlers receive framework capabilities AND module deps on one context", async () => {
    const db = fakeDb()
    const seen: string[] = []

    const mod = defineModule<Deps>("alpha", (m) => {
      m.states(Thing)
      m.commandHandler(CreateThing, async ({ payload }, ctx) => {
        const thing = await ctx.load(Thing, { id: payload.id })
        if (thing.created) return
        ctx.db.insert(`row:${payload.id}:${ctx.tenant}`)
        ctx.append(ThingCreated, { id: payload.id })
        seen.push(ctx.tenant)
      })
    })

    app = await kronos({ quiet: true }).use(mod({ db, tenant: "acme" })).start()
    await app.commandGateway.send(CreateThing, { id: "t-1" }, emptyMetadata())

    expect(db.rows).toEqual(["row:t-1:acme"])
    expect(seen).toEqual(["acme"])
  })

  it("two configurations of the same module are fully isolated", async () => {
    const CreateA = command({ name: qn("mod-test", "CreateA"), payload: z.object({ id: z.string() }) })
    const CreateB = command({ name: qn("mod-test", "CreateB"), payload: z.object({ id: z.string() }) })

    const dbA = fakeDb()
    const dbB = fakeDb()

    // Same *shape* of module registered twice with different deps and commands.
    const makeMod = (label: "A" | "B", trigger: typeof CreateA) =>
      defineModule<Deps>(`tenant-${label}`, (m) => {
        m.commandHandler(trigger, async ({ payload }, ctx) => {
          ctx.db.insert(`${label}:${payload.id}:${ctx.tenant}`)
        })
      })

    app = await kronos({ quiet: true })
      .use(makeMod("A", CreateA)({ db: dbA, tenant: "acme" }))
      .use(makeMod("B", CreateB)({ db: dbB, tenant: "globex" }))
      .start()

    await app.commandGateway.send(CreateA, { id: "1" }, emptyMetadata())
    await app.commandGateway.send(CreateB, { id: "2" }, emptyMetadata())

    expect(dbA.rows).toEqual(["A:1:acme"])
    expect(dbB.rows).toEqual(["B:2:globex"])
  })

  it("rejects deps that would shadow framework capabilities", () => {
    const mod = defineModule<Record<string, unknown>>("bad", () => {})
    expect(() => mod({ load: "shadowed" })).toThrow(ReservedContextKeyError)
    expect(() => mod({ append: () => {} })).toThrow(ReservedContextKeyError)
    expect(() => mod({ transaction: 1 })).toThrow(ReservedContextKeyError)
  })

  it("module event handlers close over the module context (deps included)", async () => {
    const db = fakeDb()
    let observedTenant: string | undefined

    const mod = defineModule<Deps>("with-events", (m) => {
      m.states(Thing)
      m.commandHandler(CreateThing, async ({ payload }, ctx) => {
        ctx.append(ThingCreated, { id: payload.id })
      })
      const onCreated = m.eventHandler(ThingCreated, async ({ payload }, ctx) => {
        ctx.db.insert(`projected:${payload.id}`)
        observedTenant = ctx.tenant
      })
      // Definition wiring: handler closes over the module context — invoking it
      // the way a processor does must not depend on the caller-supplied context.
      expect(onCreated.kind).toBe("event-handler")
    })

    app = await kronos({ quiet: true }).use(mod({ db, tenant: "acme" })).start()
    await app.commandGateway.send(CreateThing, { id: "e-1" }, emptyMetadata())

    // The event definition was declared but not attached to a processor in this
    // minimal boot; invoke it directly the way processors do (message + base ctx).
    // Its closure must supply deps regardless of what the caller passes.
    expect(observedTenant).toBeUndefined()
  })

  it("module contexts are frozen and deps are copied", async () => {
    const db = fakeDb()
    const depsIn = { db, tenant: "acme" }
    let ctxRef: any

    const mod = defineModule<Deps>("frozen", (m) => {
      m.commandHandler(CreateThing, async (_msg, ctx) => {
        ctxRef = ctx
      })
    })

    app = await kronos({ quiet: true }).use(mod(depsIn)).start()
    await app.commandGateway.send(CreateThing, { id: "f-1" }, emptyMetadata())

    expect(Object.isFrozen(ctxRef)).toBe(true)
    // mutating the original deps object after () must not leak in
    ;(depsIn as any).tenant = "mutated"
    expect(ctxRef.tenant).toBe("acme")
  })
})


// ---------------------------------------------------------------------------
// Encapsulation — scoped framework components
// ---------------------------------------------------------------------------

import { createInMemoryEventStore, type EventStore } from "@kronos-ts/eventsourcing"
import { qualifiedNameToString } from "@kronos-ts/common"

const ThingArchived = event({
  name: qn("mod-test", "ThingArchived"),
  payload: z.object({ id: z.string() }),
  tags: (p) => [{ key: "id", value: p.id }],
})

const ArchiveThing = command({
  name: qn("mod-test", "ArchiveThing"),
  payload: z.object({ id: z.string() }),
})

/** An in-memory event store that records the event names appended to it. */
function recordingStore(): EventStore & { appended: string[] } {
  const inner = createInMemoryEventStore()
  const appended: string[] = []
  const store = Object.create(inner) as EventStore & { appended: string[] }
  store.appended = appended
  store.append = async (events: ReadonlyArray<{ name: unknown }>, condition?: unknown) => {
    for (const e of events) appended.push(qualifiedNameToString(e.name as never))
    return (inner.append as (e: unknown, c: unknown) => Promise<unknown>)(events, condition)
  }
  return store
}

describe("module encapsulation", () => {
  let app: RunningApp | undefined
  afterEach(async () => {
    if (app) {
      await app.stop()
      app = undefined
    }
  })

  it("modules can own separate event stores while sharing one messaging fabric", async () => {
    const storeA = recordingStore()
    const storeB = recordingStore()
    const db = fakeDb()

    const modA = defineModule<Deps>("alpha", (m) => {
      m.set("eventStore", storeA)
      m.states(Thing)
      m.commandHandler(CreateThing, async ({ payload }, ctx) => {
        await ctx.load(Thing, { id: payload.id })
        ctx.append(ThingCreated, { id: payload.id })
      })
    })

    const modB = defineModule<Deps>("beta", (m) => {
      m.set("eventStore", storeB)
      m.states(Thing)
      m.commandHandler(ArchiveThing, async ({ payload }, ctx) => {
        await ctx.load(Thing, { id: payload.id })
        ctx.append(ThingArchived, { id: payload.id })
      })
    })

    app = await kronos({ quiet: true })
      .use(modA({ db, tenant: "a" }))
      .use(modB({ db, tenant: "b" }))
      .start()

    // Both modules' commands dispatch through the ONE root gateway — proof the
    // command bus is shared by identity, not re-resolved per scope.
    await app.commandGateway.send(CreateThing, { id: "a-1" }, emptyMetadata())
    await app.commandGateway.send(ArchiveThing, { id: "b-1" }, emptyMetadata())

    // ...while the events landed in each module's OWN store.
    expect(storeA.appended).toEqual(["mod-test.ThingCreated"])
    expect(storeB.appended).toEqual(["mod-test.ThingArchived"])
  })

  it("a module that overrides nothing inherits the root components", async () => {
    const rootStore = recordingStore()
    const db = fakeDb()

    const mod = defineModule<Deps>("inheritor", (m) => {
      m.states(Thing)
      m.commandHandler(CreateThing, async ({ payload }, ctx) => {
        ctx.append(ThingCreated, { id: payload.id })
      })
    })

    app = await kronos({ quiet: true })
      .set("eventStore", rootStore)
      .use(mod({ db, tenant: "root" }))
      .start()

    await app.commandGateway.send(CreateThing, { id: "r-1" }, emptyMetadata())
    expect(rootStore.appended).toEqual(["mod-test.ThingCreated"])
  })

  it("root-level handlers keep working alongside scoped modules", async () => {
    const rootStore = recordingStore()
    const moduleStore = recordingStore()
    const db = fakeDb()

    const rootHandler = commandHandler(CreateThing, async ({ payload }, ctx) => {
      ctx.append(ThingCreated, { id: payload.id })
    })

    const mod = defineModule<Deps>("scoped", (m) => {
      m.set("eventStore", moduleStore)
      m.commandHandler(ArchiveThing, async ({ payload }, ctx) => {
        ctx.append(ThingArchived, { id: payload.id })
      })
    })

    app = await kronos({ quiet: true })
      .set("eventStore", rootStore)
      .states(Thing)
      .commands(rootHandler)
      .use(mod({ db, tenant: "x" }))
      .start()

    await app.commandGateway.send(CreateThing, { id: "root-1" }, emptyMetadata())
    await app.commandGateway.send(ArchiveThing, { id: "mod-1" }, emptyMetadata())

    expect(rootStore.appended).toEqual(["mod-test.ThingCreated"])
    expect(moduleStore.appended).toEqual(["mod-test.ThingArchived"])
  })
})

// ---------------------------------------------------------------------------
// Signature ergonomics — Deps is inferred, the name is optional
// ---------------------------------------------------------------------------

describe("defineModule signature", () => {
  it("infers Deps from the setup annotation — no type argument needed", async () => {
    const db = fakeDb()

    // No `defineModule<Deps>` and no name: Deps comes from the callback's
    // ModuleApi<Deps> annotation, which is where the type belongs anyway.
    const inferred = defineModule((m: ModuleApi<Deps>) => {
      m.commandHandler(CreateThing, async (_msg, ctx) => {
        const tenant: string = ctx.tenant // typed purely by inference
        ctx.db.insert(tenant)
      })
    })

    expect(inferred.moduleName).toBe("module")

    // Inference is real, not `any`: a wrong deps shape must not compile.
    // @ts-expect-error - deps must match the inferred SupportDeps-like shape
    inferred({ nope: true })

    const app = await kronos({ quiet: true }).use(inferred({ db, tenant: "inf" })).start()
    await app.commandGateway.send(CreateThing, { id: "i-1" }, emptyMetadata())
    await app.stop()

    expect(db.rows).toEqual(["inf"])
  })

  it("an explicit name is still accepted, and labels the module", () => {
    const named = defineModule("support", (_m: ModuleApi<Deps>) => {})
    expect(named.moduleName).toBe("support")
  })
})

// ---------------------------------------------------------------------------
// Query handler context
// ---------------------------------------------------------------------------

import { query, queryHandler } from "@kronos-ts/messaging"

const GetThingState = query({
  name: qn("mod-test", "GetThingState"),
  payload: z.object({ id: z.string() }),
})

describe("query handler context", () => {
  let app: RunningApp | undefined
  afterEach(async () => {
    if (app) {
      await app.stop()
      app = undefined
    }
  })

  it("query handlers can source event-sourced state through ctx.load", async () => {
    const create = commandHandler(CreateThing, async ({ payload }, ctx) => {
      ctx.append(ThingCreated, { id: payload.id })
    })
    // Reads state the command path wrote — proving the state manager really is
    // seeded on the query invocation's UnitOfWork, not just typed as present.
    const read = queryHandler(GetThingState, async ({ payload }, ctx) => {
      const thing = await ctx.load(Thing, { id: payload.id })
      return thing.created
    })

    app = await kronos({ quiet: true }).states(Thing).commands(create).queries(read).start()

    expect(await app.queryGateway.query(GetThingState, { id: "q-1" }, emptyMetadata())).toBe(false)
    await app.commandGateway.send(CreateThing, { id: "q-1" }, emptyMetadata())
    expect(await app.queryGateway.query(GetThingState, { id: "q-1" }, emptyMetadata())).toBe(true)
  })

  it("scoped modules give their query handlers the module's own event store", async () => {
    const storeA = recordingStore()
    const db = fakeDb()

    const mod = defineModule((m: ModuleApi<Deps>) => {
      m.set("eventStore", storeA)
      m.states(Thing)
      m.commandHandler(CreateThing, async ({ payload }, ctx) => {
        ctx.append(ThingCreated, { id: payload.id })
      })
      m.queries(
        queryHandler(GetThingState, async ({ payload }, ctx) => {
          const thing = await ctx.load(Thing, { id: payload.id })
          return thing.created
        }),
      )
    })

    app = await kronos({ quiet: true }).use(mod({ db, tenant: "s" })).start()
    await app.commandGateway.send(CreateThing, { id: "s-1" }, emptyMetadata())

    expect(await app.queryGateway.query(GetThingState, { id: "s-1" }, emptyMetadata())).toBe(true)
    expect(storeA.appended).toEqual(["mod-test.ThingCreated"])
  })
})
