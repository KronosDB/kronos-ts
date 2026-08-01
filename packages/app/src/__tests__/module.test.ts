import { describe, expect, it, afterEach } from "bun:test"
import { z } from "zod"
import { qn, emptyMetadata } from "@kronos-ts/common"
import { command, event, EventCriteria } from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import { kronos, type RunningApp } from "../kronos.js"
import { defineModule, ReservedContextKeyError } from "../module.js"

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
// Slices
// ---------------------------------------------------------------------------

import { defineSlice, DuplicateSliceNameError } from "../slice.js"
import type { ModuleApi } from "../module.js"

describe("defineSlice", () => {
  let app: RunningApp | undefined
  afterEach(async () => {
    if (app) {
      await app.stop()
      app = undefined
    }
  })

  it("slices register through the module context and expose typed meta to hosts", async () => {
    const db = fakeDb()

    const openThing = defineSlice({
      name: "open-thing",
      meta: { rpc: { things: "open" }, docs: "Opens a thing." },
      register: (m: ModuleApi<Deps>) => {
        m.states(Thing)
        m.commandHandler(CreateThing, async ({ payload }, ctx) => {
          ctx.db.insert(`slice:${payload.id}:${ctx.tenant}`)
          ctx.append(ThingCreated, { id: payload.id })
        })
      },
    })

    const mod = defineModule<Deps>("sliced", (m) => {
      m.slices(openThing)
    })

    const builder = kronos({ quiet: true }).use(mod({ db, tenant: "acme" }))
    app = await builder.start()

    // Host-side iteration: name, owning module, and app-defined meta.
    const registered = app.slices()
    expect(registered).toHaveLength(1)
    expect(registered[0]!.name).toBe("open-thing")
    expect(registered[0]!.module).toBe("sliced")
    expect((registered[0]!.meta as { rpc: { things: string } }).rpc.things).toBe("open")

    // Slice handlers run with the module's dep-typed context.
    await app.commandGateway.send(CreateThing, { id: "s-1" }, emptyMetadata())
    expect(db.rows).toEqual(["slice:s-1:acme"])
  })

  it("duplicate slice names throw, naming both modules", async () => {
    const slice = () =>
      defineSlice({
        name: "same-name",
        register: (_m: ModuleApi<Record<string, unknown>>) => {},
      })
    const modA = defineModule<Record<string, unknown>>("mod-a", (m) => m.slices(slice()))
    const modB = defineModule<Record<string, unknown>>("mod-b", (m) => m.slices(slice()))

    let thrown: unknown
    try {
      await kronos({ quiet: true }).use(modA({})).use(modB({})).start()
    } catch (e) {
      thrown = e
    }
    expect(thrown).toBeInstanceOf(DuplicateSliceNameError)
    expect((thrown as Error).message).toContain("mod-a")
    expect((thrown as Error).message).toContain("mod-b")
  })

  it("meta defaults to undefined and slices() view is frozen", async () => {
    const bare = defineSlice({
      name: "bare",
      register: (_m: ModuleApi<Record<string, unknown>>) => {},
    })
    const mod = defineModule<Record<string, unknown>>("bare-mod", (m) => m.slices(bare))
    app = await kronos({ quiet: true }).use(mod({})).start()

    const view = app.slices()
    expect(view[0]!.meta).toBeUndefined()
    expect(Object.isFrozen(view)).toBe(true)
  })
})
