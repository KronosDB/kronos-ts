import { afterEach, describe, expect, it } from "bun:test"
import { emptyMetadata, qn } from "@kronos-ts/common"
import { command, commandHandler, EventCriteria, event, query, queryHandler } from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import { z } from "zod"
import { kronos, type RunningApp } from "../kronos.js"

const ThingCreated = event({
  name: qn("query-ctx", "ThingCreated"),
  payload: z.object({ id: z.string() }),
  tags: (p) => [{ key: "id", value: p.id }],
})
const CreateThing = command({ name: qn("query-ctx", "CreateThing"), payload: z.object({ id: z.string() }) })
const GetThingState = query({ name: qn("query-ctx", "GetThingState"), payload: z.object({ id: z.string() }) })

const Thing = state({
  name: "QueryCtxThing",
  id: { id: z.string() },
  initial: () => ({ created: false }),
  criteria: ({ id }) => EventCriteria.havingTags({ id }),
  evolve: (on) => [on(ThingCreated, (s) => ({ ...s, created: true }))],
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
})
