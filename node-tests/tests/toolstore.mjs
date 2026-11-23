// The tool store every test uses: a command that can refuse, a command that lands once, a projection.
import { commandHandler, eventHandler, queryHandler, state, withNamespace } from "@kronos-ts/core"
import { z } from "zod"

export const ns = withNamespace("toolstore")

export const AddTool = ns.command("AddTool", {
  payload: z.object({ toolId: z.string().min(1), name: z.string().min(1) }),
  result: z.object({ ok: z.literal(true) }),
})
export const RentTool = ns.command("RentTool", {
  payload: z.object({ toolId: z.string().min(1), customerId: z.string().min(1) }),
  result: z.union([
    z.object({ ok: z.literal(true) }),
    z.object({ ok: z.literal(false), rejection: z.literal("TOOL_UNAVAILABLE"), toolId: z.string() }),
  ]),
})
export const ToolAdded = ns.event("ToolAdded", {
  payload: z.object({ toolId: z.string(), name: z.string() }),
  tags: { toolId: (payload) => payload.toolId },
})
export const ToolRented = ns.event("ToolRented", {
  payload: z.object({ toolId: z.string(), customerId: z.string() }),
  tags: { toolId: (payload) => payload.toolId, customerId: (payload) => payload.customerId },
})
export const GetInventory = ns.query("GetInventory", {
  payload: z.object({}),
  result: z.array(z.object({ toolId: z.string(), available: z.boolean() })),
})

const Tool = state({
  id: { toolId: z.string() },
  tags: (id) => ({ toolId: id.toolId }),
  evolve: [
    () => ({ added: false, rentedBy: [] }),
    [ToolAdded, (tool) => ({ ...tool, added: true })],
    [ToolRented, (tool, { payload }) => ({ ...tool, rentedBy: [...tool.rentedBy, payload.customerId] })],
  ],
})

export const addTool = commandHandler(AddTool, async ({ payload }, ctx) => {
  const tool = await ctx.load(Tool, { toolId: payload.toolId })
  if (!tool.added) ctx.append(ToolAdded, payload)
  return { ok: true }
})

export const rentTool = commandHandler(RentTool, async ({ payload }, ctx) => {
  const tool = await ctx.load(Tool, { toolId: payload.toolId })
  if (tool.rentedBy.includes(payload.customerId)) return { ok: true }
  if (!tool.added || tool.rentedBy.length > 0) return { ok: false, rejection: "TOOL_UNAVAILABLE", toolId: payload.toolId }
  ctx.append(ToolRented, payload)
  return { ok: true }
})

/** An in-memory inventory, one per app. */
export function memoryInventory() {
  const rows = new Map()
  return {
    eventHandlers: [
      eventHandler(ToolAdded, ({ payload }) => void rows.set(payload.toolId, { toolId: payload.toolId, available: true })),
      eventHandler(ToolRented, ({ payload }) => void rows.set(payload.toolId, { toolId: payload.toolId, available: false })),
    ],
    queryHandler: queryHandler(GetInventory, () => [...rows.values()].sort((a, b) => a.toolId.localeCompare(b.toolId))),
  }
}

/** Resolves once the processor has handled every event in the log. */
export async function caughtUp(processor, eventStore) {
  const head = await eventStore.getHeadPosition()
  const deadline = Date.now() + 10_000
  while (processor.position < head) {
    if (Date.now() > deadline) throw new Error(`${processor.name} did not catch up to ${head}`)
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
}
