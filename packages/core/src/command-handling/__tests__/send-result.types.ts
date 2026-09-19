/**
 * The TYPE test for WHAT `ctx.send` RESOLVES TO. Listed in the root
 * `tsconfig.json` `files` array, so `bunx tsc --noEmit` judges it.
 *
 * What it pins: `ctx.send` resolves to the command descriptor's `result`, the
 * same answer `ctx.query` and the edge verb `send(bus, …)` give — and to
 * `unknown`, not `void` or `any`, when the descriptor declares none.
 */
import { z } from "zod"
import { command, qn } from "../../messaging/messages.js"
import type { CommandHandlerContext } from "../context.js"

const Reserve = command({
  name: qn("sendprobe", "Reserve"),
  payload: z.object({ sku: z.string() }),
  result: z.object({ reserved: z.number() }),
})
const Notify = command({ name: qn("sendprobe", "Notify"), payload: z.object({ sku: z.string() }) })

export async function probe(ctx: CommandHandlerContext) {
  const reserved = await ctx.send(Reserve, { sku: "espresso" })
  const amount: number = reserved.reserved
  // @ts-expect-error — the result is the descriptor's, not `any`
  const wrong: string = reserved.reserved

  const notified = await ctx.send(Notify, { sku: "espresso" })
  // @ts-expect-error — no `result` schema means `unknown`: nothing to read off it
  notified.anything

  return { amount, wrong, notified }
}
