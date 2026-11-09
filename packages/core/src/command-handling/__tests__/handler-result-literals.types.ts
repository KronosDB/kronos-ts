/**
 * The TYPE test for WHAT A COMMAND HANDLER MAY RETURN when the descriptor's
 * result has literal members. Listed in the root `tsconfig.json` `files`
 * array, so `bunx tsc --noEmit` judges it.
 *
 * What it pins, and why. `commandHandler` infers the descriptor's result
 * schema `R` from its first argument and types the handler's return as
 * `InferOutput<R>`. TypeScript types the handler's return while `R` is still
 * being inferred: it decides whether a literal stays a literal against
 * `InferOutput<R>` unresolved, whose constraint is `unknown`, so `{ ok: true }`
 * widens to `{ ok: boolean }` and the call is refused. It caches that type, so
 * no overload order changes it, and `NoInfer` does not either.
 *
 * The one signature change that helps, `const R`, keeps the literals only in
 * a handler that reads its message (`async ({ payload }) => …`); one that takes
 * no arguments is still refused. It also turns an array literal built from
 * values (`{ items: [payload.itemId] }`) into a readonly tuple, which a
 * `z.array` output refuses. So the signature stays, and a handler says the
 * literal in one of the two ways below. The `@ts-expect-error` on the widened spelling goes red if a
 * TypeScript release stops widening it; then the note on `commandHandler` and
 * this file can go.
 */
import { z } from "zod"
import { command, qn, type InferResult } from "../../messaging/messages.js"
import type { CommandHandlerContext } from "../context.js"
import { commandHandler } from "../handler.js"

const AddItem = command({
  name: qn("literalprobe", "AddItem"),
  payload: z.object({ itemId: z.string() }),
  result: z.discriminatedUnion("ok", [
    z.object({ ok: z.literal(true) }),
    z.object({ ok: z.literal(false), rejection: z.literal("FULL") }),
  ]),
})
const ListItems = command({
  name: qn("literalprobe", "ListItems"),
  payload: z.object({ itemId: z.string() }),
  result: z.object({ items: z.array(z.string()), count: z.number() }),
})

// The limitation: a literal in the returned object widens.
// @ts-expect-error — `{ ok: boolean }` is not the result's `{ ok: true } | …`
export const widened = commandHandler(AddItem, async ({ payload }, _ctx: CommandHandlerContext) =>
  payload.itemId ? { ok: true } : { ok: false, rejection: "FULL" },
)

// Say it with `as const` …
export const asConst = commandHandler(AddItem, async ({ payload }, _ctx: CommandHandlerContext) =>
  payload.itemId ? ({ ok: true } as const) : ({ ok: false, rejection: "FULL" } as const),
)

// … or annotate the handler's return with the descriptor's result.
export const annotated = commandHandler(
  AddItem,
  async ({ payload }, _ctx: CommandHandlerContext): Promise<InferResult<typeof AddItem.result>> =>
    payload.itemId ? { ok: true } : { ok: false, rejection: "FULL" },
)

// Either way a wrong literal is still refused.
// @ts-expect-error — "EMPTY" is not a rejection the result declares
export const wrong = commandHandler(AddItem, async () => ({ ok: false, rejection: "EMPTY" }) as const)

// What the signature keeps: array literals in a result stay mutable arrays.
export const arrays = commandHandler(ListItems, async ({ payload }) => ({ items: [payload.itemId], count: 1 }))
export const empty = commandHandler(ListItems, async () => ({ items: [], count: 0 }))
