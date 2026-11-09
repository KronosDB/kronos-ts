/**
 * The TYPE test for WHAT A COMMAND HANDLER MAY RETURN. Listed in the root
 * `tsconfig.json` `files` array, so `bunx tsc --noEmit` judges it.
 *
 * What it pins: a handler returns its descriptor's result as plain literals,
 * with no `as const` and no annotation, whatever the shape of the handler
 * (no parameters, the message, a context, sync, statements, the options form).
 * Array literals in the return are accepted too, readonly or not. And nothing
 * the result does not allow gets through.
 *
 * Why it needs pinning: TypeScript types a handler's return while it is still
 * inferring the descriptor's result schema `R`, and widens every literal in it
 * (`{ ok: true }` becomes `{ ok: boolean }`). `commandHandler` declares `const R`
 * so the return is read in a const context, which keeps the literals; one
 * signature, because with overloads the first one tried fixes the handler's
 * type; and a return type that takes arrays readonly, because a const context
 * makes an array literal a readonly tuple.
 */
import { z } from "zod"
import { command, qn, type InferResult } from "../../messaging/messages.js"
import type { CommandHandlerContext } from "../context.js"
import { commandHandler, type CommandHandler } from "../handler.js"

type Exactly<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false
const exactly = <A, B>(proof: Exactly<A, B>): Exactly<A, B> => proof

const AddItem = command({
  name: qn("literalprobe", "AddItem"),
  payload: z.object({ itemId: z.string() }),
  result: z.discriminatedUnion("ok", [
    z.object({ ok: z.literal(true) }),
    z.object({ ok: z.literal(false), rejection: z.literal("FULL") }),
  ]),
})
const Checkout = command({
  name: qn("literalprobe", "Checkout"),
  payload: z.object({ cartId: z.string() }),
  result: z.object({
    status: z.enum(["PLACED", "EMPTY"]),
    lines: z.array(z.object({ sku: z.string(), qty: z.number() })),
    at: z.date(),
  }),
})
const ListItems = command({
  name: qn("literalprobe", "ListItems"),
  payload: z.object({ itemId: z.string() }),
  result: z.object({ items: z.array(z.string()) }),
})
const Archive = command({ name: qn("literalprobe", "Archive"), payload: z.object({ itemId: z.string() }) })

type Rich = CommandHandlerContext & { readonly full: boolean }
declare const full: boolean
declare const names: string[]

// ---------------------------------------------------------------------------
// Literals, as written.
// ---------------------------------------------------------------------------

export const noParameters = commandHandler(AddItem, async () => (full ? { ok: false, rejection: "FULL" } : { ok: true }))
export const readsTheMessage = commandHandler(AddItem, async ({ payload }) =>
  payload.itemId ? { ok: true } : { ok: false, rejection: "FULL" },
)
export const synchronous = commandHandler(AddItem, ({ payload }) =>
  payload.itemId ? { ok: true } : { ok: false, rejection: "FULL" },
)
export const statements = commandHandler(AddItem, async ({ payload }) => {
  if (!payload.itemId) return { ok: false, rejection: "FULL" }
  return { ok: true }
})
export const optionsForm = commandHandler(AddItem, {
  handler: async ({ payload }) => (payload.itemId ? { ok: true } : { ok: false, rejection: "FULL" }),
  appendCondition: (_message, query) => query,
})
export const enumAndNestedArray = commandHandler(Checkout, async ({ payload }) => ({
  status: "PLACED",
  lines: [{ sku: payload.cartId, qty: 1 }],
  at: new Date(),
}))

// ---------------------------------------------------------------------------
// Arrays, however they are built.
// ---------------------------------------------------------------------------

export const fromValues = commandHandler(ListItems, async ({ payload }) => ({ items: [payload.itemId] }))
export const empty = commandHandler(ListItems, async () => ({ items: [] }))
export const fromAVariable = commandHandler(ListItems, async () => ({ items: names.filter(Boolean) }))

// ---------------------------------------------------------------------------
// The spellings that were needed before still work.
// ---------------------------------------------------------------------------

export const asConst = commandHandler(AddItem, async () => ({ ok: true }) as const)
export const annotated = commandHandler(
  AddItem,
  async (): Promise<InferResult<typeof AddItem.result>> => (full ? { ok: false, rejection: "FULL" } : { ok: true }),
)

// ---------------------------------------------------------------------------
// The entry is what it was: the descriptor's schemas, and the context asked for.
// ---------------------------------------------------------------------------

export const withContext = commandHandler(AddItem, async (_message, ctx: Rich) =>
  ctx.full ? { ok: false, rejection: "FULL" } : { ok: true },
)
export const entryType = exactly<
  typeof withContext,
  CommandHandler<typeof AddItem.payload, NonNullable<typeof AddItem.result>, Rich>
>(true)

export const voidCommand = commandHandler(Archive, async ({ payload }) => {
  void payload.itemId
})
export const voidEntryType = exactly<typeof voidCommand, CommandHandler<typeof Archive.payload, undefined>>(true)

// ---------------------------------------------------------------------------
// What the result does not allow is still refused.
// ---------------------------------------------------------------------------

// @ts-expect-error — "EMPTY" is not a rejection the result declares
export const wrongLiteral = commandHandler(AddItem, async () => ({ ok: false, rejection: "EMPTY" }))

// @ts-expect-error — nor is it here
export const wrongLiteralFromTheMessage = commandHandler(AddItem, async ({ payload }) => (payload.itemId ? { ok: true } : { ok: false, rejection: "EMPTY" }))

// @ts-expect-error — `at` is missing
export const missingField = commandHandler(Checkout, async () => ({ status: "PLACED", lines: [] }))

// @ts-expect-error — an item is a string
export const wrongElement = commandHandler(ListItems, async () => ({ items: [1] }))

// @ts-expect-error — a command with a result must answer it
export const noAnswer = commandHandler(AddItem, async () => {})
