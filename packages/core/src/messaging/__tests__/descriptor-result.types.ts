/**
 * The TYPE test for READING A DESCRIPTOR'S RESULT. Listed in the root
 * `tsconfig.json` `files` array, so `bunx tsc --noEmit` judges it.
 *
 * What it pins: `InferResult<typeof D.result>` is the result schema's output.
 * `result` is optional on a command, so `typeof D.result` carries an
 * `undefined`; that must not turn the answer into `unknown`. A command without
 * a result still reads as `unknown`. A query always has one.
 */
import { z } from "zod"
import { command, qn, queryDescriptor, withNamespace, type InferResult, type QueryDescriptor } from "../messages.js"
import type { CommandBus } from "../../command-handling/bus.js"
import type { CommandHandlerContext } from "../../command-handling/context.js"
import { send } from "../../command-handling/send.js"
import type { QueryBus } from "../../query-handling/bus.js"
import { query } from "../../query-handling/query.js"

type Exactly<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false
const exactly = <A, B>(proof: Exactly<A, B>): Exactly<A, B> => proof

const AddItem = command({
  name: qn("resultprobe", "AddItem"),
  payload: z.object({ itemId: z.string() }),
  result: z.discriminatedUnion("ok", [
    z.object({ ok: z.literal(true) }),
    z.object({ ok: z.literal(false), rejection: z.literal("FULL") }),
  ]),
})
const Archive = command({ name: qn("resultprobe", "Archive"), payload: z.object({ itemId: z.string() }) })
const CountItems = queryDescriptor({
  name: qn("resultprobe", "CountItems"),
  payload: z.object({}),
  result: z.object({ count: z.number() }),
})

type Added = { ok: true } | { ok: false; rejection: "FULL" }

export const fromTheField = exactly<InferResult<typeof AddItem.result>, Added>(true)
export const fromTheSchema = exactly<InferResult<NonNullable<typeof AddItem.result>>, Added>(true)
export const fromAQuery = exactly<InferResult<typeof CountItems.result>, { count: number }>(true)
export const none = exactly<InferResult<typeof Archive.result>, unknown>(true)
export const undeclared = exactly<InferResult<undefined>, unknown>(true)

// @ts-expect-error — the result is the schema's output, not `unknown`
export const notUnknown = exactly<InferResult<typeof AddItem.result>, unknown>(true)

// The bus verbs and the context verbs answer the same type, so a host can
// implement `ctx.send` and `ctx.query` with `send` and `query` themselves.
export const edgeOverBuses = (commandBus: CommandBus, queryBus: QueryBus): Pick<CommandHandlerContext, "send" | "query"> => ({
  send: (descriptor, payload, metadata) => send(commandBus, descriptor, payload, metadata),
  query: (descriptor, payload, metadata) => query(queryBus, descriptor, payload, metadata),
})

// A query always declares what it answers. Every way of declaring one refuses a
// definition without `result`.
const probe = withNamespace("resultprobe")

// @ts-expect-error — `queryDescriptor` without a result
export const bareDescriptor = queryDescriptor({ name: qn("resultprobe", "Bare"), payload: z.object({}) })
// @ts-expect-error — `query({ … })` without a result
export const bareQuery = query({ name: qn("resultprobe", "Bare"), payload: z.object({}) })
// @ts-expect-error — `ns.query` without a result
export const bareScoped = probe.query("Bare", { payload: z.object({}) })
// @ts-expect-error — and no descriptor can be written by hand without one
export const bareLiteral: QueryDescriptor = { kind: "query", name: qn("resultprobe", "Bare"), version: "1.0", payload: z.object({}) }

// A command may still answer nothing.
export const voidCommand = probe.command("Archive", { payload: z.object({ itemId: z.string() }) })
