/**
 * The TYPE test for WHAT A COMMAND DECLARES. Listed in the root
 * `tsconfig.json` `files` array, so `tsc --noEmit` judges it; a
 * `@ts-expect-error` that stops erroring turns that gate red.
 *
 * What it pins: a command descriptor has no `routingKey`, and no way of
 * declaring a command accepts one. A routing key travels on the message, as
 * `metadata.processingInstructions.routingKey`, which the transports read.
 */
import { z } from "zod"
import { command, qn, withNamespace, type CommandDescriptor } from "../messages.js"
import type { CommandBus } from "../../command-handling/bus.js"
import { send } from "../../command-handling/send.js"

const payload = z.object({ courseId: z.string() })
const result = z.object({ courseId: z.string() })
const ns = withNamespace("routingprobe")

// @ts-expect-error — `command` takes no `routingKey`
export const declared = command({ name: qn("routingprobe", "Declared"), payload, routingKey: "courseId" })
// @ts-expect-error — nor with a result
export const declaredWithResult = command({ name: qn("routingprobe", "WithResult"), payload, result, routingKey: "courseId" })
// @ts-expect-error — nor through a namespace
export const scoped = ns.command("Scoped", { payload, routingKey: "courseId" })
// @ts-expect-error — nor through a namespace with a result
export const scopedWithResult = ns.command("ScopedWithResult", { payload, result, routingKey: "courseId" })

const CreateCourse = command({ name: qn("routingprobe", "CreateCourse"), payload })

// @ts-expect-error — the descriptor has no `routingKey` to read
export const read = CreateCourse.routingKey
// @ts-expect-error — nor does the type
export type Read = CommandDescriptor["routingKey"]

// The routing key of one command is given where the command is sent.
export const keyed = (commandBus: CommandBus) =>
  send(commandBus, CreateCourse, { courseId: "cs-101" }, { processingInstructions: { routingKey: "cs-101" } })
