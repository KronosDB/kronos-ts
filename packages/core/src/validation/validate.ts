import { qualifiedNameToString, type MessageDescriptor } from "../messaging/messages.js"
import type { InferOutput, StandardSchemaResult } from "../messaging/standard-schema.js"

/**
 * VALIDATE A PAYLOAD AGAINST THE MESSAGE TYPE IT CLAIMS TO BE.
 *
 * The primitive, usable anywhere a descriptor is in hand — which is everywhere
 * that validation is a question at all. A descriptor already carries its payload
 * schema, and every site that would validate is handed the descriptor as an
 * ARGUMENT: the edge verbs (`send(bus, descriptor, payload)`), the birth verbs
 * (`ctx.append(descriptor, payload)`), the entry a handler was declared with.
 * That is why there is no registry here and nothing to register: a registry
 * answers "which schema goes with this type name", and nobody has to ask.
 *
 * ```ts
 * send(commandBus, OpenAccount, validate(OpenAccount, req.body), { actor })
 * ```
 *
 * WHAT COMES BACK IS THE PARSED VALUE, not a boolean and not the input. Standard
 * validation is a parse: a schema may coerce, default and transform, and the
 * value it produces is the one the rest of the system should see. Hand the
 * return value onward — dropping it keeps the check and throws away half of it.
 *
 * SYNC IN, SYNC OUT. `~standard.validate` may answer a promise (some libraries
 * have async refinements) and may answer directly. This returns whichever it
 * got: a synchronous schema gives the caller the value with no `await` in sight,
 * and an async one gives a promise the caller in an async position awaits. The
 * union is the honest type — pretending everything is a promise would tax every
 * sync site, and pretending nothing is would silently hand a thenable onward as
 * a payload.
 *
 * Failure THROWS, naming the message type and joining what the schema said. A
 * function returning `Result<T, E>` here would put a second error protocol
 * beside the one the whole framework already has, and an invalid payload at a
 * boundary is not a value the caller was going to branch on.
 */
export function validate<D extends MessageDescriptor>(
  descriptor: D,
  payload: unknown,
): InferOutput<D["payload"]> | Promise<InferOutput<D["payload"]>> {
  const outcome = descriptor.payload["~standard"].validate(payload)
  return outcome instanceof Promise
    ? outcome.then((settled: StandardSchemaResult<unknown>) => accepted(descriptor, settled))
    : accepted(descriptor, outcome as StandardSchemaResult<unknown>)
}

/**
 * The same reading, for a site that cannot await — the SYNCHRONOUS birth verbs.
 *
 * INTERNAL: `validation/` exports two names, and this is not one of them.
 * `ctx.append` returns `void` and `ctx.schedule` builds its event message in the
 * caller's turn, so neither has anywhere to put an await. A schema that
 * validates asynchronously there throws with the message type in the message —
 * the same rule (and the same reason) the serializer had, one boundary over.
 */
export function validatedNow(
  descriptor: MessageDescriptor,
  payload: unknown,
  verb: string,
): unknown {
  const outcome = descriptor.payload["~standard"].validate(payload)
  if (outcome instanceof Promise) {
    throw new Error(
      `validatingHandler: the payload schema for "${qualifiedNameToString(descriptor.name)}" ` +
        `validates asynchronously, and \`ctx.${verb}\` gives birth in the caller's turn — ` +
        "there is nothing there to await in. Declare a schema whose validation is " +
        "synchronous, or give birth through `ctx.send` / `ctx.query`, which can await.",
    )
  }
  return accepted(descriptor, outcome as StandardSchemaResult<unknown>)
}

/**
 * What a settled standard result means: the parsed value, or an error naming the
 * message type and everything the schema found wrong with it.
 *
 * `issues` is the discriminant — the standard says a success carries no issues —
 * so this is the union narrowing, written once for both readings above.
 */
function accepted<D extends MessageDescriptor>(
  descriptor: D,
  outcome: StandardSchemaResult<unknown>,
): InferOutput<D["payload"]> {
  if (outcome.issues) {
    throw new Error(
      `validate: "${qualifiedNameToString(descriptor.name)}" failed validation — ` +
        outcome.issues.map((issue) => issue.message).join("; "),
    )
  }
  return outcome.value as InferOutput<D["payload"]>
}
