/**
 * The TYPE test for wrapper ORDER. Listed in the root `tsconfig.json` `files`
 * array, so `bunx tsc --noEmit` judges it; a `@ts-expect-error` that stops
 * erroring turns that gate red.
 *
 * What it pins: a wrapper that reads the message refuses to be handed a chain
 * with a message-stamping wrapper INSIDE it — through a `pipe` helper of the
 * kind hosts use, not only through direct nesting — and the refusal is a
 * sentence, not a structural mismatch. And a chain in the right order keeps
 * the handler's message, context and result types intact.
 */
import type { Message } from "../../messaging/messages.js"
import { describe, type Described } from "../describe.js"
import { correlatingHandler } from "../../correlation/correlating-handler.js"
import { loggingHandler, type LogCapability } from "../../logging/logging-handler.js"
import type { Logger } from "../../logging/logger.js"

declare function pipe<A, B>(a: A, ab: (a: A) => B): B
declare function pipe<A, B, C>(a: A, ab: (a: A) => B, bc: (b: B) => C): C
declare function pipe<A, B, C, D>(a: A, ab: (a: A) => B, bc: (b: B) => C, cd: (c: C) => D): D

/** A stand-in for a tracing wrapper: supplies `trace`, stamps the message. Described exactly as one would be. */
function tracingHandler<H extends (message: any, context: any) => any>(
  next: H,
): ((message: Parameters<H>[0], context: Omit<Parameters<H>[1], "trace">) => ReturnType<H>) &
  Described<{ readonly name: "tracingHandler"; readonly supplies: readonly ["trace"]; readonly stamps: readonly ["message.metadata"]; readonly next: H }> {
  const wrapped = (message: Parameters<H>[0], context: Omit<Parameters<H>[1], "trace">): ReturnType<H> => next(message, context)
  return describe(wrapped, { name: "tracingHandler", supplies: ["trace"], stamps: ["message.metadata"], next } as const)
}

type Ctx = { readonly unitOfWork: object } & LogCapability
declare const h: (message: Message & { payload: { id: string } }, context: Ctx) => Promise<{ ok: true }>
declare const logger: Logger

// ---------------------------------------------------------------------------
// (a) RIGHT ORDER — stamping outermost. Compiles, and the handler's types come
// through: message, context (minus what was supplied) and result.
// ---------------------------------------------------------------------------

export const right = pipe(
  h,
  (x) => loggingHandler(x, logger),
  (x) => correlatingHandler(x),
  (x) => tracingHandler(x),
)

export const typesSurvive: (
  message: Message & { payload: { id: string } },
  context: { readonly unitOfWork: object },
) => Promise<{ ok: true }> = right

// ---------------------------------------------------------------------------
// (b) WRONG ORDER — stamping inside the reader. Refused with a sentence.
// ---------------------------------------------------------------------------

export const wrongForCorrelation = pipe(
  h,
  (x) => loggingHandler(x, logger),
  (x) => tracingHandler(x),
  // @ts-expect-error — "a wrapper that stamps message.metadata is inside correlatingHandler: move it outside…"
  (x) => correlatingHandler(x),
)

export const wrongForLogging = pipe(
  h,
  (x) => tracingHandler(x),
  // @ts-expect-error — "a wrapper that stamps message.metadata is inside loggingHandler: move it outside…"
  (x) => loggingHandler(x, logger),
)

// The rule looks THROUGH undescribed-but-typed links: the stamp two layers in
// is still found.
export const wrongTwoLayersDeep = pipe(
  h,
  (x) => tracingHandler(x),
  (x) => loggingHandler(x as never, logger) as unknown as typeof x,
  // @ts-expect-error — the stamp is inside, two layers down
  (x) => correlatingHandler(x),
)

// ---------------------------------------------------------------------------
// (c) NO STAMPER AT ALL — a host that never traces wires the same handlers
// unchanged.
// ---------------------------------------------------------------------------

export const untraced = pipe(
  h,
  (x) => loggingHandler(x, logger),
  (x) => correlatingHandler(x),
)
