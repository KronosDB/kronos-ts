/**
 * `validatingHandler(next, descriptor)` — the mechanism.
 *
 * Two questions per invocation, and they are different questions: what came IN
 * (checked against the ENTRY's descriptor) and what this handling gives birth to
 * (each checked against the descriptor IT was called with). The write side is
 * the one that used to live in the serializer, and it is the one that matters
 * most: THE LOG NEVER ACCEPTS A LIE.
 */
import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, command, event, queryDescriptor } from "../../messaging/messages.js"
import type {
  CommandMessage,
  EventMessage,
  Message,
  Metadata,
  StandardSchemaV1,
} from "../../index.js"
import {
  correlating,
  correlatingHandler,
  commandHandler,
  inMemoryEventStore,
  kronos,
  localCommandBus,
  localQueryBus,
  send,
  unitOfWork,
} from "../../index.js"
import { validatingHandler } from "../validating-handler.js"

// ---------------------------------------------------------------------------
// The vocabulary these tests are written in.
// ---------------------------------------------------------------------------

const RecordCharge = command({
  name: qn("billing", "RecordCharge"),
  payload: z.object({ accountId: z.string(), amount: z.number() }),
})

const Charged = event({
  name: qn("billing", "Charged"),
  payload: z.object({ accountId: z.string(), amount: z.number(), currency: z.string().default("EUR") }),
  tags: { accountId: (p) => p.accountId },
})

const Settled = event({
  name: qn("billing", "Settled"),
  payload: z.object({ accountId: z.string() }),
  tags: { accountId: (p) => p.accountId },
})

const Notify = command({
  name: qn("billing", "Notify"),
  payload: z.object({ accountId: z.string(), channel: z.string().default("email") }),
})

const GetBalance = queryDescriptor({
  name: qn("billing", "GetBalance"),
  payload: z.object({ accountId: z.string() }),
})

/** A schema that answers a promise — some libraries' refinements do. */
const slowly = <T>(check: (value: unknown) => boolean): StandardSchemaV1<T, T> => ({
  "~standard": {
    version: 1,
    vendor: "slow",
    validate: async (value: unknown) =>
      check(value) ? { value: value as T } : { issues: [{ message: "the slow schema said no" }] },
  },
})

const SlowCommand = command({
  name: qn("billing", "SlowCommand"),
  payload: slowly<{ accountId: string }>((v) => typeof (v as any)?.accountId === "string"),
})

const SlowEvent = event({
  name: qn("billing", "SlowEvent"),
  payload: slowly<{ accountId: string }>(() => true),
})

const commandMessage = <P>(payload: P, name = RecordCharge.name): CommandMessage<P> => ({
  kind: "command",
  identifier: "cmd-1",
  name,
  payload,
  metadata: {},
  timestamp: 0,
})

/**
 * A context as a unit test writes one: a plain record of the verbs, recording
 * what each was given. No fixture — a handler is a function, and a `ctx` is an
 * object literal.
 */
function recordingContext() {
  const calls = {
    sent: [] as Array<[unknown, unknown, Metadata | undefined]>,
    queried: [] as Array<[unknown, unknown, Metadata | undefined]>,
    appended: [] as Array<[unknown, unknown, Metadata | undefined]>,
    scheduled: [] as Array<[unknown, unknown, unknown, Metadata | undefined]>,
    scheduledAfter: [] as Array<[unknown, unknown, unknown, Metadata | undefined]>,
  }
  const ctx = {
    send: async (d: unknown, p: unknown, m?: Metadata) => {
      calls.sent.push([d, p, m])
      return "sent"
    },
    query: async (d: unknown, p: unknown, m?: Metadata) => {
      calls.queried.push([d, p, m])
      return "answered"
    },
    append: (
      d: unknown | ReadonlyArray<readonly [unknown, unknown, Metadata?]>,
      p?: unknown,
      m?: Metadata,
    ): void => {
      if (Array.isArray(d)) {
        for (const [entryDescriptor, entryPayload, entryMetadata] of d) {
          calls.appended.push([entryDescriptor, entryPayload, entryMetadata])
        }
        return
      }
      calls.appended.push([d, p, m])
    },
    schedule: async (d: unknown, p: unknown, at: unknown, m?: Metadata) => {
      calls.scheduled.push([d, p, at, m])
      return "token"
    },
    scheduleAfter: async (d: unknown, p: unknown, delayMs: unknown, m?: Metadata) => {
      calls.scheduledAfter.push([d, p, delayMs, m])
      return "token"
    },
  }
  return { ctx, calls }
}

// ---------------------------------------------------------------------------
// INBOUND — what came in is what the entry says it is.
// ---------------------------------------------------------------------------

describe("validatingHandler — inbound", () => {
  it("hands `next` a message whose payload is the PARSED value", async () => {
    let seen: CommandMessage<unknown> | undefined
    const handler = validatingHandler(async (message: CommandMessage<unknown>) => {
      seen = message
      return "ok"
    }, RecordCharge)

    const message = commandMessage({ accountId: "a-1", amount: 10 })
    expect(await handler(message, {})).toBe("ok")

    expect(seen?.payload).toEqual({ accountId: "a-1", amount: 10 })
    // Everything else about the message is the message it was handed.
    expect(seen?.identifier).toBe(message.identifier)
    expect(seen?.metadata).toBe(message.metadata)
  })

  it("refuses a message that is not what the entry says it is — `next` never runs", async () => {
    let ran = false
    const handler = validatingHandler(async () => {
      ran = true
    }, RecordCharge)

    await expect(handler(commandMessage({ accountId: "a-1" }), {})).rejects.toThrow(
      /billing\.RecordCharge.*failed validation/s,
    )
    expect(ran).toBe(false)
  })

  it("AWAITS an asynchronous schema inbound — the wrapper is async for exactly this", async () => {
    const seen: unknown[] = []
    const handler = validatingHandler(async (message: CommandMessage<unknown>) => {
      seen.push(message.payload)
    }, SlowCommand)

    await handler(commandMessage({ accountId: "a-1" }, SlowCommand.name), {})
    expect(seen).toEqual([{ accountId: "a-1" }])

    await expect(
      handler(commandMessage({ accountId: 7 }, SlowCommand.name), {}),
    ).rejects.toThrow(/the slow schema said no/)
  })
})

// ---------------------------------------------------------------------------
// OUTBOUND — every birth, against the descriptor IT was called with.
// ---------------------------------------------------------------------------

describe("validatingHandler — outbound births", () => {
  it("validates ctx.append against the APPENDED event's descriptor, not the entry's", async () => {
    const { ctx, calls } = recordingContext()
    const handler = validatingHandler(async (_m: Message, c: typeof ctx) => {
      c.append(Charged, { accountId: "a-1", amount: 10 } as never)
    }, RecordCharge)

    await handler(commandMessage({ accountId: "a-1", amount: 10 }), ctx)

    // The parsed value is what was appended — `currency` defaulted on the way
    // through, exactly as the event's own schema says it should.
    expect(calls.appended).toEqual([[Charged, { accountId: "a-1", amount: 10, currency: "EUR" }, undefined]])
  })

  it("refuses an invalid ctx.append — and the verb throws where the lie was told", async () => {
    const { ctx, calls } = recordingContext()
    const handler = validatingHandler(async (_m: Message, c: typeof ctx) => {
      c.append(Charged, { accountId: "a-1" } as never)
    }, RecordCharge)

    await expect(handler(commandMessage({ accountId: "a-1", amount: 10 }), ctx)).rejects.toThrow(
      /billing\.Charged.*failed validation/s,
    )
    expect(calls.appended).toEqual([])
  })

  it("validates the BATCH form per tuple, because each tuple names its own descriptor", async () => {
    const { ctx, calls } = recordingContext()
    const handler = validatingHandler(async (_m: Message, c: typeof ctx) => {
      c.append([
        [Charged, { accountId: "a-1", amount: 10 }],
        [Settled, { accountId: "a-1" }, { note: "batched" }],
      ] as never)
    }, RecordCharge)

    await handler(commandMessage({ accountId: "a-1", amount: 10 }), ctx)

    expect(calls.appended).toEqual([
      [Charged, { accountId: "a-1", amount: 10, currency: "EUR" }, undefined],
      [Settled, { accountId: "a-1" }, { note: "batched" }],
    ])
  })

  it("refuses the tuple that lies, in a batch of tuples that do not", async () => {
    const { ctx, calls } = recordingContext()
    const handler = validatingHandler(async (_m: Message, c: typeof ctx) => {
      c.append([
        [Charged, { accountId: "a-1", amount: 10 }],
        [Settled, {}],
      ] as never)
    }, RecordCharge)

    await expect(handler(commandMessage({ accountId: "a-1", amount: 10 }), ctx)).rejects.toThrow(
      /billing\.Settled/,
    )
    // The whole call is refused: nothing reaches the buffer, so the batch stays
    // the atomic thing it is.
    expect(calls.appended).toEqual([])
  })

  it("validates ctx.send and ctx.query, replacing each payload with the parse", async () => {
    const { ctx, calls } = recordingContext()
    const handler = validatingHandler(async (_m: Message, c: typeof ctx) => {
      await c.send(Notify, { accountId: "a-1" } as never, { actor: "u-1" })
      await c.query(GetBalance, { accountId: "a-1" }, undefined)
    }, RecordCharge)

    await handler(commandMessage({ accountId: "a-1", amount: 10 }), ctx)

    expect(calls.sent).toEqual([[Notify, { accountId: "a-1", channel: "email" }, { actor: "u-1" }]])
    expect(calls.queried).toEqual([[GetBalance, { accountId: "a-1" }, undefined]])
  })

  it("AWAITS an async schema on ctx.send — the verb already answers a promise", async () => {
    const { ctx, calls } = recordingContext()
    const handler = validatingHandler(async (_m: Message, c: typeof ctx) => {
      await c.send(SlowCommand, { accountId: "a-1" } as never)
    }, RecordCharge)

    await handler(commandMessage({ accountId: "a-1", amount: 10 }), ctx)
    expect(calls.sent).toEqual([[SlowCommand, { accountId: "a-1" }, undefined]])
  })

  it("REFUSES an async schema on ctx.append, and says why — the same rule the serializer had", async () => {
    const { ctx, calls } = recordingContext()
    const handler = validatingHandler(async (_m: Message, c: typeof ctx) => {
      c.append(SlowEvent, { accountId: "a-1" } as never)
    }, RecordCharge)

    await expect(handler(commandMessage({ accountId: "a-1", amount: 10 }), ctx)).rejects.toThrow(
      /billing\.SlowEvent.*validates asynchronously.*`ctx\.append`/s,
    )
    expect(calls.appended).toEqual([])
  })

  it("validates ctx.schedule and ctx.scheduleAfter, and refuses an async schema there too", async () => {
    const { ctx, calls } = recordingContext()
    const at = new Date(0)

    const scheduling = validatingHandler(async (_m: Message, c: typeof ctx) => {
      await c.schedule(Charged, { accountId: "a-1", amount: 10 } as never, at)
      await c.scheduleAfter(Charged, { accountId: "a-1", amount: 20 } as never, 1000)
    }, RecordCharge)
    await scheduling(commandMessage({ accountId: "a-1", amount: 10 }), ctx)

    expect(calls.scheduled).toEqual([
      [Charged, { accountId: "a-1", amount: 10, currency: "EUR" }, at, undefined],
    ])
    expect(calls.scheduledAfter).toEqual([
      [Charged, { accountId: "a-1", amount: 20, currency: "EUR" }, 1000, undefined],
    ])

    const slow = validatingHandler(async (_m: Message, c: typeof ctx) => {
      await c.schedule(SlowEvent, { accountId: "a-1" } as never, at)
    }, RecordCharge)
    await expect(slow(commandMessage({ accountId: "a-1", amount: 10 }), ctx)).rejects.toThrow(
      /validates asynchronously.*`ctx\.schedule`/s,
    )
  })

  it("wraps only the verbs the context HAS — one wrapper, three kinds", async () => {
    // A query context has neither `send` nor `append`. Nothing is conjured, and
    // nothing is required.
    const queryish = { query: async () => "answered", unitOfWork: "uow" }
    let seen: Record<string, unknown> | undefined

    await validatingHandler(async (_m: Message, c: typeof queryish) => {
      seen = c as unknown as Record<string, unknown>
    }, GetBalance)(commandMessage({ accountId: "a-1" }, GetBalance.name), queryish)

    expect(Object.keys(seen ?? {}).sort()).toEqual(["query", "unitOfWork"])
    expect(seen?.unitOfWork).toBe("uow")
  })
})

// ---------------------------------------------------------------------------
// THE LOG NEVER ACCEPTS A LIE — the claim the serializer's write-side test made,
// now pinned where the write actually happens.
// ---------------------------------------------------------------------------

describe("validatingHandler — the log never accepts a lie", () => {
  it("a wrapped ctx.append refuses an invalid payload before the store sees it", async () => {
    const eventStore = inMemoryEventStore()
    const stored: EventMessage[] = []
    eventStore.subscribe(async (events) => {
      stored.push(...events)
    })

    const honest = commandHandler(RecordCharge, async ({ payload }, ctx) => {
      ctx.append(Charged, { accountId: payload.accountId, amount: payload.amount })
    })
    const buggy = commandHandler(Notify, async (_message, ctx) => {
      // The lie: a handler appending something that is not the event it names.
      ctx.append(Charged, { accountId: "a-2" } as never)
    })

    const commandBus = localCommandBus(unitOfWork)
    const queryBus = localQueryBus(unitOfWork)

    // THE COMPOSITION SITE: descriptor and handler sit together on the entry, so
    // the wrapper needs no arrow reaching anywhere else.
    const app = kronos({
      commandHandlers: [honest, buggy]
        .map((h) => ({ ...h, handler: validatingHandler(h.handler, h.descriptor) }))
        .map((h) => ({ ...h, commandBus, queryBus, eventStore })),
    })

    try {
      await send(commandBus, RecordCharge, { accountId: "a-1", amount: 10 })
      expect(stored).toHaveLength(1)
      expect(stored[0]?.payload).toEqual({ accountId: "a-1", amount: 10, currency: "EUR" })

      await expect(send(commandBus, Notify, { accountId: "a-2" })).rejects.toThrow(
        /billing\.Charged.*failed validation/s,
      )
      // A buggy handler's garbage fails at the moment of the lie, not later in a
      // replay that did nothing wrong.
      expect(stored).toHaveLength(1)
    } finally {
      await app.stop()
    }
  })
})

// ---------------------------------------------------------------------------
// COMPOSITION — the wrappers are functions, so they nest.
// ---------------------------------------------------------------------------

describe("validatingHandler — composes with the other mechanisms", () => {
  const correlationFrom = (parent: Message): Metadata => ({
    correlationId: String(parent.metadata.correlationId ?? parent.identifier),
    causationId: String(parent.identifier),
  })

  it("inbound validation runs, correlation still attaches, and a birth overlays BOTH", async () => {
    const { ctx: base, calls } = recordingContext()
    const ctx = { ...base, unitOfWork: correlating(unitOfWork()) }

    const handler = validatingHandler(
      correlatingHandler(async (message: CommandMessage<{ accountId: string }>, c: typeof ctx) => {
        c.append(Charged, { accountId: message.payload.accountId, amount: 10 } as never)
      }, correlationFrom),
      RecordCharge,
    )

    await handler(
      { ...commandMessage({ accountId: "a-1", amount: 10 }), metadata: { correlationId: "corr-1" } },
      ctx,
    )

    const [[descriptor, payload, metadata]] = calls.appended
    expect(descriptor).toBe(Charged)
    // Validation's parse…
    expect(payload).toEqual({ accountId: "a-1", amount: 10, currency: "EUR" })
    // …and correlation's cargo, on the same birth.
    expect(metadata).toEqual({ correlationId: "corr-1", causationId: "cmd-1" })
  })

  it("still refuses an invalid inbound message when a correlating handler is underneath", async () => {
    const { ctx: base } = recordingContext()
    const ctx = { ...base, unitOfWork: correlating(unitOfWork()) }
    let ran = false

    const handler = validatingHandler(
      correlatingHandler(async () => {
        ran = true
      }, correlationFrom),
      RecordCharge,
    )

    await expect(handler(commandMessage({ accountId: "a-1" }), ctx)).rejects.toThrow(
      /billing\.RecordCharge/,
    )
    expect(ran).toBe(false)
  })
})
