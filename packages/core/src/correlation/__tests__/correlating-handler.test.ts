import { describe, expect, it } from "bun:test"
import { unitOfWork } from "../../unit-of-work/unit-of-work.js"
import { correlatingHandler } from "../correlating-handler.js"
import { messageOrigin } from "../message-origin.js"
import { localQueryBus } from "../../query-handling/local-bus.js"
import { qn, type Message, type QueryMessage } from "../../messaging/messages.js"

const message = (identifier: string, metadata: Record<string, unknown> = {}) =>
  ({
    kind: "command",
    identifier,
    name: { namespace: "test", name: "Cause" },
    payload: {},
    metadata,
    timestamp: 0,
  }) as never

/** The smallest context the wrapper touches: a task and a `send` that records. */
function contextFor(uow = unitOfWork()) {
  const sent: Array<Record<string, unknown>> = []
  return {
    sent,
    ctx: {
      unitOfWork: uow,
      send: async (_d: unknown, _p: unknown, metadata?: Record<string, unknown>) => {
        sent.push(metadata ?? {})
      },
    },
  }
}

describe("correlatingHandler — the cargo function decides what jumps", () => {
  it("carries the standard cargo when given no cargo function", async () => {
    const { sent, ctx } = contextFor()
    const handler = correlatingHandler(async (_m: never, c: typeof ctx) => { await c.send(null, null) })

    await ctx.unitOfWork.execute(async () => {
      await handler(message("cmd-1", { correlationId: "corr-root" }), ctx)
    })

    expect(sent[0]).toEqual({ correlationId: "corr-root", causationId: "cmd-1" })
  })

  it("carries whatever `from` returns — the id pair is only the DEFAULT cargo", async () => {
    const { sent, ctx } = contextFor()

    const handler = correlatingHandler(
      async (_m: never, c: typeof ctx) => { await c.send(null, null) },
      // A host's own cargo: the standard pair PLUS a fact it cares about.
      (m) => ({ ...messageOrigin(m), actor: String(m.metadata.actor ?? "") }),
    )

    await ctx.unitOfWork.execute(async () => {
      await handler(message("cmd-1", { correlationId: "corr-root", actor: "alice" }), ctx)
    })

    expect(sent[0]).toEqual({ correlationId: "corr-root", causationId: "cmd-1", actor: "alice" })
  })

  it("lets the CALLER's metadata win over the overlay", async () => {
    const { sent, ctx } = contextFor()

    const handler = correlatingHandler(async (_m: never, c: typeof ctx) => {
      await c.send(null, null, { causationId: "i-mean-it" })
    })

    await ctx.unitOfWork.execute(async () => {
      await handler(message("cmd-1", { correlationId: "corr-root" }), ctx)
    })

    expect(sent[0]).toEqual({ correlationId: "corr-root", causationId: "i-mean-it" })
  })

  it("stores NOTHING on the unit of work", async () => {
    const uow = unitOfWork()
    const before = Object.keys(uow)
    const { ctx } = contextFor(uow)
    const handler = correlatingHandler(async (_m: never, c: typeof ctx) => { await c.send(null, null) })

    await uow.execute(async () => {
      await handler(message("cmd-1"), ctx)
    })

    expect(Object.keys(uow)).toEqual(before)
  })

  it("wraps only the verbs a context actually has", async () => {
    // ONE wrapper, three handler kinds. A query context has neither `send` nor
    // `append`, and the wrapper must not invent them.
    const asked: Array<Record<string, unknown>> = []
    const queryCtx = {
      unitOfWork: unitOfWork(),
      query: async (_d: unknown, _p: unknown, metadata?: Record<string, unknown>) => {
        asked.push(metadata ?? {})
      },
    }

    const handler = correlatingHandler(async (_m: never, c: typeof queryCtx) => {
      expect("send" in c).toBe(false)
      expect("append" in c).toBe(false)
      await c.query(null, null)
    })

    await queryCtx.unitOfWork.execute(async () => {
      await handler(message("qry-1", { correlationId: "corr-root" }), queryCtx)
    })

    expect(asked[0]).toEqual({ correlationId: "corr-root", causationId: "qry-1" })
  })

  it("overlays each entry of append's BATCH form", async () => {
    const appended: Array<unknown> = []
    const ctx = {
      unitOfWork: unitOfWork(),
      append: (list: unknown) => { appended.push(list) },
    }

    const handler = correlatingHandler(async (_m: never, c: typeof ctx) => {
      c.append([
        ["A", { one: 1 }],
        ["B", { two: 2 }, { tenant: "acme" }],
      ])
    })

    await ctx.unitOfWork.execute(async () => {
      await handler(message("cmd-1", { correlationId: "corr-root" }), ctx)
    })

    expect(appended[0]).toEqual([
      ["A", { one: 1 }, { correlationId: "corr-root", causationId: "cmd-1" }],
      ["B", { two: 2 }, { correlationId: "corr-root", causationId: "cmd-1", tenant: "acme" }],
    ])
  })
})

describe("correlatingHandler — the cargo belongs to the INVOCATION, not the task", () => {
  // These are the cases the task-wide map got wrong. A nested handling on the
  // same task must not overwrite its caller's cargo; concurrent nested
  // handlings must not see each other; a batch delivering several events on
  // one task must stamp each event's own cause.

  const READ = qn("probe", "Read")

  /** A query bus whose handler is itself correlated, answering on its own task. */
  function nestedQueryBus(answers: Array<Record<string, unknown>>) {
    const bus = localQueryBus(unitOfWork)
    const nested = correlatingHandler(async (_m: QueryMessage, c: { unitOfWork: unknown; send: (d: unknown, p: unknown, metadata?: Record<string, unknown>) => Promise<void> }) => {
      await c.send(null, null)
      return "answer"
    })
    bus.subscribe("probe.Read", (m, task) =>
      nested(m, {
        unitOfWork: task,
        send: async (_d: unknown, _p: unknown, metadata?: Record<string, unknown>) => { answers.push(metadata ?? {}) },
      }),
    )
    return bus
  }

  it("keeps the parent's cause after a nested query on the same task", async () => {
    const uow = unitOfWork()
    const records: Array<Record<string, unknown>> = []
    const nestedRecords: Array<Record<string, unknown>> = []
    const bus = nestedQueryBus(nestedRecords)
    const ctx = {
      unitOfWork: uow,
      send: async (_d: unknown, _p: unknown, metadata?: Record<string, unknown>) => { records.push(metadata ?? {}) },
      query: async (_d: unknown, _p: unknown, metadata?: Record<string, unknown>) =>
        bus.query({ kind: "query", identifier: "child-query", name: READ, payload: {}, metadata: metadata ?? {} }),
    }

    const parent = correlatingHandler(async (_m: Message, c: typeof ctx) => {
      await c.send(null, null)
      await c.query(null, null)
      await c.send(null, null)
    })

    await uow.execute(() => parent(message("parent"), ctx))

    expect(records[0]).toEqual({ correlationId: "parent", causationId: "parent" })
    expect(records[1]).toEqual({ correlationId: "parent", causationId: "parent" })
    // The nested handling saw its OWN cause and the inherited chain.
    expect(nestedRecords[0]).toEqual({ correlationId: "parent", causationId: "child-query" })
  })

  it("keeps concurrent nested handlings apart", async () => {
    const uow = unitOfWork()
    const nestedRecords: Array<Record<string, unknown>> = []
    const bus = nestedQueryBus(nestedRecords)
    const records: Array<Record<string, unknown>> = []
    const ctx = {
      unitOfWork: uow,
      send: async (_d: unknown, _p: unknown, metadata?: Record<string, unknown>) => { records.push(metadata ?? {}) },
      query: async (id: string, _p: unknown, metadata?: Record<string, unknown>) =>
        bus.query({ kind: "query", identifier: id, name: READ, payload: {}, metadata: metadata ?? {} }),
    }

    const parent = correlatingHandler(async (_m: Message, c: typeof ctx) => {
      await Promise.all([c.query("q-a", null), c.query("q-b", null)])
      await c.send(null, null)
    })

    await uow.execute(() => parent(message("parent"), ctx))

    expect(nestedRecords.map((r) => r.causationId).sort()).toEqual(["q-a", "q-b"])
    expect(records[0]).toEqual({ correlationId: "parent", causationId: "parent" })
  })

  it("stamps each invocation's own cause when a batch shares one task", async () => {
    const uow = unitOfWork()
    const records: Array<Record<string, unknown>> = []
    const ctx = {
      unitOfWork: uow,
      send: async (_d: unknown, _p: unknown, metadata?: Record<string, unknown>) => { records.push(metadata ?? {}) },
    }
    const handler = correlatingHandler(async (_m: Message, c: typeof ctx) => { await c.send(null, null) })

    // A processor delivers a batch sequentially on ONE unit of work.
    await uow.execute(async () => {
      for (const id of ["evt-1", "evt-2", "evt-3"]) {
        await handler(message(id, { correlationId: "corr-root" }), ctx)
      }
    })

    expect(records.map((r) => r.causationId)).toEqual(["evt-1", "evt-2", "evt-3"])
    expect(new Set(records.map((r) => r.correlationId))).toEqual(new Set(["corr-root"]))
  })
})

describe("messageOrigin — the standard cargo", () => {
  it("seeds the chain at the parent when the parent has none", () => {
    expect(messageOrigin(message("cmd-1"))).toEqual({ correlationId: "cmd-1", causationId: "cmd-1" })
  })

  it("inherits the chain and re-stamps causation at every hop", () => {
    // The asymmetry IS the rule: correlationId is inherited, causationId is the
    // parent's identifier — never the parent's own causationId, which would
    // name the GRANDparent and collapse the chain.
    expect(messageOrigin(message("cmd-2", { correlationId: "corr-root", causationId: "cmd-1" }))).toEqual({
      correlationId: "corr-root",
      causationId: "cmd-2",
    })
  })

  it("carries a trace context when the parent has one, and nothing when it does not", () => {
    const tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    expect(messageOrigin(message("cmd-1", { traceparent: tp }))).toMatchObject({ traceparent: tp })
    expect(messageOrigin(message("cmd-1"))).not.toHaveProperty("traceparent")
    expect(messageOrigin(message("cmd-1", { traceparent: 42 }))).not.toHaveProperty("traceparent")
  })
})
