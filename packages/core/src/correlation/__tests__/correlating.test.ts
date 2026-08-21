import { describe, expect, it } from "bun:test"
import { Phase, unitOfWork, type UnitOfWork } from "../../unit-of-work/unit-of-work.js"
import { correlating } from "../correlating.js"
import { correlatingHandler } from "../correlating-handler.js"
import { type Message, type Metadata } from "../../messaging/messages.js"
// The id-pair cargo, written out as any host writes it: the chain is inherited
// or seeded; the cause is the parent, unconditionally.
const correlationFrom = (parent: Message): Metadata => ({
  correlationId: String(parent.metadata.correlationId ?? parent.identifier),
  causationId: String(parent.identifier),
})

const message = (identifier: string, metadata: Record<string, unknown> = {}) =>
  ({
    kind: "command",
    identifier,
    name: { namespace: "test", name: "Cause" },
    payload: {},
    metadata,
    timestamp: 0,
  }) as never

describe("correlating — the unit of work that carries a map", () => {
  it("DELEGATES `phase` and `closed` instead of copying them", async () => {
    // The reason the wrapper is written member by member rather than as a
    // spread: these two are GETTERS over the lifecycle's own state. A spread
    // reads them once, at wrap time, and freezes a unit of work that is forever
    // un-started and never closed — so every guard downstream would pass.
    const uow = correlating(unitOfWork())

    expect(uow.phase).toBeNull()
    expect(uow.closed).toBe(false)

    let phaseDuring: number | null = null
    await uow.execute(async () => {
      phaseDuring = uow.phase
    })

    expect(phaseDuring).toBe(Phase.INVOCATION)
    expect(uow.closed).toBe(true)
  })

  it("delegates `replaying` in BOTH directions", async () => {
    // The processor writes it, the handler reads it. A one-way copy loses one.
    const inner = unitOfWork()
    const uow = correlating(inner)

    uow.replaying = true
    expect(inner.replaying).toBe(true)

    inner.replaying = false
    expect(uow.replaying).toBe(false)
  })

  it("shares the inner handle's buffers rather than making new ones", async () => {
    const inner = unitOfWork()
    const uow = correlating(inner)

    expect(uow.events).toBe(inner.events)
    expect(uow.stateCache).toBe(inner.stateCache)
  })

  it("registers lifecycle actions on the handle it wrapped", async () => {
    const log: string[] = []
    const uow = correlating(unitOfWork())

    await uow.execute(async () => {
      uow.onPrepareCommit(() => { log.push("prepare") })
      uow.onCommit(() => { log.push("commit") })
      uow.onAfterCommit(() => { log.push("after") })
    })

    expect(log).toEqual(["prepare", "commit", "after"])
  })

  it("hands the CORRELATED handle to the action, so a task has ONE identity", async () => {
    // This is the load-bearing one. Adapter transactions live in WeakMaps keyed
    // by unit of work, and the factory claims the object it RETURNS — the
    // correlated one. If `execute` handed the inner handle inward, the action
    // would be asking a table about an object nobody ever claimed, and every
    // handler write would silently fall outside its transaction.
    const uow = correlating(unitOfWork())
    let handed: UnitOfWork | undefined
    await uow.execute(async (handle) => {
      handed = handle
    })
    expect(handed).toBe(uow)
  })

  it("keeps an adapter's transaction reachable through the composed handle", async () => {
    // The same claim, end to end: decorate a correlating factory the way a host
    // does, and the transaction the adapter opened is found via the very handle
    // the handler receives.
    //
    // The adapter is written out INLINE here, in six lines, against nothing but
    // the public phase API — a WeakMap keyed by unit of work, a PRE_INVOCATION
    // hook that opens, a COMMIT hook that closes. That is the whole of what
    // core used to ship as `@kronos-ts/core/transaction`, and writing it here
    // is the proof the eviction was sound: a persistence package needs no
    // privileged access to the handle to key a transaction to it.
    const tx = { tag: "the-transaction" }
    const registry = new WeakMap<UnitOfWork, { tx?: typeof tx }>()
    const make = () => {
      const handle = correlating(unitOfWork())
      const slot: { tx?: typeof tx } = {}
      registry.set(handle, slot)
      handle.on(Phase.PRE_INVOCATION, async () => { slot.tx = tx })
      handle.on(Phase.COMMIT, async () => {})
      return handle
    }

    const uow = make()
    let seen: { tag: string } | undefined
    await uow.execute(async (handle) => {
      seen = registry.get(handle)?.tx
      // …and it is still a correlating handle on the way through.
      handle.attachCorrelationData({ traceparent: "t" })
    })

    expect(seen).toBe(tx)
    expect(uow.correlationData()).toEqual({ traceparent: "t" })
  })

  it("merges attached cargo, later keys winning", async () => {
    const uow = correlating(unitOfWork())
    expect(uow.correlationData()).toEqual({})

    uow.attachCorrelationData({ correlationId: "a", causationId: "x" })
    uow.attachCorrelationData({ causationId: "y", traceparent: "t" })

    expect(uow.correlationData()).toEqual({
      correlationId: "a",
      causationId: "y",
      traceparent: "t",
    })
  })
})

describe("correlatingHandler — the cargo function decides what jumps", () => {
  /** The smallest context the wrapper needs, and a `send` that records. */
  function contextFor(uow: ReturnType<typeof correlating>) {
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

  it("carries whatever `from` returns — the id pair is only the DEFAULT cargo", async () => {
    const uow = correlating(unitOfWork())
    const { sent, ctx } = contextFor(uow)

    const handler = correlatingHandler(
      async (_m: never, c: typeof ctx) => { await c.send(null, null) },
      // A host's own cargo: the id pair PLUS a fact it cares about.
      (m) => ({ ...correlationFrom(m), actor: String(m.metadata.actor ?? "") }),
    )

    await uow.execute(async () => {
      await handler(message("cmd-1", { correlationId: "corr-root", actor: "alice" }), ctx)
    })

    expect(sent[0]).toEqual({
      correlationId: "corr-root",
      causationId: "cmd-1",
      actor: "alice",
    })
  })

  it("lets the CALLER's metadata win over the overlay", async () => {
    const uow = correlating(unitOfWork())
    const sent: Array<Record<string, unknown>> = []
    const ctx = {
      unitOfWork: uow,
      send: async (_d: unknown, _p: unknown, metadata?: Record<string, unknown>) => {
        sent.push(metadata ?? {})
      },
    }

    const handler = correlatingHandler(
      async (_m: never, c: typeof ctx) => {
        await c.send(null, null, { causationId: "i-mean-it" })
      },
      correlationFrom,
    )

    await uow.execute(async () => {
      await handler(message("cmd-1", { correlationId: "corr-root" }), ctx)
    })

    expect(sent[0]).toEqual({ correlationId: "corr-root", causationId: "i-mean-it" })
  })

  it("reads the map PER CALL, so a mid-handling attach reaches the next birth", async () => {
    const uow = correlating(unitOfWork())
    const { sent, ctx } = contextFor(uow)

    const handler = correlatingHandler(async (_m: never, c: typeof ctx) => {
      await c.send(null, null)
      c.unitOfWork.attachCorrelationData({ traceparent: "t" })
      await c.send(null, null)
    }, correlationFrom)

    await uow.execute(async () => {
      await handler(message("cmd-1", { correlationId: "corr-root" }), ctx)
    })

    expect(sent[0]).not.toHaveProperty("traceparent")
    expect(sent[1]).toMatchObject({ traceparent: "t" })
  })

  it("wraps only the verbs a context actually has", async () => {
    // ONE wrapper, three handler kinds. A query context has neither `send` nor
    // `append`, and the wrapper must not invent them.
    const uow = correlating(unitOfWork())
    const asked: Array<Record<string, unknown>> = []
    const queryCtx = {
      unitOfWork: uow,
      query: async (_d: unknown, _p: unknown, metadata?: Record<string, unknown>) => {
        asked.push(metadata ?? {})
      },
    }

    const handler = correlatingHandler(async (_m: never, c: typeof queryCtx) => {
      expect("send" in c).toBe(false)
      expect("append" in c).toBe(false)
      await c.query(null, null)
    }, correlationFrom)

    await uow.execute(async () => {
      await handler(message("qry-1", { correlationId: "corr-root" }), queryCtx)
    })

    expect(asked[0]).toEqual({ correlationId: "corr-root", causationId: "qry-1" })
  })

  it("overlays each entry of append's BATCH form", async () => {
    const uow = correlating(unitOfWork())
    const appended: Array<unknown> = []
    const ctx = {
      unitOfWork: uow,
      append: (list: unknown) => { appended.push(list) },
    }

    const handler = correlatingHandler(async (_m: never, c: typeof ctx) => {
      c.append([
        ["A", { one: 1 }],
        ["B", { two: 2 }, { tenant: "acme" }],
      ])
    }, correlationFrom)

    await uow.execute(async () => {
      await handler(message("cmd-1", { correlationId: "corr-root" }), ctx)
    })

    expect(appended[0]).toEqual([
      ["A", { one: 1 }, { correlationId: "corr-root", causationId: "cmd-1" }],
      ["B", { two: 2 }, { correlationId: "corr-root", causationId: "cmd-1", tenant: "acme" }],
    ])
  })
})

describe("correlationFrom — the id-pair cargo", () => {
  it("seeds the chain at the parent when the parent has none", () => {
    expect(correlationFrom(message("cmd-1"))).toEqual({
      correlationId: "cmd-1",
      causationId: "cmd-1",
    })
  })

  it("inherits the chain and re-stamps causation at every hop", () => {
    // The asymmetry IS the rule: correlationId is inherited, causationId is the
    // parent's identifier — never the parent's own causationId, which would
    // name the GRANDparent and collapse the chain.
    expect(
      correlationFrom(message("cmd-2", { correlationId: "corr-root", causationId: "cmd-1" })),
    ).toEqual({ correlationId: "corr-root", causationId: "cmd-2" })
  })
})
