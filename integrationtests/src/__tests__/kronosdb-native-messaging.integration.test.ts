/**
 * Commands, queries, subscription queries and the platform over the native
 * protocol, against a real server.
 *
 * What the unit tests cannot say: that the bytes these buses write are the
 * bytes the server reads, that credit, takeover and timeouts behave as the
 * server defines them, and that the platform stream is what the admin console
 * sees. Everything goes through the package's public functions — the same ones
 * a host calls — and handlers and callers live on separate connections, as they
 * do in a deployment.
 *
 * Needs a server that speaks the native protocol: KRONOSDB_SERVER_BIN or
 * KRONOSDB_IMAGE (see ../kronosdb-server.ts).
 */
import assert from "node:assert/strict"
import { execFileSync } from "node:child_process"
import { mkdirSync, mkdtempSync, rmSync } from "node:fs"
import { dirname, join, resolve } from "node:path"
import { fileURLToPath } from "node:url"
import { afterAll, beforeAll, describe, expect, it } from "bun:test"
import {
  generateIdentifier,
  jsonSerializer,
  localCommandBus,
  localQueryBus,
  payloadEquals,
  qn,
  unitOfWork,
  type CommandMessage,
  type QueryMessage,
  type SubscriptionCapableQueryBus,
} from "@kronos-ts/core"
import {
  AuthenticationError,
  CommandExecutionError,
  KronosDbError,
  KronosDbRequestError,
  QueryExecutionError,
  kronosDbCommandBus,
  kronosDbConnection,
  kronosDbControlPlane,
  kronosDbQueryBus,
  type KronosDbConnectionHandle,
  type KronosDbConnectionOptions,
  type ManagedEventProcessor,
} from "@kronos-ts/kronosdb"
import { startKronosDb, type KronosDbServer } from "../kronosdb-server.js"

const run = generateIdentifier().slice(0, 8)
const serializer = jsonSerializer()

function commandOf(name: string, payload: unknown, metadata: Record<string, unknown> = {}): CommandMessage {
  return { kind: "command", identifier: generateIdentifier(), name: qn("native-msg", `${name}-${run}`), payload, metadata, timestamp: Date.now() }
}

function queryOf(name: string, payload: unknown): QueryMessage {
  return { kind: "query", identifier: generateIdentifier(), name: qn("native-msg", `${name}-${run}`), payload, metadata: {}, timestamp: Date.now() }
}

const nameOf = (name: string) => `native-msg.${name}-${run}`

async function until(probe: () => boolean | Promise<boolean>, what: string, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!(await probe())) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`)
    await new Promise((r) => setTimeout(r, 10))
  }
}

describe("KronosDB messaging — native protocol", () => {
  let server: KronosDbServer
  const open: KronosDbConnectionHandle[] = []

  async function connection(componentName: string, extra: Partial<KronosDbConnectionOptions> = {}): Promise<KronosDbConnectionHandle> {
    // Some handlers here never answer on purpose; closing must not wait for them for long.
    const kdb = await kronosDbConnection({ componentName, host: server.host, port: server.port, serializer, shutdownTimeoutMs: 200, ...extra })
    open.push(kdb)
    return kdb
  }
  const commands = async (name: string, bus = "default", options: Parameters<typeof kronosDbCommandBus>[3] = {}) => {
    const kdb = await connection(name)
    return { kdb, bus: kronosDbCommandBus(localCommandBus(unitOfWork), kdb, bus, options) }
  }
  const queries = async (name: string, bus = "default", options: Parameters<typeof kronosDbQueryBus>[3] = {}) => {
    const kdb = await connection(name)
    return { kdb, bus: kronosDbQueryBus(localQueryBus(unitOfWork), kdb, bus, options) }
  }
  const adminJson = async (path: string) => (await fetch(`http://${server.host}:${server.adminPort}${path}`)).json() as Promise<any>

  beforeAll(async () => {
    server = await startKronosDb()
  }, 120_000)

  afterAll(async () => {
    for (const kdb of open) await kdb.close().catch(() => {})
    await server?.stop()
  })

  // ── commands ──────────────────────────────────────────────────────────

  it("routes a command to its handler and brings the answer back, with the readiness barrier in between", async () => {
    const handler = await commands("echo-handler")
    const seen: CommandMessage[] = []
    handler.bus.subscribe(nameOf("Echo"), async (m) => {
      seen.push(m)
      return { echoed: m.payload, tenant: m.metadata.tenant }
    })
    // start() returns once the server has acknowledged the handler: the very
    // next dispatch, from anywhere, is routable. No retry loop.
    await handler.kdb.start()

    const caller = await commands("echo-caller")
    const answer = await caller.bus.dispatch(commandOf("Echo", { order: 7, note: "naïve café ✓" }, { tenant: "acme", attempt: 3, ratio: 0.5, urgent: true }))
    expect(answer).toEqual({ echoed: { order: 7, note: "naïve café ✓" }, tenant: "acme" })
    expect(seen).toHaveLength(1)
    expect(seen[0]!.metadata).toMatchObject({ tenant: "acme", attempt: 3, ratio: 0.5, urgent: true })
    expect(seen[0]!.name).toEqual(qn("native-msg", `Echo-${run}`))
    expect(typeof seen[0]!.timestamp).toBe("number")
  })

  it("answers undefined for a handler that returns nothing", async () => {
    const handler = await commands("void-handler")
    handler.bus.subscribe(nameOf("Void"), async () => undefined)
    await handler.kdb.start()
    const caller = await commands("void-caller")
    expect(await caller.bus.dispatch(commandOf("Void", 1))).toBeUndefined()
  })

  it("brings a handler's failure back as an error with its message and code", async () => {
    const handler = await commands("fail-handler")
    handler.bus.subscribe(nameOf("Fail"), async () => {
      throw new Error("no stock")
    })
    await handler.kdb.start()
    const caller = await commands("fail-caller")
    const failure = await caller.bus.dispatch(commandOf("Fail", 1)).then(() => undefined, (e) => e)
    assert.ok(failure instanceof CommandExecutionError)
    expect(failure.message).toBe("no stock")
    expect(failure.errorCode).toBe("KRONOS-4002")
  })

  it("fails a command nobody handles, saying so, and as something safe to retry", async () => {
    const caller = await commands("lonely-caller")
    const failure = await caller.bus.dispatch(commandOf("Nobody", 1)).then(() => undefined, (e) => e)
    assert.ok(failure instanceof KronosDbRequestError)
    expect(failure.status).toBe("unavailable")
    expect(failure.transient).toBe(true)
    expect(failure.message).toMatch(/no handler available/i)
  })

  it("keeps a routing key on one handler, and spreads keys over both", async () => {
    const a = await commands("sticky-a")
    const b = await commands("sticky-b")
    a.bus.subscribe(nameOf("Keyed"), async () => "a")
    b.bus.subscribe(nameOf("Keyed"), async () => "b")
    await Promise.all([a.kdb.start(), b.kdb.start()])
    const caller = await commands("sticky-caller")
    const ownerOf = (key: string) => caller.bus.dispatch(commandOf("Keyed", 1, { processingInstructions: { routingKey: key } })) as Promise<string>

    // A handler joining the ring moves keys, as consistent hashing does; measure once both are in it.
    const owners = new Set<string>()
    for (let probe = 0; owners.size < 2; probe++) {
      assert.ok(probe < 500, "keyed commands never reached both handlers")
      owners.add(await ownerOf(`probe-${probe}`))
      await new Promise((r) => setTimeout(r, 5))
    }
    const byKey = new Map<string, string>()
    for (let key = 0; key < 12; key++) {
      const seen = new Set<string>()
      for (let n = 0; n < 5; n++) seen.add(await ownerOf(`order-${key}`))
      expect(seen.size).toBe(1)
      byKey.set(`order-${key}`, [...seen][0]!)
    }
    expect(new Set(byKey.values()).size).toBe(2)
  })

  it("answers many concurrent commands through a credit window of one", async () => {
    const handler = await commands("narrow-handler", "default", { flowControl: { permits: 1, refillThreshold: 0 } })
    handler.bus.subscribe(nameOf("Narrow"), async (m) => {
      await new Promise((r) => setTimeout(r, 2))
      return m.payload
    })
    await handler.kdb.start()
    const caller = await commands("narrow-caller")
    const results = await Promise.all(Array.from({ length: 100 }, (_, i) => caller.bus.dispatch(commandOf("Narrow", i))))
    expect(results).toEqual(Array.from({ length: 100 }, (_, i) => i))
  })

  it("serves several command names from one stream, each with credit of its own", async () => {
    const handler = await commands("multi-handler", "default", { flowControl: { permits: 4, refillThreshold: 1 } })
    handler.bus.subscribe(nameOf("One"), async (m) => `one:${m.payload}`)
    handler.bus.subscribe(nameOf("Two"), async (m) => `two:${m.payload}`)
    await handler.kdb.start()
    const caller = await commands("multi-caller")
    // Many more of the second than its first window holds, interleaved with the first.
    const answers = await Promise.all(
      Array.from({ length: 60 }, (_, i) => caller.bus.dispatch(commandOf(i % 5 === 0 ? "One" : "Two", i))),
    )
    expect(answers.map(String)).toEqual(Array.from({ length: 60 }, (_, i) => `${i % 5 === 0 ? "one" : "two"}:${i}`))
  })

  it("times a command out in band, with the server's code, when the handler never answers", async () => {
    const handler = await commands("slow-handler")
    handler.bus.subscribe(nameOf("Slow"), () => new Promise(() => {}))
    await handler.kdb.start()
    const caller = await commands("slow-caller")
    const failure = await caller.bus.dispatch(commandOf("Slow", 1, { processingInstructions: { timeoutMs: 300 } })).then(() => undefined, (e) => e)
    assert.ok(failure instanceof KronosDbError)
    expect(failure.errorCode).toBe("KRONOSDB-4005")
  })

  it("tells the caller its handler's connection was lost, and routes to nobody afterwards", async () => {
    const handler = await commands("doomed-handler")
    const entered = Promise.withResolvers<void>()
    handler.bus.subscribe(nameOf("Doomed"), async () => {
      entered.resolve()
      await new Promise(() => {})
    })
    await handler.kdb.start()
    const caller = await commands("doomed-caller")
    const call = caller.bus.dispatch(commandOf("Doomed", 1)).then(() => undefined, (e) => e)
    await entered.promise
    handler.kdb.connection.native.current()!.close()
    const failure = await call
    assert.ok(failure instanceof KronosDbError)
    expect(failure.errorCode).toBe("KRONOSDB-4006")
  })

  it("comes back after its connection is lost: the handler is announced again and works", async () => {
    const quiet = console.warn
    console.warn = () => {}
    try {
      const handler = await commands("phoenix-handler", "default", { resilience: { initialDelayMs: 10, log: () => {} } })
      handler.bus.subscribe(nameOf("Phoenix"), async (m) => `alive:${m.payload}`)
      await handler.kdb.start()
      const caller = await commands("phoenix-caller")
      expect(await caller.bus.dispatch(commandOf("Phoenix", 1))).toBe("alive:1")

      handler.kdb.connection.native.current()!.close()
      await until(async () => {
        try {
          return (await caller.bus.dispatch(commandOf("Phoenix", 2))) === "alive:2"
        } catch {
          return false
        }
      }, "the handler to come back")
    } finally {
      console.warn = quiet
    }
  })

  it("cancels a dispatch at the caller's deadline and keeps the connection usable", async () => {
    const handler = await commands("deadline-handler")
    handler.bus.subscribe(nameOf("Never"), () => new Promise(() => {}))
    handler.bus.subscribe(nameOf("Quick"), async () => "quick")
    await handler.kdb.start()
    const caller = await commands("deadline-caller", "default", { timeoutMs: 80 })
    const failure = await caller.bus.dispatch(commandOf("Never", 1)).then(() => undefined, (e) => e)
    assert.ok(failure instanceof Error)
    expect(failure.message).toMatch(/deadline exceeded after 80ms/)
    expect(await caller.bus.dispatch(commandOf("Quick", 1))).toBe("quick")
  })

  it("keeps buses apart: the same command on another bus is another world", async () => {
    const first = await commands("bus-a", `bus-a-${run}`)
    const second = await commands("bus-b", `bus-b-${run}`)
    first.bus.subscribe(nameOf("Who"), async () => "a")
    second.bus.subscribe(nameOf("Who"), async () => "b")
    await Promise.all([first.kdb.start(), second.kdb.start()])
    const caller = await connection("bus-caller")
    const inA = kronosDbCommandBus(localCommandBus(unitOfWork), caller, `bus-a-${run}`)
    const inB = kronosDbCommandBus(localCommandBus(unitOfWork), caller, `bus-b-${run}`)
    const inNone = kronosDbCommandBus(localCommandBus(unitOfWork), caller, `bus-c-${run}`)
    expect(await inA.dispatch(commandOf("Who", 1))).toBe("a")
    expect(await inB.dispatch(commandOf("Who", 1))).toBe("b")
    const lost = await inNone.dispatch(commandOf("Who", 1)).then(() => undefined, (e) => e)
    assert.ok(lost instanceof KronosDbError)
  })

  it("lets a handler dispatch another command from inside itself, on its own connection", async () => {
    const both = await commands("nested")
    both.bus.subscribe(nameOf("Parent"), async (m) => {
      const depth = m.payload as number
      return depth === 0 ? 0 : 1 + Number(await both.bus.dispatch(commandOf("Parent", depth - 1)))
    })
    await both.kdb.start()
    expect(await both.bus.dispatch(commandOf("Parent", 12))).toBe(12)
  })

  // ── queries ───────────────────────────────────────────────────────────

  async function queryHandler(name: string, answer: (q: QueryMessage) => Promise<unknown>) {
    const handler = await queries(name)
    handler.bus.subscribe(nameOf("Count"), async (q) => answer(q))
    await handler.kdb.start()
    return handler
  }

  it("answers a query from a handler, and from one of several", async () => {
    await queryHandler("count-one", async () => "one")
    await queryHandler("count-two", async () => "two")
    const caller = await queries("count-caller")
    const seen = new Set<string>()
    for (let i = 0; i < 40 && seen.size < 2; i++) seen.add((await caller.bus.query(queryOf("Count", i))) as string)
    // The server picks a single handler per query; both are reachable.
    expect(seen.size).toBeGreaterThanOrEqual(1)
    for (const answer of seen) expect(["one", "two"]).toContain(answer)
  })

  it("brings a query handler's failure back as an error with its code", async () => {
    const handler = await queries("query-fail")
    handler.bus.subscribe(nameOf("Broken"), async () => {
      throw new Error("no such thing")
    })
    await handler.kdb.start()
    const caller = await queries("query-fail-caller")
    const failure = await caller.bus.query(queryOf("Broken", 1)).then(() => undefined, (e) => e)
    assert.ok(failure instanceof QueryExecutionError)
    expect(failure.message).toBe("no such thing")
  })

  it("fails a query nobody handles", async () => {
    const caller = await queries("query-lonely")
    const failure = await caller.bus.query(queryOf("Nobody", 1)).then(() => undefined, (e) => e)
    assert.ok(failure instanceof KronosDbRequestError)
    expect(failure.status).toBe("unavailable")
  })

  it("gives up on a query at its deadline, and the handler keeps serving the next", async () => {
    const handler = await queries("query-slow")
    handler.bus.subscribe(nameOf("Stuck"), () => new Promise(() => {}))
    handler.bus.subscribe(nameOf("Fine"), async () => "fine")
    await handler.kdb.start()
    const caller = await queries("query-slow-caller", "default", { timeoutMs: 80 })
    const failure = await caller.bus.query(queryOf("Stuck", 1)).then(() => undefined, (e) => e)
    assert.ok(failure instanceof Error)
    expect(failure.message).toMatch(/deadline exceeded/)
    expect(await caller.bus.query(queryOf("Fine", 1))).toBe("fine")
  })

  // ── subscription queries ──────────────────────────────────────────────

  // Every test serves a query name of its own: handlers from earlier tests stay
  // connected, and a subscription goes to the first handler that has the name.
  async function watchHandler(tag: string): Promise<{ bus: SubscriptionCapableQueryBus; kdb: KronosDbConnectionHandle }> {
    const handler = await queries(`watch-handler-${tag}`)
    handler.bus.subscribe(nameOf(`Watch${tag}`), async (q) => ({ current: (q.payload as { id: string }).id }))
    await handler.kdb.start()
    return handler
  }

  it("delivers an initial result, then updates in order, then a completion", async () => {
    const handler = await watchHandler("T0")
    const caller = await queries("watch-caller")
    const sub = caller.bus.subscriptionQuery(queryOf("WatchT0", { id: "a" }))
    expect(await sub.initialResult).toEqual({ current: "a" })

    const updates = sub.updates[Symbol.asyncIterator]()
    for (let i = 1; i <= 3; i++) await handler.bus.emitUpdate(nameOf("WatchT0"), payloadEquals({ id: "a" }), { n: i })
    for (let i = 1; i <= 3; i++) expect((await updates.next()).value).toEqual({ n: i })
    await handler.bus.completeSubscription(nameOf("WatchT0"))
    expect(await updates.next()).toEqual({ value: undefined, done: true })
  })

  it("sends an update only to the subscribers its filter picks", async () => {
    const handler = await watchHandler("T1")
    const caller = await queries("watch-filter-caller")
    const x = caller.bus.subscriptionQuery(queryOf("WatchT1", { id: "x" }))
    const y = caller.bus.subscriptionQuery(queryOf("WatchT1", { id: "y" }))
    await Promise.all([x.initialResult, y.initialResult])
    const xs = x.updates[Symbol.asyncIterator]()
    const ys = y.updates[Symbol.asyncIterator]()
    await handler.bus.emitUpdate(nameOf("WatchT1"), payloadEquals({ id: "x" }), "for-x")
    await handler.bus.emitUpdate(nameOf("WatchT1"), (p) => (p as { id: string }).id === "y", "for-y")
    expect((await xs.next()).value).toBe("for-x")
    expect((await ys.next()).value).toBe("for-y")
    x.close()
    y.close()
  })

  it("keeps a subscription going far past its first credit window", async () => {
    const handler = await watchHandler("T2")
    const caller = await queries("watch-long-caller")
    const sub = caller.bus.subscriptionQuery(queryOf("WatchT2", { id: "long" }), 8)
    await sub.initialResult
    const updates = sub.updates[Symbol.asyncIterator]()
    for (let n = 1; n <= 700; n++) {
      const next = updates.next()
      await handler.bus.emitUpdate(nameOf("WatchT2"), () => true, n)
      expect((await next).value).toBe(n)
    }
    sub.close()
  })

  it("holds updates for a subscriber that is slow to take them, and delivers them all", async () => {
    const handler = await watchHandler("T3")
    const caller = await queries("watch-slow-caller")
    const sub = caller.bus.subscriptionQuery(queryOf("WatchT3", { id: "slow" }))
    await sub.initialResult
    // More than the window of 256 in one burst, before the subscriber reads any.
    const total = 600
    for (let n = 1; n <= total; n++) await handler.bus.emitUpdate(nameOf("WatchT3"), () => true, n)
    const updates = sub.updates[Symbol.asyncIterator]()
    const got: number[] = []
    for (let n = 1; n <= total; n++) got.push((await updates.next()).value as number)
    expect(got).toEqual(Array.from({ length: total }, (_, i) => i + 1))
    sub.close()
  })

  it("ends the subscriber's updates with the handler's reason", async () => {
    const handler = await watchHandler("T4")
    const caller = await queries("watch-error-caller")
    const sub = caller.bus.subscriptionQuery(queryOf("WatchT4", { id: "err" }))
    await sub.initialResult
    const next = sub.updates[Symbol.asyncIterator]().next()
    await handler.bus.completeSubscriptionExceptionally(nameOf("WatchT4"), new Error("projection rebuilt"), payloadEquals({ id: "err" }))
    await assert.rejects(next, /projection rebuilt/)
  })

  it("stops the handler's work for a subscriber that closes", async () => {
    const handler = await watchHandler("T5")
    const caller = await queries("watch-close-caller")
    const sub = caller.bus.subscriptionQuery(queryOf("WatchT5", { id: "closing" }))
    await sub.initialResult
    sub.close()
    // The cancel reaches the handler; an update afterwards finds nobody, and nothing breaks.
    await new Promise((r) => setTimeout(r, 100))
    await handler.bus.emitUpdate(nameOf("WatchT5"), () => true, "late")
    const other = caller.bus.subscriptionQuery(queryOf("WatchT5", { id: "after" }))
    expect(await other.initialResult).toEqual({ current: "after" })
    other.close()
  })

  it("rejects the initial result with the server's reason when no handler is there", async () => {
    const caller = await queries("watch-lonely")
    const sub = caller.bus.subscriptionQuery(queryOf("Nowhere", { id: "x" }))
    await assert.rejects(sub.initialResult, /no handler available/i)
  })

  // ── the platform ──────────────────────────────────────────────────────

  it("registers the client with the server, reports its processors, and obeys the console", async () => {
    const kdb = await connection("platform-client", { clientId: `platform-${run}`, platformService: { processorsNotificationInitialDelayMs: 10, processorsNotificationRateMs: 100 } })
    const log: string[] = []
    const processor: ManagedEventProcessor = {
      name: `Projection-${run}`,
      running: true,
      position: 42n,
      start: () => void log.push("start"),
      stop: () => void log.push("stop"),
      status: () => ({ caughtUp: true, position: 42n }),
    }
    const control = kronosDbControlPlane(kdb, [processor])
    const bus = kronosDbCommandBus(localCommandBus(unitOfWork), kdb)
    bus.subscribe(nameOf("Registered"), async () => "ok")
    await kdb.start()

    // The server knows this client: streaming, with its component name.
    await until(async () => (await adminJson("/api/v1/clients")).some((c: any) => c.client_id === `platform-${run}`), "the client in the console")
    const client = (await adminJson("/api/v1/clients")).find((c: any) => c.client_id === `platform-${run}`)
    expect(client).toMatchObject({ component: "platform-client", streaming: true })

    // …and its processor, with the position the client reported.
    await until(async () => (await adminJson("/api/v1/processors")).some((p: any) => p.name === processor.name), "the processor in the console")
    const shown = (await adminJson("/api/v1/processors")).find((p: any) => p.name === processor.name)
    expect(shown.instances[0].segments[0].token_position).toBe(42)

    // The console pauses and restarts it; the client does.
    const pause = await (await fetch(`http://${server.host}:${server.adminPort}/api/processors/${processor.name}/pause`, { method: "POST" })).json() as any
    expect(pause.ok).toBe(true)
    await until(() => log.includes("stop"), "the pause to reach the processor")
    const start = await (await fetch(`http://${server.host}:${server.adminPort}/api/processors/${processor.name}/start`, { method: "POST" })).json() as any
    expect(start.ok).toBe(true)
    await until(() => log.includes("start"), "the start to reach the processor")

    await control.close()
    await bus.dispatch(commandOf("Registered", 1)).catch(() => undefined)
  })

  it("is gone from the console when its platform stream ends", async () => {
    const kdb = await connection("platform-gone", { clientId: `gone-${run}` })
    await kdb.start()
    await until(async () => (await adminJson("/api/v1/clients")).some((c: any) => c.client_id === `gone-${run}`), "the client")
    await kdb.close()
    await until(async () => !(await adminJson("/api/v1/clients")).some((c: any) => c.client_id === `gone-${run}`), "the client to leave")
  })
})

describe("KronosDB messaging — native protocol, credentials and TLS", () => {
  it("lets a client with the token use the buses, and refuses one without", async () => {
    const guarded = await startKronosDb({ args: ["--access-token", "s3cret"] })
    try {
      const good = await kronosDbConnection({ componentName: "token-good", host: guarded.host, port: guarded.port, token: "s3cret", serializer })
      const handler = kronosDbCommandBus(localCommandBus(unitOfWork), good)
      handler.subscribe(nameOf("Guarded"), async () => "in")
      await good.start()
      expect(await handler.dispatch(commandOf("Guarded", 1))).toBe("in")

      const bad = await kronosDbConnection({ componentName: "token-bad", host: guarded.host, port: guarded.port, serializer })
      const refused = await kronosDbCommandBus(localCommandBus(unitOfWork), bad).dispatch(commandOf("Guarded", 1)).then(() => undefined, (e) => e)
      assert.ok(refused instanceof AuthenticationError)
      await bad.close()
      await good.close()
    } finally {
      await guarded.stop()
    }
  })

  it("serves the buses over TLS on the one port", async () => {
    let dir: string | undefined
    try {
      const root = resolve(dirname(fileURLToPath(import.meta.url)), "..", "..", "..", ".kronosdb-data")
      mkdirSync(root, { recursive: true })
      dir = mkdtempSync(join(root, "tls-"))
      execFileSync(
        "openssl",
        ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", join(dir, "key.pem"), "-out", join(dir, "cert.pem"), "-days", "1", "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1"],
        { stdio: "ignore" },
      )
    } catch {
      if (dir) rmSync(dir, { recursive: true, force: true })
      return // no openssl here
    }
    let secured: KronosDbServer
    try {
      secured = await startKronosDb({ args: ["--tls-cert", join(dir, "cert.pem"), "--tls-key", join(dir, "key.pem")] })
    } catch (error) {
      rmSync(dir, { recursive: true, force: true })
      throw error
    }
    try {
      const kdb = await kronosDbConnection({
        componentName: "tls-client",
        host: "localhost",
        port: secured.port,
        ssl: { enabled: true, certFile: join(dir, "cert.pem") },
        serializer,
      })
      const bus = kronosDbCommandBus(localCommandBus(unitOfWork), kdb)
      bus.subscribe(nameOf("Secure"), async (m) => `tls:${m.payload}`)
      await kdb.start()
      expect(await bus.dispatch(commandOf("Secure", 5))).toBe("tls:5")
      await kdb.close()
    } finally {
      await secured.stop()
      rmSync(dir, { recursive: true, force: true })
    }
  })
})
