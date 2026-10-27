/**
 * BUSES ARE NAMED, CONTEXTS ARE NOT INVOLVED (server ADR-0006).
 *
 * Every messaging request — the handler streams, dispatch, query, subscription
 * queries — names its BUS by a small id the connection binds with BindBus, and
 * never a context: the server routes messaging by bus with no fallback to
 * context, so a context on a messaging request would be a lie waiting for a
 * reader. The store plane is the mirror image and binds contexts.
 */
import { afterEach, describe, expect, it } from "bun:test"
import { jsonSerializer, localCommandBus, localQueryBus, qn, unitOfWork } from "@kronos-ts/core"
import { kronosDbCommandBus } from "../kronosdb-command-bus.js"
import { kronosDbEventStore } from "../kronosdb-event-store.js"
import { kronosDbQueryBus } from "../kronosdb-query-bus.js"
import { rig, serverSide, until, type Rig } from "./bus-test-kit.js"

describe("kronosDb buses — bus-name addressing on the wire", () => {
  let r: Rig
  afterEach(() => r?.close())

  const frames = (name: string) => {
    const mine = r.server.connections.filter((c) => c.hello.clientId === r.client.connection.config.clientId)
    return mine.flatMap((c) => c.received).filter((f) => f.frame.name === name).map((f) => f.frame.body as any)
  }

  it("opens the command stream on the named bus, not the connection's context", async () => {
    r = await rig({ client: { context: "orders-ctx" } })
    kronosDbCommandBus(localCommandBus(unitOfWork), r.client.host, "orders").subscribe("test.Cmd", async () => undefined)
    await until(() => r.server.handlerStreams.length === 1, "the stream")
    expect(frames("BindBus")).toEqual([{ id: 1, name: "orders" }])
    expect(frames("OpenCommandHandlers")[0].bus).toBe(1)
    expect(r.server.handlerStreams[0]!.bus).toBe("orders")
    expect(frames("BindContext")).toEqual([])
  })

  it("means the server's default bus when none is named, which needs no binding", async () => {
    r = await rig({ client: { context: "orders-ctx" } })
    kronosDbCommandBus(localCommandBus(unitOfWork), r.client.host).subscribe("test.Cmd", async () => undefined)
    await until(() => r.server.handlerStreams.length === 1, "the stream")
    expect(frames("BindBus")).toEqual([])
    expect(frames("OpenCommandHandlers")[0].bus).toBe(0)
    expect(r.server.handlerStreams[0]!.bus).toBe("default")
  })

  it("opens the query stream on the named bus, not the connection's context", async () => {
    r = await rig({ client: { context: "orders-ctx" } })
    kronosDbQueryBus(localQueryBus(unitOfWork), r.client.host, "reads").subscribe("test.Query", async () => undefined)
    await until(() => r.server.handlerStreams.length === 1, "the stream")
    expect(frames("BindBus")).toEqual([{ id: 1, name: "reads" }])
    expect(frames("OpenQueryHandlers")[0].bus).toBe(1)
    expect(frames("BindContext")).toEqual([])
  })

  it("addresses dispatches, queries and subscription queries by the bus too", async () => {
    r = await rig({ client: { context: "orders-ctx" } })
    const commands = kronosDbCommandBus(localCommandBus(unitOfWork), r.client.host, "orders")
    const queries = kronosDbQueryBus(localQueryBus(unitOfWork), r.client.host, "reads")
    const message = (kind: "command" | "query") => ({ kind, identifier: crypto.randomUUID(), name: qn("test", "X"), payload: 1, metadata: {}, timestamp: 1 }) as never
    await commands.dispatch(message("command")).catch(() => {})
    await queries.query(message("query")).catch(() => {})
    const sub = queries.subscriptionQuery(message("query"))
    void sub.initialResult.catch(() => {})
    await until(() => frames("OpenSubscriptionQuery").length === 1, "the subscription")
    sub.close()
    expect(frames("BindBus")).toEqual([{ id: 1, name: "orders" }, { id: 2, name: "reads" }])
    expect(frames("DispatchCommand")[0].bus).toBe(1)
    expect(frames("DispatchQuery")[0].bus).toBe(2)
    expect(frames("OpenSubscriptionQuery")[0].bus).toBe(2)
    expect(frames("BindContext")).toEqual([])
  })

  it("binds a bus once, however many buses and streams use its name", async () => {
    r = await rig()
    const commands = kronosDbCommandBus(localCommandBus(unitOfWork), r.client.host, "shared")
    const queries = kronosDbQueryBus(localQueryBus(unitOfWork), r.client.host, "shared")
    commands.subscribe("test.Cmd", async () => undefined)
    queries.subscribe("test.Query", async () => undefined)
    await until(() => r.server.handlerStreams.length === 2, "both streams")
    expect(frames("BindBus")).toEqual([{ id: 1, name: "shared" }])
    expect(r.server.handlerStreams.map((h) => [h.kind, h.bus])).toEqual([["command", "shared"], ["query", "shared"]])
  })

  it("keeps the store plane's contexts and the messaging plane's buses in separate id spaces", async () => {
    r = await rig()
    const store = kronosDbEventStore({ connection: r.client.connection, serializer: jsonSerializer() }, "billing")
    // The fake does not serve the event store; the binding is what is under test.
    void store.source({ query: { tags: { k: "v" } } }).catch(() => {})
    const bus = kronosDbCommandBus(localCommandBus(unitOfWork), r.client.host, "billing")
    bus.subscribe("test.Cmd", async () => undefined)
    await until(() => frames("BindContext").length === 1 && frames("BindBus").length === 1, "both bindings")
    expect(frames("BindContext")).toEqual([{ id: 1, name: "billing" }])
    expect(frames("BindBus")).toEqual([{ id: 1, name: "billing" }])
  })
})

describe("kronosDb buses — credentials", () => {
  let r: Rig
  afterEach(() => r?.close())

  it("send the token once, in the handshake, and not on any request", async () => {
    r = await rig({ client: { token: "secret" } })
    kronosDbCommandBus(localCommandBus(unitOfWork), r.client.host, "orders").subscribe("test.Cmd", async () => undefined)
    await until(() => r.server.handlerStreams.length === 1, "the stream")
    const hello = serverSide(r.server, r.client).hello
    // A client that cannot tell a static token from a JWT says `token`.
    expect(hello).toMatchObject({ credentialKind: 1, credential: "secret" })
  })
})
