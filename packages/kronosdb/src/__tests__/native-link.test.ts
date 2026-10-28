import { afterEach, describe, expect, it } from "bun:test"
import { AuthenticationError, ConnectionFailedError } from "../errors.js"
import { nativeLink, type NativeLink } from "../native-link.js"
import { ErrorCode } from "../protocol/kronosdb-protocol.js"
import { fakeNativeServer, type FakeNativeServer } from "./fake-native-server.js"

const servers: FakeNativeServer[] = []
let link: NativeLink | undefined

afterEach(async () => {
  link?.close()
  link = undefined
  await Promise.all(servers.splice(0).map((s) => s.close()))
})

async function fake(options?: Parameters<typeof fakeNativeServer>[0]) {
  const server = await fakeNativeServer(options)
  servers.push(server)
  return server
}

async function deadPort(): Promise<number> {
  const probe = await fakeNativeServer()
  const port = probe.port
  await probe.close()
  return port
}

const base = { clientName: "link-test", clientId: "link-1", token: "", connectTimeoutMs: 2000 }

describe("native link", () => {
  it("opens no socket until somebody needs one", async () => {
    const server = await fake()
    link = nativeLink({ ...base, addresses: [`127.0.0.1:${server.port}`] })
    await Bun.sleep(50)
    expect(server.connections).toHaveLength(0)
    expect(link.current()).toBeUndefined()

    const transport = await link.ready()
    expect(server.connections).toHaveLength(1)
    expect(link.current()).toBe(transport)
  })

  it("shares one connection among callers who ask at once", async () => {
    const server = await fake()
    link = nativeLink({ ...base, addresses: [`127.0.0.1:${server.port}`] })
    const [a, b, c] = await Promise.all([link.ready(), link.ready(), link.ready()])
    expect(a).toBe(b)
    expect(b).toBe(c)
    expect(server.connections).toHaveLength(1)
  })

  it("sends the credentials and names it was configured with", async () => {
    const server = await fake()
    link = nativeLink({ ...base, token: "tok", clientName: "billing", clientId: "billing-7", addresses: [`127.0.0.1:${server.port}`] })
    await link.ready()
    expect(server.connections[0]!.hello).toMatchObject({ clientName: "billing", clientId: "billing-7", credential: "tok", credentialKind: 1 })
  })

  it("tries the next server when the first is down, and starts from the one that worked", async () => {
    const down = await deadPort()
    const up = await fake()
    link = nativeLink({ ...base, addresses: [`127.0.0.1:${down}`, `127.0.0.1:${up.port}`] })
    await link.ready()
    expect(up.connections).toHaveLength(1)

    link.current()!.close()
    await Bun.sleep(20)
    await link.ready()
    expect(up.connections).toHaveLength(2)
  })

  it("does not shop refused credentials around the cluster", async () => {
    const refusing = await fake({ refuse: { code: ErrorCode.unauthenticated, message: "bad token" } })
    const accepting = await fake()
    link = nativeLink({ ...base, addresses: [`127.0.0.1:${refusing.port}`, `127.0.0.1:${accepting.port}`] })
    const failure = await link.ready().then(
      () => undefined,
      (e) => e,
    )
    expect(failure).toBeInstanceOf(AuthenticationError)
    expect(accepting.connections).toHaveLength(0)
  })

  it("reports a ConnectionFailedError when every server is down", async () => {
    link = nativeLink({ ...base, addresses: [`127.0.0.1:${await deadPort()}`, `127.0.0.1:${await deadPort()}`] })
    const failure = await link.ready().then(
      () => undefined,
      (e) => e,
    )
    expect(failure).toBeInstanceOf(ConnectionFailedError)
    // And a later call tries again rather than remembering the failure.
    const again = await link.ready().then(
      () => undefined,
      (e) => e,
    )
    expect(again).toBeInstanceOf(ConnectionFailedError)
  })

  it("replaces a transport the server closed, and tells the listeners", async () => {
    const server = await fake()
    link = nativeLink({ ...base, addresses: [`127.0.0.1:${server.port}`] })
    const connects: unknown[] = []
    const closes: Array<Error | undefined> = []
    link.onConnect((t) => connects.push(t))
    link.onClose((e) => closes.push(e))

    const first = await link.ready()
    server.connections[0]!.destroy()
    await Bun.sleep(30)
    expect(link.current()).toBeUndefined()
    expect(closes).toHaveLength(1)

    const second = await link.ready()
    expect(second).not.toBe(first)
    expect(connects).toEqual([first, second])
  })

  it("moves new work to a fresh connection when the server says GoAway, and leaves the old one open", async () => {
    const server = await fake()
    link = nativeLink({ ...base, addresses: [`127.0.0.1:${server.port}`] })
    const first = await link.ready()
    server.connections[0]!.reply(0, 0, { name: "GoAway", body: { code: ErrorCode.unavailable, message: "draining" } })
    await Bun.sleep(30)

    expect(link.current()).toBeUndefined()
    const second = await link.ready()
    expect(second).not.toBe(first)
    expect(first.closed).toBe(false)
    expect(server.connections).toHaveLength(2)
  })

  describe("restart", () => {
    it("does nothing if there was never a connection, or the connection is healthy", async () => {
      const server = await fake()
      link = nativeLink({ ...base, addresses: [`127.0.0.1:${server.port}`] })
      await link.restart()
      expect(server.connections).toHaveLength(0)

      const transport = await link.ready()
      await link.restart()
      expect(link.current()).toBe(transport)
      expect(server.connections).toHaveLength(1)
    })

    it("reconnects a lost connection, and fails if the server is still gone", async () => {
      const server = await fake()
      link = nativeLink({ ...base, addresses: [`127.0.0.1:${server.port}`] })
      await link.ready()
      server.connections[0]!.destroy()
      await Bun.sleep(30)

      await link.restart()
      expect(link.current()).toBeDefined()
      expect(server.connections).toHaveLength(2)

      const gone = nativeLink({ ...base, addresses: [`127.0.0.1:${server.port}`] })
      await gone.ready()
      await server.close()
      servers.splice(servers.indexOf(server), 1)
      await Bun.sleep(30)
      const failure = await gone.restart().then(
        () => undefined,
        (e) => e,
      )
      expect(failure).toBeInstanceOf(ConnectionFailedError)
      gone.close()
    })
  })

  it("closes every socket and refuses to connect again", async () => {
    const server = await fake()
    link = nativeLink({ ...base, addresses: [`127.0.0.1:${server.port}`] })
    const transport = await link.ready()
    link.close()
    expect(transport.closed).toBe(true)
    const failure = await link.ready().then(
      () => undefined,
      (e) => e,
    )
    expect(failure).toBeInstanceOf(ConnectionFailedError)
  })

  it("rejects an address with no port", () => {
    expect(() => nativeLink({ ...base, addresses: ["localhost"] })).toThrow(/host:port/)
  })
})
