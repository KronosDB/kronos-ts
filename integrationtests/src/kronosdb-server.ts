/**
 * A KronosDB server for a test to talk to.
 *
 * Two ways to get one, chosen by the environment:
 *
 *   KRONOSDB_SERVER_BIN=/path/to/kronosdb-server   a local binary, started on
 *                                                  free ports with its own data
 *                                                  directory, stopped and
 *                                                  deleted afterwards
 *   (unset)                                        a container, from
 *                                                  KRONOSDB_IMAGE (default
 *                                                  ghcr.io/kronosdb/kronosdb:0.11.0)
 *
 * KRONOSDB_SERVER_ARGS adds command-line arguments to the binary (split on
 * spaces), e.g. `--ack-mode written` to take fsync out of a measurement. A
 * test's own arguments (`startKronosDb({ args })`) go to either.
 *
 * Everything — the event store, the buses, the platform — speaks the native
 * protocol on `port`, which KronosDB serves from 0.11.0 on.
 *
 * `startKronosDbCluster` starts a Raft cluster the same way: three local
 * processes with the binary, three containers on a network otherwise.
 *
 * The data directory lives under `.kronosdb-data/` in the repository (ignored
 * by git), not in the system temp directory, which the OS may clean mid-run.
 */
import { spawn, type ChildProcess } from "node:child_process"
import { existsSync, mkdirSync, mkdtempSync, rmSync } from "node:fs"
import { createServer } from "node:net"
import { basename, dirname, isAbsolute, join, resolve } from "node:path"
import { fileURLToPath } from "node:url"

export type KronosDbServer = {
  readonly host: string
  /** The port clients (and, in a cluster, peers) connect to. */
  readonly port: number
  /** The admin HTTP port: the console, `/ready` and `/api/*`. */
  readonly adminPort: number
  stop(): Promise<void>
}

const DEFAULT_IMAGE = "ghcr.io/kronosdb/kronosdb:0.11.0"
const READY_TIMEOUT_MS = 60_000

async function freePort(): Promise<number> {
  return new Promise((resolvePort, reject) => {
    const probe = createServer()
    probe.once("error", reject)
    probe.listen(0, "127.0.0.1", () => {
      const address = probe.address()
      probe.close(() => (address && typeof address === "object" ? resolvePort(address.port) : reject(new Error("no port"))))
    })
  })
}

async function untilReady(url: string, child: ChildProcess, log: () => string): Promise<void> {
  const deadline = Date.now() + READY_TIMEOUT_MS
  while (Date.now() < deadline) {
    if (child.exitCode !== null) throw new Error(`KronosDB exited with code ${child.exitCode} before it was ready:\n${log()}`)
    try {
      // /ready is claim-aware: 200 only once the node can execute writes. The
      // startup log line prints BEFORE the leader claim commits.
      if ((await fetch(url)).status === 200) return
    } catch {
      // Not listening yet.
    }
    await new Promise((r) => setTimeout(r, 100))
  }
  throw new Error(`KronosDB was not ready at ${url} after ${READY_TIMEOUT_MS}ms:\n${log()}`)
}

type Binding = { port: number; adminPort: number }

/** Start the binary on the given ports; `cluster` makes it node `id` of a cluster with the listed peers. */
async function startBinary(
  binary: string,
  ports?: Binding,
  cluster?: { id: number; peers: string },
  args: readonly string[] = [],
): Promise<KronosDbServer> {
  const root = resolve(dirname(fileURLToPath(import.meta.url)), "..", "..", ".kronosdb-data")
  mkdirSync(root, { recursive: true })
  const data = mkdtempSync(join(root, "run-"))
  const port = ports?.port ?? (await freePort())
  const adminPort = ports?.adminPort ?? (await freePort())

  let output = ""
  const child = spawn(
    binary,
    [
      "--data-dir",
      data,
      "--listen",
      `127.0.0.1:${port}`,
      "--admin-listen",
      `127.0.0.1:${adminPort}`,
      ...(cluster ? ["--cluster-node-id", String(cluster.id), "--cluster-peers", cluster.peers, "--node-name", `kronos-${cluster.id}`] : []),
      ...(process.env.KRONOSDB_SERVER_ARGS?.split(" ").filter(Boolean) ?? []),
      ...args,
    ],
    { stdio: ["ignore", "pipe", "pipe"] },
  )
  child.stdout?.on("data", (chunk) => (output = (output + chunk).slice(-8_000)))
  child.stderr?.on("data", (chunk) => (output = (output + chunk).slice(-8_000)))

  try {
    await untilReady(`http://127.0.0.1:${adminPort}/ready`, child, () => output)
  } catch (error) {
    child.kill("SIGKILL")
    rmSync(data, { recursive: true, force: true })
    throw error
  }

  return {
    host: "127.0.0.1",
    port,
    adminPort,
    async stop() {
      if (child.exitCode === null) {
        const exited = new Promise<void>((r) => child.once("exit", () => r()))
        child.kill("SIGTERM")
        const killer = setTimeout(() => child.kill("SIGKILL"), 10_000)
        await exited
        clearTimeout(killer)
      }
      rmSync(data, { recursive: true, force: true })
    },
  }
}

async function startContainer(image: string, args: readonly string[] = []): Promise<KronosDbServer> {
  // Loaded only here: a run against a local binary needs no Docker.
  const { GenericContainer, Wait } = await import("testcontainers")
  // A host file among the arguments (a TLS certificate or key) is copied in,
  // and the argument names it by its path inside the container.
  const files = args.filter((arg) => isAbsolute(arg) && existsSync(arg))
  const inside = (file: string) => `/config/${basename(file)}`
  const container = await new GenericContainer(image)
    .withCommand(args.map((arg) => (files.includes(arg) ? inside(arg) : arg)))
    .withCopyFilesToContainer(files.map((source) => ({ source, target: inside(source), mode: 0o644 })))
    .withExposedPorts(50051, 9240)
    .withWaitStrategy(Wait.forHttp("/ready", 9240).forStatusCode(200))
    .start()
  return {
    host: container.getHost(),
    port: container.getMappedPort(50051),
    adminPort: container.getMappedPort(9240),
    async stop() {
      await container.stop()
    },
  }
}

/** Start a KronosDB server: the local binary if KRONOSDB_SERVER_BIN is set, a container otherwise. */
export function startKronosDb(options: { args?: readonly string[] } = {}): Promise<KronosDbServer> {
  const binary = process.env.KRONOSDB_SERVER_BIN
  if (binary) return startBinary(binary, undefined, undefined, options.args)
  return startContainer(process.env.KRONOSDB_IMAGE ?? DEFAULT_IMAGE, options.args)
}

/**
 * Start a cluster of `size` voters and wait until every node is ready. With a
 * local binary the nodes are processes on free ports of 127.0.0.1, each with its
 * own data directory; peers reach each other on the port clients use. Otherwise
 * they are containers on one network, aliased `kronos-1`…`kronos-N`.
 *
 * Stopping any node stops it alone; the network of a container cluster goes
 * with the last one.
 */
export async function startKronosDbCluster(size = 3): Promise<KronosDbServer[]> {
  const binary = process.env.KRONOSDB_SERVER_BIN
  if (binary) {
    const ports: Binding[] = []
    for (let i = 0; i < size; i++) ports.push({ port: await freePort(), adminPort: await freePort() })
    const peers = ports.map((p, i) => `${i + 1}=127.0.0.1:${p.port}`).join(",")
    // Voters wait for a quorum to be ready, so they have to start together.
    const started = await Promise.allSettled(ports.map((binding, i) => startBinary(binary, binding, { id: i + 1, peers })))
    const nodes = started.flatMap((r) => (r.status === "fulfilled" ? [r.value] : []))
    const failure = started.find((r) => r.status === "rejected")
    if (failure) {
      await Promise.all(nodes.map((n) => n.stop()))
      throw (failure as PromiseRejectedResult).reason
    }
    return nodes
  }

  const { GenericContainer, Network, Wait } = await import("testcontainers")
  const image = process.env.KRONOSDB_IMAGE ?? DEFAULT_IMAGE
  const network = await new Network().start()
  const peers = Array.from({ length: size }, (_, i) => `${i + 1}=kronos-${i + 1}:50051`).join(",")
  const containers = await Promise.all(
    Array.from({ length: size }, (_, i) =>
      new GenericContainer(image)
        .withNetwork(network)
        .withNetworkAliases(`kronos-${i + 1}`)
        .withEnvironment({ KRONOSDB_CLUSTER_NODE_ID: String(i + 1), KRONOSDB_CLUSTER_PEERS: peers })
        .withExposedPorts(50051, 9240)
        .withWaitStrategy(Wait.forHttp("/ready", 9240).forStatusCode(200))
        .start(),
    ),
  )
  let remaining = containers.length
  return containers.map((container) => ({
    host: container.getHost(),
    port: container.getMappedPort(50051),
    adminPort: container.getMappedPort(9240),
    async stop() {
      await container.stop()
      if (--remaining === 0) await network.stop()
    },
  }))
}
