import { execFileSync } from "node:child_process"
import { arch, cpus, platform, release } from "node:os"
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers"
import { jsonSerializer } from "@kronos-ts/messaging"
import type { EventStore } from "@kronos-ts/eventsourcing"
import { descriptorBasedTagResolver } from "@kronos-ts/eventsourcing"
import { connectToKronosDb, kronosDbEventStore, type KronosDbConnection } from "@kronos-ts/kronosdb"
import {
  bootstrapSchema,
  postgresEventStore,
  type PostgresAdapter,
} from "@kronos-ts/postgres"
import { bunSqlAdapter } from "@kronos-ts/postgres/adapters/bun-sql"
import { pgAdapter } from "@kronos-ts/postgres/adapters/pg"
import {
  POSTGRES_IMAGE,
  type BackendName,
  type BenchmarkOptions,
} from "./config.js"

export interface BackendMetadata {
  readonly backend: BackendName
  readonly image: string
  readonly imageDigests: readonly string[]
  readonly containerId: string
  readonly cpuLimit: number
  readonly memoryBytes: number
  readonly durability: Record<string, string>
}

export interface BackendHarness {
  readonly name: BackendName
  readonly store: EventStore
  readonly metadata: BackendMetadata
  close(): Promise<void>
}

export interface HostMetadata {
  readonly platform: string
  readonly release: string
  readonly architecture: string
  readonly cpuModel: string
  readonly logicalCpus: number
  readonly bunVersion: string
}

export function hostMetadata(): HostMetadata {
  const hostCpus = cpus()
  return {
    platform: platform(),
    release: release(),
    architecture: arch(),
    cpuModel: hostCpus[0]?.model ?? "unknown",
    logicalCpus: hostCpus.length,
    bunVersion: (process.versions as Record<string, string | undefined>).bun ?? "unknown",
  }
}

async function imageDigests(image: string): Promise<string[]> {
  try {
    const text = execFileSync(
      "docker",
      ["image", "inspect", image, "--format", "{{json .RepoDigests}}"],
      { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] },
    )
    const parsed = JSON.parse(text.trim())
    return Array.isArray(parsed) ? parsed.filter((value): value is string => typeof value === "string") : []
  } catch {
    return []
  }
}

async function stopQuietly(container: StartedTestContainer | undefined): Promise<void> {
  if (!container) return
  try {
    await container.stop()
  } catch {
    // Preserve the original benchmark error.
  }
}

async function startKronosDb(options: BenchmarkOptions): Promise<BackendHarness> {
  let container: StartedTestContainer | undefined
  let connection: KronosDbConnection | undefined
  try {
    container = await new GenericContainer(options.kronosdbImage)
      .withEnvironment({
        KRONOSDB_LISTEN: "0.0.0.0:50051",
        KRONOSDB_ADMIN_LISTEN: "0.0.0.0:9240",
        KRONOSDB_GROUP_COMMIT_MS: String(options.kronosdbGroupCommitMs),
      })
      .withExposedPorts(50051, 9240)
      .withResourcesQuota({ cpu: options.cpu, memory: options.memoryBytes })
      .withStartupTimeout(120_000)
      .withWaitStrategy(Wait.forHttp("/ready", 9240).forStatusCode(200))
      .start()

    connection = connectToKronosDb({
      componentName: `kronos-ts-benchmark-${options.seed}`,
      host: container.getHost(),
      port: container.getMappedPort(50051),
      context: "default",
    })
    const store = kronosDbEventStore(connection, jsonSerializer())
    const metadata: BackendMetadata = {
      backend: "kronosdb",
      image: options.kronosdbImage,
      imageDigests: await imageDigests(options.kronosdbImage),
      containerId: container.getId(),
      cpuLimit: options.cpu,
      memoryBytes: options.memoryBytes,
      durability: { mode: "strict", groupCommitMs: String(options.kronosdbGroupCommitMs), voters: "1" },
    }

    return {
      name: "kronosdb",
      store,
      metadata,
      async close() {
        connection?.close()
        connection = undefined
        await stopQuietly(container)
        container = undefined
      },
    }
  } catch (error) {
    connection?.close()
    await stopQuietly(container)
    throw error
  }
}

async function startPostgres(options: BenchmarkOptions): Promise<BackendHarness> {
  let container: StartedTestContainer | undefined
  let adapter: PostgresAdapter | undefined
  try {
    container = await new GenericContainer(POSTGRES_IMAGE)
      .withEnvironment({
        POSTGRES_PASSWORD: "benchmark",
        POSTGRES_DB: "benchmark",
        POSTGRES_USER: "benchmark",
      })
      .withExposedPorts(5432)
      .withResourcesQuota({ cpu: options.cpu, memory: options.memoryBytes })
      .withStartupTimeout(120_000)
      .withWaitStrategy(Wait.forLogMessage("database system is ready to accept connections", 2))
      .start()

    const connectionString = `postgresql://benchmark:benchmark@${container.getHost()}:${container.getMappedPort(5432)}/benchmark`
    adapter = options.postgresAdapter === "pg"
      ? pgAdapter({ connectionString })
      : bunSqlAdapter({ connectionString })
    await adapter.connect()
    await bootstrapSchema(adapter)

    const fsyncRows = await adapter.query<{ fsync: string }>("SHOW fsync")
    const syncRows = await adapter.query<{ synchronous_commit: string }>("SHOW synchronous_commit")
    const fsync = fsyncRows[0]?.fsync ?? "unknown"
    const synchronousCommit = syncRows[0]?.synchronous_commit ?? "unknown"
    if (fsync !== "on" || synchronousCommit !== "on") {
      throw new Error(`Postgres durability is not strict: fsync=${fsync}, synchronous_commit=${synchronousCommit}`)
    }

    const store = postgresEventStore({
      adapter,
      serializer: jsonSerializer(),
      tagResolver: descriptorBasedTagResolver(),
    })
    const metadata: BackendMetadata = {
      backend: "postgres",
      image: POSTGRES_IMAGE,
      imageDigests: await imageDigests(POSTGRES_IMAGE),
      containerId: container.getId(),
      cpuLimit: options.cpu,
      memoryBytes: options.memoryBytes,
      durability: { fsync, synchronousCommit, adapter: options.postgresAdapter },
    }

    return {
      name: "postgres",
      store,
      metadata,
      async close() {
        await adapter?.disconnect()
        adapter = undefined
        await stopQuietly(container)
        container = undefined
      },
    }
  } catch (error) {
    try { await adapter?.disconnect() } catch { /* Preserve the original error. */ }
    await stopQuietly(container)
    throw error
  }
}

export function startBackend(name: BackendName, options: BenchmarkOptions): Promise<BackendHarness> {
  return name === "kronosdb" ? startKronosDb(options) : startPostgres(options)
}
