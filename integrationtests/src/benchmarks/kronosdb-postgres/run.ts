import { execFileSync } from "node:child_process"
import { fileURLToPath } from "node:url"
import { backendOrder, parseArgs, type ScenarioName } from "./config.js"
import { hostMetadata, startBackend, type BackendMetadata } from "./backends.js"
import { runScenario } from "./scenarios.js"
import {
  comparisonTable,
  createDocument,
  stringifyDocument,
  type ScenarioSample,
} from "./report.js"

const ALL_SCENARIOS: readonly ScenarioName[] = [
  "append",
  "append-c",
  "workflow",
  "dcb",
  "rehydration",
  "catchup-default",
  "catchup-100",
  "live",
]

function gitMetadata(): { sha: string; dirty: boolean } {
  const root = fileURLToPath(new URL("../../../..", import.meta.url))
  try {
    const sha = execFileSync("git", ["-C", root, "rev-parse", "HEAD"], { encoding: "utf8" }).trim()
    const status = execFileSync("git", ["-C", root, "status", "--porcelain"], { encoding: "utf8" })
    return { sha, dirty: status.trim().length > 0 }
  } catch {
    return { sha: "unknown", dirty: true }
  }
}

async function main(): Promise<void> {
  const options = parseArgs(process.argv.slice(2))
  const scenarios = options.scenario ? [options.scenario] : ALL_SCENARIOS
  const startedAt = new Date()
  const results: ScenarioSample[] = []
  const metadata: BackendMetadata[] = []
  let correctnessChecks = 0

  console.error(
    `KronosDB/Postgres benchmark: profile=${options.profileName}, samples=${options.samples}, ` +
    `postgresAdapter=${options.postgresAdapter}, cpu=${options.cpu}, ` +
    `memory=${(options.memoryBytes / 1024 ** 3).toFixed(1)} GiB, ` +
    `backend=${options.backend ?? "both"}, kronosdbGroupCommitMs=${options.kronosdbGroupCommitMs}`,
  )

  for (let group = 0; group < scenarios.length; group++) {
    const scenario = scenarios[group]!
    const backends = backendOrder(group, options.order)
      .filter((backend) => options.backend === undefined || backend === options.backend)
    for (const backend of backends) {
      console.error(`\n[${scenario}] starting ${backend}`)
      const harness = await startBackend(backend, options)
      metadata.push(harness.metadata)
      try {
        const started = performance.now()
        const run = await runScenario(scenario, harness, options)
        results.push(...run.samples)
        correctnessChecks += run.correctnessChecks
        console.error(
          `[${scenario}] ${backend} complete in ${((performance.now() - started) / 1_000).toFixed(1)}s ` +
          `(${run.correctnessChecks} checks)`,
        )
      } finally {
        await harness.close()
      }
    }
  }

  const finishedAt = new Date()
  const document = createDocument({
    options,
    startedAt,
    finishedAt,
    host: hostMetadata(),
    backendMetadata: metadata,
    git: gitMetadata(),
    results,
    correctnessChecks,
  })
  console.error(`\n${comparisonTable(document.comparisons)}`)
  for (const highlight of document.highlights) console.error(`\n${highlight}`)
  process.stdout.write(`${stringifyDocument(document)}\n`)
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack ?? error.message : error)
  process.exit(1)
})
