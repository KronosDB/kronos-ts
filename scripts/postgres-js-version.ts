// Fails unless the postgres.js that `@kronos-ts/postgres` and the integration
// tests import is the version given: CI runs the Postgres suites once per
// accepted major, and this is what says which one a run was on.
//
//   bun run scripts/postgres-js-version.ts 4.1.1

import { readFileSync } from "node:fs"
import { createRequire } from "node:module"
import { dirname, join, resolve } from "node:path"

const wanted = process.argv[2]
if (!wanted) {
  console.error("usage: bun run scripts/postgres-js-version.ts <version>")
  process.exit(2)
}

function installedFrom(workspace: string): string {
  const entry = createRequire(resolve(workspace, "package.json")).resolve("postgres")
  for (let dir = dirname(entry); dir !== dirname(dir); dir = dirname(dir)) {
    try {
      const manifest = JSON.parse(readFileSync(join(dir, "package.json"), "utf8")) as { name?: string; version?: string }
      if (manifest.name === "postgres" && manifest.version) return manifest.version
    } catch {
      // No manifest in this directory; look one up.
    }
  }
  throw new Error(`No postgres package.json above ${entry}`)
}

let wrong = false
for (const workspace of ["packages/postgres", "integrationtests"]) {
  const installed = installedFrom(workspace)
  console.log(`${workspace}: postgres ${installed}`)
  if (installed !== wanted) wrong = true
}
if (wrong) {
  console.error(`Expected postgres ${wanted} in both. A lockfile that moved needs the matrix in .github/workflows/ci.yml to move with it.`)
  process.exit(1)
}
