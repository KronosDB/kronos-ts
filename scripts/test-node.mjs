// Runs the Node.js suite in node-tests/ against the packages as npm publishes them.
//
// Each public package is built, its manifest rewritten by resolve-workspace.mjs (as publish.sh does),
// packed with `npm pack`, and restored. The tarballs are installed into node-tests/, a plain Node project
// outside the workspace, and its tests run with `node --test`. Run with Node, after `bun run build`.
import { execFileSync } from "node:child_process"
import { mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")
const suite = join(root, "node-tests")
const packs = join(suite, ".packs")
const packages = ["core", "test", "postgres", "otlp", "kronosdb", "rabbitmq", "axon-server"]

const run = (command, args, cwd) => execFileSync(command, args, { cwd, stdio: "inherit" })

rmSync(packs, { recursive: true, force: true })
mkdirSync(packs, { recursive: true })
for (const name of packages) {
  const dir = join(root, "packages", name)
  const manifest = join(dir, "package.json")
  const original = readFileSync(manifest, "utf8")
  try {
    run("node", [join(root, "scripts", "resolve-workspace.mjs"), manifest], root)
    run("npm", ["pack", "--silent", "--pack-destination", packs], dir)
  } finally {
    writeFileSync(manifest, original)
  }
}

const tarballs = readdirSync(packs).map((file) => join(packs, file))
rmSync(join(suite, "node_modules"), { recursive: true, force: true })
run("npm", ["install", "--no-save", "--no-package-lock", "--no-audit", "--no-fund", ...tarballs], suite)
const tests = readdirSync(join(suite, "tests"))
  .filter((file) => file.endsWith(".test.mjs"))
  .map((file) => join("tests", file))
run("node", ["--test", "--test-concurrency=1", ...tests], suite)
