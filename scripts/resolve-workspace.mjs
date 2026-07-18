// Prepare a workspace package manifest for `npm publish`:
//
// 1. Rewrite `workspace:*` (and workspace:^ / workspace:~) dependency
//    specifiers to concrete versions read from the workspace.
// 2. Copy compiled entrypoint overrides from `publishConfig` to the top-level
//    manifest fields that Node and npm consumers actually resolve.
//
// `npm publish` does not understand Bun's workspace protocol and does not use
// nested `publishConfig.main` / `types` / `exports` as package entrypoints. It
// does support npm Trusted Publishing (OIDC), which `bun publish` does not. The
// publish script restores the original package.json after each package.
import { readFileSync, writeFileSync, readdirSync } from "node:fs"
import { join, dirname } from "node:path"
import { fileURLToPath } from "node:url"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")
const target = process.argv[2]
if (!target) {
  console.error("usage: resolve-workspace.mjs <path-to-package.json>")
  process.exit(1)
}

// Build name -> version from every workspace package.
const dirs = [
  ...readdirSync(join(root, "packages")).map((d) => join(root, "packages", d)),
  ...readdirSync(join(root, "packages", "extensions")).map((d) => join(root, "packages", "extensions", d)),
]
const versions = {}
for (const d of dirs) {
  try {
    const p = JSON.parse(readFileSync(join(d, "package.json"), "utf8"))
    if (p.name && p.version) versions[p.name] = p.version
  } catch {
    // not a package dir — ignore
  }
}

const pkg = JSON.parse(readFileSync(target, "utf8"))
let resolvedDependencies = 0
for (const field of ["dependencies", "devDependencies", "peerDependencies", "optionalDependencies"]) {
  const deps = pkg[field]
  if (!deps) continue
  for (const [name, spec] of Object.entries(deps)) {
    if (typeof spec !== "string" || !spec.startsWith("workspace:")) continue
    const v = versions[name]
    if (!v) throw new Error(`Cannot resolve workspace dependency ${name}: not found in workspace`)
    const rest = spec.slice("workspace:".length)
    deps[name] = rest === "*" || rest === "" ? v : rest === "^" ? `^${v}` : rest === "~" ? `~${v}` : rest
    resolvedDependencies++
  }
}

let appliedEntrypoints = 0
for (const field of ["main", "types", "exports"]) {
  const value = pkg.publishConfig?.[field]
  if (value === undefined) continue
  pkg[field] = value
  appliedEntrypoints++
}

writeFileSync(target, JSON.stringify(pkg, null, 2) + "\n")
console.log(
  `resolved ${resolvedDependencies} workspace dep(s) and applied ${appliedEntrypoints} publish entrypoint(s) in ${target.replace(root + "/", "")}`,
)
