import { afterEach, describe, expect, it } from "bun:test"
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { spawnSync } from "node:child_process"

const repositoryRoot = resolve(import.meta.dir, "../../../..")
const temporaryDirectories: string[] = []

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    rmSync(directory, { recursive: true, force: true })
  }
})

describe("npm publish manifest preparation", () => {
  it("resolves workspace versions and exposes compiled entrypoints", () => {
    const directory = mkdtempSync(join(tmpdir(), "kronos-ts-publish-"))
    temporaryDirectories.push(directory)
    const manifestPath = join(directory, "package.json")
    writeFileSync(
      manifestPath,
      JSON.stringify({
        name: "@kronos-ts/publish-fixture",
        version: "1.0.0",
        main: "src/index.ts",
        types: "src/index.ts",
        exports: { ".": "./src/index.ts" },
        publishConfig: {
          access: "public",
          main: "./dist/index.js",
          types: "./dist/index.d.ts",
          exports: {
            ".": {
              types: "./dist/index.d.ts",
              default: "./dist/index.js",
            },
          },
        },
        dependencies: {
          "@kronos-ts/core": "workspace:*",
        },
      }),
    )

    const result = spawnSync(
      process.execPath,
      [join(repositoryRoot, "scripts/resolve-workspace.mjs"), manifestPath],
      { encoding: "utf8" },
    )
    expect(result.status, result.stderr).toBe(0)

    const manifest = JSON.parse(readFileSync(manifestPath, "utf8"))
    const core = JSON.parse(
      readFileSync(join(repositoryRoot, "packages/core/package.json"), "utf8"),
    )
    expect(manifest.dependencies["@kronos-ts/core"]).toBe(core.version)
    expect(manifest.main).toBe("./dist/index.js")
    expect(manifest.types).toBe("./dist/index.d.ts")
    expect(manifest.exports).toEqual(manifest.publishConfig.exports)
    expect(manifest.publishConfig.access).toBe("public")
  })
})
