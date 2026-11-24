import { resolve } from "node:path"

/**
 * Type-checks every test file: each package's `tsconfig.test.json`, the
 * integration tests' and this directory's own `tsconfig.json`.
 * `bun run typecheck` compiles the shipped source and leaves the tests out.
 *
 * A package without a `tsconfig.test.json` fails the run, so a new package
 * cannot arrive with tests that nothing compiles.
 */
const root = resolve(import.meta.dir, "..")

const projects = ["integrationtests/tsconfig.test.json", "scripts/tsconfig.json"]
const missing: string[] = []
for await (const manifest of new Bun.Glob("packages/*/package.json").scan(root)) {
  const project = manifest.replace(/package\.json$/, "tsconfig.test.json")
  if (await Bun.file(resolve(root, project)).exists()) projects.push(project)
  else missing.push(project)
}
if (missing.length) throw new Error(`No test tsconfig:\n${missing.sort().join("\n")}`)

const tsc = resolve(root, "node_modules/.bin/tsc")
const results = await Promise.all(
  projects.sort().map(async (project) => {
    const child = Bun.spawn([tsc, "-p", project], { cwd: root, stdout: "pipe", stderr: "pipe" })
    const [output, errors, code] = await Promise.all([
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
      child.exited,
    ])
    return { project, code, output: output + errors }
  }),
)

const failed = results.filter((result) => result.code !== 0)
for (const { project, output } of failed) console.error(`\n${project}\n${output.trimEnd()}`)
if (failed.length) {
  throw new Error(`Test type-check failed:\n${failed.map((result) => result.project).join("\n")}`)
}
console.log(`Type-checked the tests of ${results.length} projects.`)
