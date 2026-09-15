import { resolve } from "node:path"
import { runTestSuites } from "./run-test-suites.js"

const suites: string[] = []
for (const root of ["packages", "integrationtests"]) {
  for await (const file of new Bun.Glob("**/src/**/*.integration.test.ts").scan(root)) {
    suites.push(resolve(root, file))
  }
}
if (!suites.length) throw new Error("No integration suites found")
await runTestSuites(suites.sort())
