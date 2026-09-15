import { resolve } from "node:path"

/** Separate processes keep closed HTTP/2 sessions and container ports out of subsequent suites. */
export async function runTestSuites(suites: readonly string[]): Promise<void> {
  const failed: string[] = []
  for (const suite of suites) {
    console.log(`\nRunning ${suite}`)
    const child = Bun.spawn([process.execPath, "test", suite], {
      stdout: "inherit",
      stderr: "inherit",
    })
    if ((await child.exited) !== 0) failed.push(suite)
  }
  if (failed.length) throw new Error(`Failed suites:\n${failed.join("\n")}`)
  console.log(`All ${suites.length} test suites passed.`)
}

if (import.meta.main) {
  const roots = process.argv.slice(2)
  if (!roots.length) throw new Error("Provide at least one test directory")
  const suites: string[] = []
  for (const root of roots) {
    for await (const file of new Bun.Glob("**/*.test.ts").scan(root))
      suites.push(resolve(root, file))
  }
  if (!suites.length) throw new Error("No test suites found")
  await runTestSuites([...new Set(suites)].sort())
}
