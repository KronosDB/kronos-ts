// Postgres and KronosDB tests need a server. CI sets these; locally a test without its server is skipped,
// unless KRONOS_NODE_TESTS_REQUIRE_SERVICES is set, which turns a missing server into a failure.
const required = process.env.KRONOS_NODE_TESTS_REQUIRE_SERVICES === "1"

/** The `skip` option for a test that needs `variable`. */
export function needs(variable) {
  if (process.env[variable]) return false
  if (required) throw new Error(`${variable} is not set, and KRONOS_NODE_TESTS_REQUIRE_SERVICES requires it`)
  return `${variable} is not set`
}
