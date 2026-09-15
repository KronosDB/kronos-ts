import { runTestSuites } from "./run-test-suites.js"

/** Run container suites in separate Bun processes: avoid cross-container HTTP/2 state reuse. */
const suites = [
  "packages/rabbitmq/src/__tests__/rabbitmq-command.integration.test.ts",
  "packages/rabbitmq/src/__tests__/rabbitmq-query.integration.test.ts",
  "packages/rabbitmq/src/__tests__/nested-messaging.integration.test.ts",
  "packages/axon-server/src/__tests__/axon-server.integration.test.ts",
  "integrationtests/src/__tests__/e2e-kronosdb.integration.test.ts",
  "integrationtests/src/__tests__/bus-topology-kronosdb.integration.test.ts",
  "integrationtests/src/__tests__/e2e-inmemory.integration.test.ts",
  "integrationtests/src/__tests__/transactional-commands.integration.test.ts",
]
await runTestSuites(suites)
