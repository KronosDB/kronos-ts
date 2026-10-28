// The native client under measurement: `bun native.ts --port 50051 --concurrency 16`,
// or bundled for Node: `bun build native.ts --target node --outfile native.mjs`.
import { kronosDbConnection, kronosDbEventStore } from "@kronos-ts/kronosdb"
import { main } from "./harness.js"

await main("native", async (host, port, serializer) => {
  const kdb = await kronosDbConnection({ componentName: "client-bench", host, port, serializer })
  return { store: kronosDbEventStore(kdb, "default"), close: () => kdb.close() }
})
