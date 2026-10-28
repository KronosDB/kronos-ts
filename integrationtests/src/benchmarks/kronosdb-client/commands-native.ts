// The native client under measurement:
//   bun commands-native.ts --role handler --port 50051
//   bun commands-native.ts --role dispatcher --port 50051 --concurrency 16
// or bundled for Node: `bun build commands-native.ts --target node --outfile commands-native.mjs`.
import { kronosDbCommandBus, kronosDbConnection } from "@kronos-ts/kronosdb"
import { localBus, main } from "./command-harness.js"

await main("native", async (host, port, serializer, role) => {
  const kdb = await kronosDbConnection({ componentName: `command-bench-${role}`, host, port, serializer })
  const bus = kronosDbCommandBus(localBus(), kdb)
  return { bus, ready: () => kdb.start(), close: () => kdb.close() }
})
