// Copies the native protocol's generated TypeScript codec, its schema and its
// golden fixtures out of a KronosDB checkout into this package.
//
//   bun packages/kronosdb/scripts/vendor-protocol.ts ../kronosdb
//
// The codec is generated from `protocol/kronosdb.schema` in the server
// repository and is never edited here. After copying, update
// `PINNED_SCHEMA_HASH` in `src/protocol/__tests__/conformance.test.ts` to the
// hash this script prints: that one-line change is the review point for "the
// wire changed", and the conformance suite proves the new codec matches the
// new fixtures byte for byte.

import { cpSync, existsSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs"
import { dirname, join, resolve } from "node:path"
import { fileURLToPath } from "node:url"

const checkout = process.argv[2]
if (!checkout) {
  console.error("usage: bun scripts/vendor-protocol.ts <path to a kronosdb checkout>")
  process.exit(2)
}

const source = resolve(checkout, "protocol")
for (const needed of ["kronosdb.schema", "ts/kronosdb-protocol.ts", "ts/kronosdb-protocol-json.ts", "fixtures"]) {
  if (!existsSync(join(source, needed))) {
    console.error(`${join(source, needed)} does not exist; is ${checkout} a KronosDB checkout with the native protocol?`)
    process.exit(1)
  }
}

const here = dirname(fileURLToPath(import.meta.url))
const target = resolve(here, "..", "src", "protocol")
const tests = join(target, "__tests__")
const fixtures = join(tests, "fixtures")

mkdirSync(tests, { recursive: true })
cpSync(join(source, "ts", "kronosdb-protocol.ts"), join(target, "kronosdb-protocol.ts"))
cpSync(join(source, "kronosdb.schema"), join(target, "kronosdb.schema"))

// The JSON bridge is a test tool: it maps frames to the fixtures' JSON form.
// Its import of the codec is rewritten from the server repository's `.ts`
// specifier to this package's `.js` one.
const bridge = readFileSync(join(source, "ts", "kronosdb-protocol-json.ts"), "utf8").replaceAll(
  /from "\.\/kronosdb-protocol\.ts"/g,
  'from "../kronosdb-protocol.js"',
)
writeFileSync(join(tests, "kronosdb-protocol-json.ts"), bridge)

// Fixtures: the golden frames and their canonical JSON, not the hand-written sources.
rmSync(fixtures, { recursive: true, force: true })
mkdirSync(fixtures, { recursive: true })
let copied = 0
for (const name of readdirSync(join(source, "fixtures"))) {
  if (name.startsWith(".") || !(name.endsWith(".bin") || name.endsWith(".json"))) continue
  cpSync(join(source, "fixtures", name), join(fixtures, name))
  copied++
}

const hash = /SCHEMA_HASH = "([0-9a-f]+)"/.exec(readFileSync(join(target, "kronosdb-protocol.ts"), "utf8"))?.[1]
console.log(`vendored the codec, the schema and ${copied} fixture files from ${source}`)
console.log(`SCHEMA_HASH is ${hash}; pin it in src/protocol/__tests__/conformance.test.ts`)
