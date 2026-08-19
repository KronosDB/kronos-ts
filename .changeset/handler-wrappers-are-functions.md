---
"@kronos-ts/core": minor
"@kronos-ts/postgres": minor
"@kronos-ts/drizzle": minor
"@kronos-ts/knex": minor
"@kronos-ts/kysely": minor
"@kronos-ts/prisma": minor
"@kronos-ts/typeorm": minor
"@kronos-ts/otlp": minor
---

Handler wrappers move from the ENTRY to the FUNCTION. Every NAME is unchanged
from the entry era — `postgresHandler`, `drizzleHandler`, `kyselyHandler`,
`knexHandler`, `prismaHandler`, `typeormHandler`, `otlpHandler`,
`otlpMetricsHandler` — because a shared-package export has to carry its
provenance. What changed is the LEVEL: a wrapper is now a plain generic function
over a plain generic function — `(next, ...config) => (message, ctx) => result`,
with `<M, C, R>` inferred — and the host does the wrapping by spreading the entry
itself.

```ts
// before — the wrapper owned the entry, and needed a type to describe one
kronos({
  commandHandlers: billing
    .map((h) => drizzleHandler(h, db))
    .map((h) => otlpHandler({ ...h, name: "billing" }, exporter)),
})

// after — same names; the wrapper owns the handler, the entry is the host's business
kronos({
  commandHandlers: billing
    .map((h) => ({ ...h, handler: drizzleHandler(otlpHandler(h.handler, exporter), db) })),
})
```

```ts
// before
export function drizzleHandler<D extends DrizzleHandlerEntry>(
  entry: D,
  db: DrizzleDb,
): WithDrizzleSupplied<D>

// after
export function drizzleHandler<M, C extends DrizzleCapability & { readonly unitOfWork: UnitOfWork }, R>(
  next: (message: M, context: C) => R,
  db: DrizzleDb,
): (message: M, context: Omit<C, "db">) => R
```

**Nothing is read off the entry any more.** The wrappers used to reach into
`entry.kind`, `entry.descriptor.name` and the optional `entry.name` label. All
three now come from the MESSAGE, at call time, because that is where they
honestly live:

- `otlpHandler` decides parent-vs-link and SERVER-vs-CONSUMER from
  `message.kind`. No kind argument, no per-kind names, no sentinel.
- the span name and the metric series key default to the message's qualified
  name; `label?: (message: Message) => string` overrides it. A function OF THE
  MESSAGE — never a per-entry string closed over at wiring time.
- `kronos.handler.group` (span) and `handler_group` (metrics) are GONE, and
  `message_type` is now the message's own kind (`"command"`, not
  `"command-handler"`). Dashboards keyed on those attributes need updating.

Because no wrapper depends on an entry, every one of them is pre-appliable —
config bound once, outside the map, and composed by bare name.

**DELETED.** The entry-constraint types existed only to describe the argument
these wrappers no longer take: `DrizzleHandlerEntry`, `WithDrizzleSupplied`,
`PostgresHandlerDefinition`, `Supplied`, `KnexHandlerEntry`, `WithKnexSupplied`,
`KyselyHandlerEntry`, `WithKyselySupplied`, `PrismaHandlerEntry`,
`WithPrismaSupplied`, `TypeormHandlerEntry`, `WithTypeormSupplied`, and
`OtlpHandlerEntry`. The named context types stay — a slice still writes
`ctx: DrizzleContext`, which is the whole point.

**The erasure is directional, and the compiler enforces it.** A wrapper takes a
handler whose ctx has the capability and returns one whose ctx does not, so
wrapping twice — or wrapping a handler that never asked — is a compile error:

```ts
const supplied = drizzleHandler(asksForDb, db)   // (m, ctx: HandlerContext) => …
drizzleHandler(supplied, db)                     // ✗ nothing left to supply
```

Wrappers that supply nothing (`otlpHandler`, `otlpMetricsHandler`) erase nothing
and compose on either side.
`packages/drizzle/src/__tests__/drizzle-handler-inference.types.ts` pins both
directions; it is listed in the root `tsconfig.json` `files` array, so
`bunx tsc --noEmit` judges it.
