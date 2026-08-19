# @kronos-ts/otlp

## 0.2.0

### Minor Changes

- 4d7b0ee: Handler wrappers move from the ENTRY to the FUNCTION. Every NAME is unchanged
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
  });

  // after — same names; the wrapper owns the handler, the entry is the host's business
  kronos({
    commandHandlers: billing.map((h) => ({
      ...h,
      handler: drizzleHandler(otlpHandler(h.handler, exporter), db),
    })),
  });
  ```

  ```ts
  // before
  export function drizzleHandler<D extends DrizzleHandlerEntry>(
    entry: D,
    db: DrizzleDb
  ): WithDrizzleSupplied<D>;

  // after
  export function drizzleHandler<
    M,
    C extends DrizzleCapability & { readonly unitOfWork: UnitOfWork },
    R
  >(
    next: (message: M, context: C) => R,
    db: DrizzleDb
  ): (message: M, context: Omit<C, "db">) => R;
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
  const supplied = drizzleHandler(asksForDb, db); // (m, ctx: HandlerContext) => …
  drizzleHandler(supplied, db); // ✗ nothing left to supply
  ```

  Wrappers that supply nothing (`otlpHandler`, `otlpMetricsHandler`) erase nothing
  and compose on either side.
  `packages/drizzle/src/__tests__/drizzle-handler-inference.types.ts` pins both
  directions; it is listed in the root `tsconfig.json` `files` array, so
  `bunx tsc --noEmit` judges it.

- 4d7b0ee: `@kronos-ts/opentelemetry` is REMOVED, replaced by `@kronos-ts/otlp`: the protocol, not the ecosystem.

  The old package took the OpenTelemetry API as a peer and an SDK pair as dev
  dependencies, and reached core through a pair of seams core had to carry for its
  benefit — `SpanFactory` and `MetricsRecorder`, plus `tracingHandler` and
  `meteringHandler`. Those seams are DELETED from core, which now contains ZERO
  tracing vocabulary. Observability is a package of functions over the public
  shapes, which anybody could have written — so it is one.

  `@kronos-ts/otlp` speaks OTLP/JSON over `fetch` and depends on
  `@kronos-ts/core` and nothing else. No SDK, no global tracer, no patching, and
  no OpenTelemetry dependency anywhere in the tree.

  ```ts
  const exporter = otlpExporter({
    endpoint: "http://collector:4318",
    serviceName: "billing",
  });

  const commandBus = otlpCommandBus(
    interceptingCommandBus(bus, lineage),
    exporter
  );
  const handlers = slice.commandHandlers.map((h) => ({
    ...h,
    handler: otlpHandler(h.handler, exporter),
  }));
  ```

  - `otlpExporter({ endpoint, serviceName, flushIntervalMs? })` — a resource:
    batches spans and metrics, flushes on an interval, POSTs to `/v1/traces` and
    `/v1/metrics`, and `close()` flushes then stops. W3C trace and span ids are
    generated here; 64-bit nanosecond times are encoded as strings, as OTLP/JSON
    requires.
  - `otlpCommandBus(bus, exporter)` / `otlpQueryBus(bus, exporter)` — a span per
    dispatch, with `traceparent` injected into the outgoing message metadata.
  - `otlpHandler(handler, exporter, label?)` — wraps the handler FUNCTION and
    extracts `traceparent` from the handled message. Command and query MESSAGES
    become CHILDREN of the extracted context; event messages get their own trace
    with a LINK back to the producing span, so a projection catching up over a
    batch of old events is not swallowed into whatever produced them. Which leg it
    is comes from `message.kind` — there is no kind argument and no entry to ask.
  - `otlpMetricsHandler(handler, exporter, label?)` — duration, throughput and failure
    counters, sliced by `message_type` and `message_name`, both read off the
    message.
  - `label` ABSENT names the span (and keys the series) by the message's qualified
    name. Pass a `(message: Message) => string` to name it otherwise — a function
    OF THE MESSAGE, never a per-handler string closed over at wiring time.

  otel-js interop is a consumer concern: write wrappers over the same public
  shapes. That was always the honest boundary, and pretending otherwise cost core
  two seams.

### Patch Changes

- 4d7b0ee: The `extensions/` directory is gone, and so is the concept.

  ```
  packages/{core,test,rabbitmq,kronosdb,axon-server,postgres,drizzle,knex,kysely,prisma,typeorm,otlp}
  ```

  An "extension" implied a plugin contract that this framework does not have and
  does not want: every one of these is a package of ordinary functions over the
  public core shapes, no more privileged than something you write yourself. Nested
  under `extensions/` they read as a second tier, which made "should this be core
  or an extension?" a question anybody could ask about anything.

  Published package names are unchanged; only repository paths, the workspace
  globs, the tsconfig include and the CI globs moved.

- Updated dependencies [4d7b0ee]
- Updated dependencies [4d7b0ee]
- Updated dependencies [4d7b0ee]
- Updated dependencies [4d7b0ee]
- Updated dependencies [4d7b0ee]
- Updated dependencies [4d7b0ee]
- Updated dependencies [4d7b0ee]
- Updated dependencies [4d7b0ee]
- Updated dependencies [4d7b0ee]
- Updated dependencies [4d7b0ee]
- Updated dependencies [4d7b0ee]
- Updated dependencies [4d7b0ee]
- Updated dependencies [4d7b0ee]
- Updated dependencies [4d7b0ee]
  - @kronos-ts/core@0.2.0
