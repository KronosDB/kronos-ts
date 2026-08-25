---
"@kronos-ts/core": patch
---

The examples are compiled by the same gate as the code, and they had drifted.

`integrationtests/examples` was in no tsconfig, so nothing typechecked it —
three renames went past it and it accumulated eight errors while every gate
stayed green. It is in the root `tsconfig.json` `include` now, and the drift is
fixed: the base `postgresEventStore` takes no `serializer` (encoding belongs to
the wrapper that owns a payload), the decisions that load a snapshotting state
declare `CommandHandlerContext<SnapshotCapableEventStore>`, and the bus helper
is generic in `U` instead of laundering the composed task away.

The university example is also recomposed the way the surface intends: no
`buildProjector(db)` factory, no bus bundle, no `any`-typed `carrying()`
wrapper. Handlers are plain top-level values; the composition root names each
store, bus, task factory and processor once and writes the five entries out.
Projections write through drizzle bound to the task's own transaction
(`postgresTransaction(ctx.unitOfWork).unwrap()`), with `postgresTokenStore` in
the same family — so a projection write and the token update that records it
commit together.

Both examples also ran to completion and did nothing: `main().catch(…)` leaves
a floating promise that does not hold Bun's event loop open across
testcontainers' docker calls, so they printed one line and exited 0. Both use
top-level await now.
