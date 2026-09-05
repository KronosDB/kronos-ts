---
"@kronos-ts/core": minor
"@kronos-ts/drizzle": minor
"@kronos-ts/knex": minor
"@kronos-ts/kysely": minor
"@kronos-ts/prisma": minor
"@kronos-ts/postgres": minor
"@kronos-ts/kronosdb": minor
"@kronos-ts/axon-server": minor
"@kronos-ts/test": minor
---

A context capability exists only when a handler has something new to call. BREAKING renames and deletions.

**Snapshotting is a store tier with no context type.** `SnapshotReads`, `SnapshotDemand`, `IfSnapshotCapable`, `FusedSourceFunction` and `SnapshottedSource` are gone. `ctx.source` has one signature everywhere, `ctx.load` accepts any state against any context, and a snapshot-policy state loaded through a bare log throws at runtime on the first load (`capableOrThrow`). Wire `<family>SnapshottingEventStore` underneath and declare `state({ snapshot })`; no handler names the tier.

**One naming rule.** `<Tier>Capability` is what a handler intersects on its context. `<Tier>StoreCapability` / `<Tier>BusCapability` is what a wrapper adds to a store or bus. `<Tier>Capable<Thing>` aliases for composition roots are unchanged.

| before | after |
| --- | --- |
| `EmitCapability` (ctx) | `SubscriptionCapability` |
| `ScheduleFunctions` (ctx) | `ScheduleCapability` |
| `SubscriptionCapability` (bus) | `SubscriptionBusCapability` |
| `ScheduleCapability` (store) | `ScheduleStoreCapability` |
| `SnapshotCapability` (store) | `SnapshotStoreCapability` |
| `ScheduleVerbs<E>` / `SubscriptionEmit<Q>` | `SuppliedScheduleCapability<E>` / `SuppliedSubscriptionCapability<Q>` |

**The per-package `<Pkg>CommandContext` / `<Pkg>EventContext` / `<Pkg>QueryContext` aliases are deleted** (drizzle, knex, kysely, prisma, postgres). A host names its context once:

```ts
// before
commandHandler(Edit, async (m, ctx: CommandHandlerContext<SnapshotCapableEventStore & ScheduleCapableEventStore> & EmitCapability & DrizzleCapability) => …)
commandHandler(Edit, async (m, ctx: DrizzleCommandContext) => …)

// after — one contexts file, no type parameters in slice code
type CmdCtx = CommandHandlerContext & ScheduleCapability & SubscriptionCapability & DrizzleCapability
commandHandler(Edit, async (m, ctx: CmdCtx) => …)
```
