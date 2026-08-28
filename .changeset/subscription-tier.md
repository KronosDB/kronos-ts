---
"@kronos-ts/core": minor
"@kronos-ts/kronosdb": minor
"@kronos-ts/axon-server": minor
"@kronos-ts/rabbitmq": minor
"@kronos-ts/otlp": minor
"@kronos-ts/test": minor
---

Live updates are the third capability tier — the first on a bus. The base
`QueryBus` shrinks to two members; the subscription surface moves to
`SubscriptionCapability`, and `ctx.emitUpdate` exists only against a bus that
claims it. BREAKING.

```ts
// before — every QueryBus implementer carried seven members
type QueryBus<U> = { query; subscribe; subscriptionQuery; subscribeToUpdates;
                     emitUpdate; completeSubscription; completeSubscriptionExceptionally }

// after — the seam is two; the tier is claimed, never implied
type QueryBus<U> = { query; subscribe }
type SubscriptionCapableQueryBus<U> = QueryBus<U> & SubscriptionCapability
```

Same construction as the two store tiers: `IfSubscriptionCapable<Q, …, …>` is
the anchor, `SubscriptionEmit<Q>` derives the context face, and the contexts
take the bus beside the log — `EventHandlerContext<E, Q, U>` /
`CommandHandlerContext<E, Q, U>`, each parameter defaulted so plain code never
writes any of them.

```ts
// a projection that pushes live updates says so — and an entry whose bus
// cannot serve them refuses it at compile time
eventHandler(Enrolled, async (m, ctx) => {
  ctx.emitUpdate(Watch, …)        // ✗ property does not exist
})
eventHandler(Enrolled, async (m, ctx: EventHandlerContext & EmitCapability) => {
  ctx.emitUpdate(Watch, …)        // ✓ and the entry's queryBus must claim the tier
})
```

A handler demands the tier by intersecting `EmitCapability` — one name for the
one thing it uses. The type parameters are the SUPPLY side (an entry threads
its bus in, and `Q` is inferred from the bus the entry names, so hosts write no
type arguments on either side); intersecting is the DEMAND side, exactly as the
persistence packages' `DrizzleCapability` / `PostgresCapability` are written.

The `subscriptionQuery` edge verb demands `SubscriptionCapableQueryBus`.
`localQueryBus` offers the tier natively; the kronosdb, axon-server and
rabbitmq buses offer it server- or broker-mediated; `interceptingQueryBus`,
`otlpQueryBus` and `recordingQueryBus` preserve whatever tier the wrapped bus
carried (`B` in, `B` out) instead of naming the members.

Interception wraps the tier where it exists: `subscriptionQuery` /
`subscribeToUpdates` run the same intercept the primary `query` runs, so
subscription queries travel correlated across transports — the KNOWN-GAP
comments in kronosdb/axon-server described an older core and are retired,
pinned by a test.
