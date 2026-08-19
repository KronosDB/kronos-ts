---
"@kronos-ts/core": minor
"@kronos-ts/rabbitmq": minor
"@kronos-ts/kronosdb": minor
"@kronos-ts/axon-server": minor
"@kronos-ts/test": minor
---

kronos() flattened; interceptors and correlation are wrapper functions.

**`kronos()` takes four named things, not a record.** `Components`,
`inMemoryComponents` and `resolveComponents` are DELETED. Stores are module-only:
an event store is the system of record for a bounded slice, and an app-level one
only ever meant "whichever module forgot to say".

```ts
// before — a record, with defaults filled in behind you
kronos({ components: inMemoryComponents({ eventStore }), modules: [module("billing", ...slices)] })

// after — the buses are yours to build and wrap
kronos({
  commandBus: correlatingCommandBus(simpleCommandBus(unitOfWork)),
  queryBus: correlatingQueryBus(simpleQueryBus(unitOfWork)),
  modules: [module("billing", { eventStore }, ...slices)],
})
```

`module(name, stores, ...items)` required a stores record naming an
`eventStore`. (Both the module wrapper and the record are deleted outright by
the flat-fields change below; what survives is that persistence is attached by
the host, per item, at composition.) Per-state `[state, options]` tuples are
unchanged.

The UoW-capture trap doc moved onto `simpleCommandBus`, where the capture
actually happens: the runner you hand the bus and the one you hand `kronos` have
to be the same value, and writing them on adjacent lines is what makes that
checkable.

**Interceptors are wrapper functions.** `interceptingCommandBus`,
`interceptingQueryBus`, `interceptingEventBus`, `interceptingEventStore`,
`DispatchInterceptor`, `HandlerInterceptor` and both
`registerDispatchInterceptor` / `registerHandlerInterceptor` are DELETED.

```ts
// before — a registry, and two far-apart call sites fighting over order
const bus = interceptingCommandBus(routing)
bus.registerDispatchInterceptor(correlationDataDispatchInterceptor())

// after — order is argument order
const bus = correlatingCommandBus(routing, (m) => metadataAnd(m, "tenantId", tenant))
```

**Providers are functions, and there are two kinds.** A `MetadataProvider` is a
plain `(metadata: Metadata) => Metadata` handed to `correlatingCommandBus` /
`correlatingQueryBus` — it runs per DISPATCH and sees only metadata. A
`CorrelationDataProvider` is `(message: Message) => Record<string, string>`,
runs ONCE per handler INVOCATION, and can see the incoming message's identifier
— which is what `causationId` is. Neither has a factory any more:
`messageOriginProvider`, `simpleCorrelationDataProvider`, `messageOrigin()`,
`copyMetadataKeys()` and `defaultCorrelationDataProviders` are all DELETED. A
host writes the rule, because the rule is two lines and reads better than a name
for it:

```ts
// before — a framework factory, and a registry to hand it to
app.correlationDataProvider(messageOriginProvider())

// after — a function the host names, passed where the invocation is built
const lineage: CorrelationDataProvider = (m) => ({
  correlationId: String(m.metadata.correlationId ?? m.identifier),
  causationId: m.identifier,
})
trackingEventProcessor({ ...opts, correlationDataProviders: [lineage, actor] })
```

`correlationDataProviders` is gone from `kronos()`. It lives on the CONSTRUCTION
SITE that builds the invocation wrapper — `subscribeCommandHandlers`,
`subscribeQueryHandlers`, and all three event processors — defaulting to lineage
alone. Note the asymmetry that makes the seam necessary: a command handler's
unit of work is opened from `message.metadata`, so the whole inbound metadata map
already rides forward through `ctx.send` / `ctx.append` with no provider at all;
a processor's batch unit of work is opened from `emptyMetadata()`, so a host key
crosses the automation boundary only if a provider extracted it.

**One optional enhancer function.** `kronos({ enhance })` replaces
`handlerEnhancer`. `multiHandlerEnhancerDefinition` is DELETED — two enhancers
compose the way two functions compose, and writing it out says the nesting order
that an array argument only implied.

```ts
// before
handlerEnhancer: multiHandlerEnhancerDefinition([tracing, metering])

// after
enhance: (handler, info) => tracing(metering(handler, info), info)
```

`tracingHandlerEnhancerDefinition` → `tracingEnhancer`,
`meteringHandlerEnhancerDefinition` → `meteringEnhancer`.
