---
"@kronos-ts/core": minor
---

Explicit components, and plain functions for `TagResolver` / `HandlerEnhancer`.

**`kronos({ components })` is now required and must be complete.** There are no
implicit defaults: what you pass is what runs. `inMemoryComponents(overrides)` is
the explicit opt-in to in-memory fallbacks, and its `overrides` argument is the
ordering-safe position — it resolves AFTER the merge, so a supplied
`unitOfWorkFactory` is the one the command bus captures.

```ts
// before — kronos silently filled the gaps
kronos({ modules })
kronos({ components: { eventStore, snapshotStore }, modules })

// after — the defaults are something you ask for
kronos({ components: inMemoryComponents(), modules })
kronos({ components: inMemoryComponents({ eventStore, snapshotStore }), modules })
```

**`TagResolver` is a bare function type.** `descriptorBasedTagResolver`,
`metadataBasedTagResolver` and `multiTagResolver` return plain functions.

```ts
// before
interface TagResolver { resolve(event: EventMessage): Tag[] }
const tags = tagResolver.resolve(event)

// after
type TagResolver = (event: EventMessage) => Tag[]
const tags = tagResolver(event)
```

**`HandlerEnhancerDefinition` is now `HandlerEnhancer`, a bare function type.**
`multiHandlerEnhancerDefinition`, `tracingHandlerEnhancerDefinition`,
`meteringHandlerEnhancerDefinition` and `openTelemetry().handlerEnhancer` all
return the function directly rather than a `{ wrapHandler }` wrapper.

```ts
// before
const enhancer: HandlerEnhancerDefinition = { wrapHandler(handler, metadata) { … } }
const wrapped = enhancer.wrapHandler(handler, metadata)

// after
const enhancer: HandlerEnhancer = (handler, metadata) => { … }
const wrapped = enhancer(handler, metadata)
```
