---
"@kronos-ts/core": minor
---

Validation is a mechanism you compose, and it needs no registry — the descriptor is already in your hand at every site that validates.

`validation/` joins `interception/`, `correlation/` and `upcasting/` as the fourth mechanism, and like them it is functions you wrap in rather than infrastructure you configure. It is two exports:

```ts
validate(descriptor, payload)          // the primitive — anywhere a descriptor is in hand
validatingHandler(next, descriptor)    // the mechanism — inbound, and every ctx birth
```

**Why there is nothing to register.** A descriptor carries its own payload schema, and it is an ARGUMENT at every face except the one where it is born: the edge verbs take one (`send(bus, descriptor, payload)`), the birth verbs take one (`ctx.append(descriptor, payload)`), an entry pairs one with its handler. A schema registry exists to answer "which schema goes with this type name" — a question that only arises somewhere holding a name and not a descriptor, which was the serializer and nowhere else. So the question stopped being asked.

**`validatingHandler` composes at the ENTRY**, the one place a descriptor and a handler already sit together:

```ts
kronos({
  commandHandlers: handlers
    .map((h) => ({ ...h, handler: validatingHandler(h.handler, h.descriptor) }))
    .map((h) => ({ ...h, commandBus, queryBus, eventStore })),
})
```

Wrapped, a handling is gated in both directions. INBOUND, the message's payload is checked against the entry's own descriptor — a message off a wire is a claim, not a fact, and a transport that decoded JSON knows the shape parsed, not that it is a command this handler can act on. OUTBOUND, the context's birth verbs are overlaid exactly the way `correlatingHandler` overlays them (only the verbs the context has, so one wrapper serves all three kinds), and each verb validates against the descriptor IT was called with — `append`'s batch form per tuple.

The write side is the one that matters most:

```ts
// before — the log's guard was a registry entry somebody had to remember to add
validatingSerializer(jsonSerializer(), registry)   // …and registry.register("Charged", "1.0", schema)

// after — the guard is the descriptor the verb was called with
ctx.append(Charged, { accountId })    // ← missing `amount`: throws HERE, before the store
                                      //   sees it, not in a replay years from now
```

**The parsed value replaces the input, on both paths.** Standard validation is a parse — coercions, defaults and transforms are part of what a schema says — so `next` is handed a message whose payload is what the schema produced, and each wrapped verb gives birth to the produced value rather than the one the caller passed. Dropping that value would keep the check and throw away half of it.

The sync/async split falls out of the verbs. `ctx.send` and `ctx.query` already answer promises, so a schema that validates asynchronously is simply awaited. `ctx.append` returns `void` and `ctx.schedule` builds its message in the caller's turn, so an async schema there throws, naming the message type and the verb — the same rule (and the same reason) the serializer had, one boundary over. Validation asks the context for no type it did not already have, so unlike `correlatingHandler` it adds no demand: a wrapped handler wires against exactly the buses the unwrapped one did, and it stacks with `correlatingHandler` and `otlpHandler` in either order.

**And the edge is yours.** The edge verbs are unclosed on purpose, and a host closes them out once — documented, never exported, because it is two lines and they are the host's two lines:

```ts
export const dispatch = (d, payload, actor) => send(commandBus, d, validate(d, payload), { actor })
export const ask      = (d, payload, actor) => query(queryBus,  d, validate(d, payload), { actor })
```

Controllers never see the unclosed verb, so validation and per-request metadata are one visible decision in one file.

**BREAKING — `validatingSerializer` is gone, and so is the whole registry apparatus.** `SchemaRegistry`, the `.register()` ceremony, and `eventSchemaRegistry()` / `commandSchemaRegistry()` / `querySchemaRegistry()` are deleted from `messaging/serialization/` and from the barrel. A serializer ENCODES; `jsonSerializer()` is what is left, and it does one thing.

```ts
// before
const registry = eventSchemaRegistry()
registry.register("Charged", "1.0", ChargedSchema)
const serializer = validatingSerializer(jsonSerializer(), registry)
postgresEventStore(pg, { serializer, tagResolver })

// after
postgresEventStore(pg, { serializer: jsonSerializer(), tagResolver })
kronos({ commandHandlers: handlers.map((h) => ({ ...h, handler: validatingHandler(h.handler, h.descriptor) })) })
```

**BREAKING — `MessageDescriptor` is widened to `CommandDescriptor<any, any> | EventDescriptor<any> | QueryDescriptor<any, any>`.** It was the defaulted spelling, and a descriptor CARRIES A FUNCTION: an event's `tags` extractor takes the payload, so it is checked contravariantly and `EventDescriptor<{ courseId: string }>` could not stand in for `EventDescriptor<StandardSchemaV1>` — nor could a descriptor with a `result` schema stand in for the `undefined` default. Between them that ruled out every descriptor anybody actually declares, which `is()` had been quietly carrying:

```ts
// before — did not compile, for any event with tags or any command with a result
is(message, CourseCreated)

// after — compiles, and narrows to the exact payload
if (is(message, CourseCreated)) message.payload.courseId
```

The widening is the CONSTRAINT, never the value: every function taking a descriptor takes it as `D extends MessageDescriptor` and reads the real payload type back off `D`, so `validate(CreateCourse, body)` returns the exact parsed object and `is()` narrows exactly as it always promised to.
