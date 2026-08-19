---
"@kronos-ts/core": patch
---

`lineage` seeds roots instead of clobbering every hop's cause.

`causationId` was stamped unconditionally as the dispatched message's own
identifier. That is right for a message born at an edge and wrong for every
other message in the system: `ctx.send` / `ctx.query` / `ctx.append` already
stamp the handled message's identifier onto everything a handler emits — the
TRUE cause — and the bus edge then overwrote it. Every message ended up claiming
to have caused itself, so the causal graph was a set of self-loops and no
multi-hop chain could be reconstructed.

```ts
// before
causationId: message.identifier

// after
causationId: String(message.metadata.causationId ?? message.identifier)
```

Both fields are `??` seeds now, which also makes double application a true
no-op — a transport bus may wrap a local segment that is itself intercepting.
`correlationId` is unchanged in behaviour.

Tests that encoded the old behaviour are updated, including the real-broker
RabbitMQ one: a command sent from a handler across the wire now arrives with the
OUTER command's identifier as its cause.
