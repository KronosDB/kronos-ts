# Spike: module-scoped identity (consumer groups)

Branch `spike/module-identity`, on top of `feat/module-composition`. **Not for merge.**

## Goal

Let a *service* own one kronos instance with universal config (RabbitMQ, OTel,
correlation) and hook each *module* underneath it with its own database and
event store — replacing karma-kronos's current "N kronos instances + one
handler-less edge app".

The blocker was never the event store (module scopes already override that
slot). It is **identity**: `commandQueue()` is derived from `app.identity.serviceName`,
which is exactly why karma-kronos boots one app per module.

## What was built

`subscribe()` gains an optional third argument:

```ts
export interface SubscribeOptions {
  readonly group?: string
}
subscribe(commandName, handler, options?: SubscribeOptions): void
```

Module scopes pass their name as the group; the topology keys the queue on the
group when present and falls back to `serviceName` when absent, so existing
deployments keep their queue names.

```ts
// before — queue identity is the hosting service
commandQueue(name) => `${prefix}.commands.${service}.${name}`
// after — queue identity is the module, when it declares one
commandQueue(name, group) => `${prefix}.commands.${group ?? service}.${name}`
```

Result: `tsc` clean, **895 pass / 0 fail**, 13 files, +55/-22.

## Finding 1 — the optional parameter compiled with ZERO errors, and was silently dropped

This is the headline, and it is a reason to *not* ship the shape as-is.

Adding an optional parameter is source-compatible, so **every existing bus still
satisfied the interface** and the compiler reported nothing. But every decorator
in the default pipeline forwarded only two arguments:

```ts
// intercepting-command-bus.ts    delegate.subscribe(commandName, wrappedHandler)
// tracing-command-bus.ts         delegate.subscribe(commandName, handler)
// retrying-command-bus.ts        delegate.subscribe(commandName, handler)
// intercepting-query-bus.ts      delegate.subscribe(queryName, wrappedHandler)
```

The app builds `tracing(intercepting(base))`, so the group was discarded **twice**
before reaching the transport. Everything typechecked; every test passed; the
feature did nothing. On a distributed transport the failure mode is not a crash —
it is two modules silently sharing one queue, or (when a subset of modules is
deployed under a different service name) **the same command being handled twice**.

Fixed here by threading `options` through all four decorators, and pinned by
`packages/messaging/src/__tests__/subscribe-group.test.ts`, which asserts the
group survives a stacked chain.

**Implication:** if this ships, the parameter should be *required* internally
(or the decorators generated), because "forwards subscribe correctly" is not
something the type system checks and not something a reviewer reliably notices.

## Finding 2 — three distributed transports silently ignore the group

Still unimplemented after the spike, and each fails silently:

| Transport | Consequence |
|---|---|
| `extensions/axon-server/src/axon-server.ts` | modules collapse onto one identity |
| `extensions/kronosdb/src/kronosdb.ts` | same |
| `extensions/rabbitmq/src/query-bus.ts` | **commands are per-module, queries are not** |

The rabbit query-bus one is the nastiest: within a single transport you would get
per-module command queues and shared query queues. In-memory buses
(`simple-command-bus`, `simple-query-bus`) ignoring the group is *correct* — they
have no queues — which is precisely why "ignores it" cannot be used as a defect
signal. It has to be tracked per implementation.

## Finding 3 — the group must be persisted per subscription, not just passed

`amqp-command-transport` keeps `handlers: Map<name, handler>` and re-binds on
reconnect. Passing the group through `subscribe()` alone leaves reconnect
re-binding the *wrong* queue. The spike adds a parallel `groups: Map<name, group>`.
Any transport that re-binds needs the same treatment — a real, easy-to-miss
requirement for third-party transports.

## Finding 4 — the payoff is real, and includes something today's design lacks

`topology-group.test.ts` pins two properties:

- two modules in one service get **distinct** queues for the same command name
- the same module gets the **same** queue regardless of hosting service

The second is strictly better than today. karma-kronos gets stable module queues
only because `serviceName === module name`; moving a module into a different
service would rename its queues. With module-scoped identity, relocation is
genuinely a deployment decision.

## Assessment

The shape works and the payoff is real, but the ergonomics are wrong in a way
that matters for a framework: **an optional, hand-forwarded parameter through a
decorator chain is a silent-failure machine**, and this spike demonstrated that
rather than argued it — it compiled clean and did nothing.

Options if this moves forward:

1. **Make it non-optional internally.** Decorators take and forward `options`
   explicitly; the compiler then catches a missed forward. Public callers keep
   an optional overload.
2. **Move identity off `subscribe()` entirely** — give `ModuleScope` an
   `identity`, and have extensions build a per-scope transport. Heavier (the
   extension becomes scope-aware) but no per-call threading and no silent drop.
3. **Do nothing in kronos; keep N apps.** karma-kronos's current design is
   coherent and already works. The cost is N slot registries, N decorator
   pipelines and N broker connections per process — real but bounded.

My recommendation is (2) for the eventual shape, with (1) as the migration step
if per-module queues are wanted sooner. What should *not* ship is the spike
exactly as written: optional, hand-forwarded, and unverifiable by the compiler.

## Not covered

- Event bus / subscription-query gossip queues (`subscribersDirectQueue` keys on
  `service.instance` and was not touched)
- Axon Server and KronosDB transports
- Any integration test against a live broker — all evidence here is unit-level
