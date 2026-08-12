# Spike: functional `createApp` vs the container

Branch `spike/functional-app`. Working code, not a sketch: `packages/app/src/create-app.ts`
(182 lines) plus `packages/app/src/__tests__/billing-both-ways.test.ts`, which builds
the **same billing domain twice** and boots it both ways.

`tsc` clean, `bun run test:unit` → **890 pass / 0 fail** (the 887 on the branch plus 3 new).

## The two composition roots

```ts
// A — container
const app = await kronos({ quiet: true })
  .use(billingContainerModule(store)({ ledger }))
  .start()

// B — functional
const app = createApp({
  components: inMemoryComponents(),
  modules: [billingModule(ledger, store)],
})
```

Both dispatch `OpenBill` then `BillLine` and produce an identical ledger. The
domain code above the roots — events, `state`, both `commandHandler`s — is
byte-identical. Only assembly differs.

## What actually changed, concretely

**1. `Dependencies` stops existing.**

```ts
// A — a framework concept, with a framework-imposed constraint
interface BillingDependencies extends Record<string, unknown> { ledger: Ledger }
m.commandHandler(BillLine, async ({ payload }, ctx) => { ctx.ledger.lines.push(...) })

// B — a closure argument
const billLine = (ledger: Ledger) =>
  commandHandler(BillLine, async ({ payload }, ctx) => { ledger.lines.push(...) })
```

No type parameter, no `extends Record<string, unknown>`, no `m.dependencies`, no
reserved-key collision check, no `ReservedContextKeyError`. The two rounds we
spent naming this concept were spent on something that need not exist.

**2. Handlers become portable values.** In B, `openBill` is a module-level
`const` importable and testable anywhere, and `billLine` is a factory. In A they
must be declared through `m.commandHandler` inside the setup callback; they can
be extracted, but only typed against that module's `Dependencies`. B's handlers
are coupled to nothing.

**3. Per-module event store is a field, not a resolution.**

```ts
// A: m.set("eventStore", store)  → scope copy of resolved root components, override, re-resolve
// B: const eventStore = module.eventStore ?? components.eventStore
```

That `??` is the entire replacement for slot scoping. Which means the whole
scope-inheritance mechanism in PR #19 — `module-scope.ts`, the scoped `built`
copy, the decorator re-application, the scoped config shim — is unnecessary in B.

**4. Boot becomes synchronous.** `createApp` returns an app; there is no
lifecycle to await for an in-memory graph. (Real adapters that must connect
still need a start step — see Open questions.)

## Findings

### The container's job is one `??` and one property access

Everything `slot-registry` (89), `resolved` (54), `decorator` (88), `defaults`
(90), `components` (51), `lifecycle` (33) and `kronos` (132) do — 537 lines
before touching `app.ts`'s 948 — reduces in the functional root to: build a
record, read fields off it, call `??` for module overrides. `createApp` is 182
lines and covers states, commands, queries, processors, per-module stores and
per-module state managers.

### The string-keyed shim is the last container residue, and it should go

`createApp` still builds a `getComponent<T>(type: string): T` shim, because
`commandInvocation` reads its dependencies that way. It is the one place in
the functional root that is an unchecked cast keyed by a string. It exists only
to satisfy the invocation path's current signature — passing a typed record
directly would delete it. **That is the next thing to fix, and it is independent
of this whole debate**: the shim is bad in the container too.

### One module per command name is enforced, and that is a feature

The first version of the two-module test reused billing's commands for the
`ordering` module and failed loudly:

> A different handler is already registered for command "billing.OpenBill".

Worth noting because karma-kronos's N-app model *cannot* catch this: each app has
its own bus, so two modules can each claim the same command name and nobody finds
out. On one shared bus it is a boot-time error. That is an argument for the
one-app model independent of the functional question.

### Extensibility goes up, as predicted

An "extension" in B is a function returning or wrapping a component:

```ts
const commandBus = tracingCommandBus(interceptingCommandBus(rabbitCommandBus(base, cfg)), spans)
```

Anyone can write one without knowing about slots, decorator ordering, or
`ConfigurationEnhancer`. Nothing needs to be registered for it to take effect, and
nothing takes effect that you did not write down.

## Open questions this spike does NOT answer

1. **Lifecycle.** `createApp` is synchronous and only calls `start()` on
   processors. Real adapters (rabbit connect, postgres bootstrap, axon-server
   channel) need ordered start/stop, which is what `lifecycle.ts` and the
   `onStart('connect'|'warmup'|…)` stages do today. The functional answer is
   probably `await createApp(...)` returning `{ start, stop }`, but it is
   unproven here.
2. **Does it stay readable at 19 modules?** This spike has two. The failure mode
   to watch is karma-kronos writing another `module-kit` to hide the assembly —
   which is exactly what it did against the current API. Building the real
   composition root for karma-kronos's 19 modules is the test that decides this.
3. **Decorator ordering as a documented default.** The container guarantees
   framework-defaults-innermost. Hand-written chains hand that back to the
   caller; a recommended stack should be documented rather than discovered.
4. **Processors are wired minimally here** — no DLQ, no error-handler policy, no
   `trackingView`/`trackingAutomation` split. Those are where the durability
   pairing lives and should be folded into the token store regardless of style.

## Assessment

The functional root is smaller, fully explicit, and deletes three framework
concepts (`Dependencies`, module scopes, reserved-key guarding) plus ~537 lines of
container machinery, while making handlers portable and extensions trivial. The
domain layer — which is the actual framework — is untouched.

The honest cost is items 1 and 2 above: lifecycle needs a real answer, and
readability at 19 modules is genuinely unproven. Neither looks like a blocker,
but item 2 is the one that decides whether this is better or merely different,
and it can only be answered by writing karma-kronos's real root.
