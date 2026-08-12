# What the functional style looks like for karma-kronos

Concrete sketch against the real repo: 19 modules, per-module Postgres, RabbitMQ,
OTel, drizzle migrations, oRPC over HTTP + WS. Functional is the **only** style —
`kronos()`, slots, decorators and extensions are gone, not deprecated.

---

## 1. A slice becomes a record, not a callback

```ts
// today — a slice is a side effect against a hidden app, plus a bolted-on rpc field
export const slice = defineSlice({
  configure: (app: App) => {
    app.states(Bill)
    app.commands(commandHandler(BillLines, async ({ payload }) => {
      const bill = await load(Bill, { billId: payload.billId })   // ambient
      append(LineBilled, { ... })                                  // ambient
    }))
  },
  rpc: { billing: { billLines: impl.billing.billLines.handler(...) } },
})

// functional — a slice DECLARES what it contributes; deps are arguments
export const billLines = (deps: BillingDeps) => ({
  states: [Bill],
  commands: [
    commandHandler(BillLines, async ({ payload }, ctx) => {
      const bill = await ctx.load(Bill, { billId: payload.billId })
      await ctx.send(PriceLines, { ... })
      ctx.append(LineBilled, { ... })
    }),
  ],
  rpc: { billing: { billLines: impl.billing.billLines.handler(...) } },
})
```

`rpc` stops being a property smuggled onto a function object and becomes an
ordinary field on an ordinary record — which also means `config/router.ts` can
merge fragments by reading `.rpc` off the slice list with no `Slice` type
gymnastics.

## 2. A module is its slices plus its own persistence

```ts
// modules/billing/src/index.ts
import { postgresEventStore, postgresTokenStore } from "@kronos-ts/postgres"

export const billingModule = (env: KarmaEnv) => {
  const pool = pgPool(env.databaseUrlFor("billing"))     // karma_billing
  const db = drizzle(pool)
  const deps = { db, uowDb: uowDbFor(pool) }

  const slices = [billLines(deps), creditBilledLine(deps), chargeBill(deps), ...]

  return {
    name: "billing",
    eventStore: postgresEventStore(pool),
    tokenStore: postgresTokenStore(pool),      // carries its own transaction manager
    states:     slices.flatMap((s) => s.states ?? []),
    commands:   slices.flatMap((s) => s.commands ?? []),
    queries:    slices.flatMap((s) => s.queries ?? []),
    processors: slices.flatMap((s) => s.processors ?? []),
    rpc:        slices.map((s) => s.rpc).filter(Boolean),
    migrate:    () => migrate(db, { migrationsFolder: billingMigrations }),
  }
}
```

What disappeared: `createModuleRuntime`, the `ModuleRuntime` interface, per-module
`boot()`, `defineModule`, the `KarmaModule` type. What replaced them: a function
returning a record.

## 3. `services/main` — one app, 19 modules

```ts
// services/main/src/main.ts
startOtel()

const env = loadEnv()

// Deployment manifest: which modules this service hosts. Still the only place
// that changes when a module moves to another service.
const modules = [
  manualDiscountsModule(env), promotionsModule(env), discountsModule(env),
  settlementModule(env),      billingModule(env),    orderingModule(env),
  paymentsModule(env),        pricingModule(env),    /* … 19 total */
]

await Promise.all(modules.map((m) => m.migrate()))

const rabbit = await connectRabbit(env.RABBITMQ_URL)

const app = await createApp({
  components: {
    ...postgresComponents(pgPool(env.PLATFORM_DATABASE_URL)),
    commandBus: tracingCommandBus(rabbitCommandBus(simpleCommandBus(), rabbit), spans),
    queryBus:   tracingQueryBus(rabbitQueryBus(simpleQueryBus(), rabbit), spans),
  },
  modules,
})

const http = await buildHttp(app, mergeRpc(modules), env.HTTP_PORT)
const ws   = startWs(app, mergeRpc(modules), env.WS_PORT)
```

Compare with today: `bootModules(modules)` (N kronos instances, sequential
migrate-then-boot each) **plus** `bootEdgeApp()` (a handler-less 20th instance
existing only to give the transports gateways). The edge app disappears — there
is one app, and it has the gateways.

## 4. Three things that get structurally better

**Boot ordering stops being load-bearing.** `modules/index.ts` currently carries a
long comment explaining that the list is ordered *consumer-first, topologically
over the seam graph*, because a source module's automations begin replaying the
instant its instance boots, and a `send` to a consumer whose queues are not yet
asserted burns a timeout-retry round. With one app that constraint evaporates:
`createApp` subscribes **every** module's handlers before **any** processor
starts (two-phase start, now implemented in the spike). The array becomes a plain
list; ordering is documentation, not correctness.

**Duplicate command ownership becomes a boot error.** N instances each own a bus,
so two modules can both claim `billing.BillLines` and nobody finds out. One shared
bus rejects it immediately — the spike hit exactly this.

**One connection set per process.** Today: 20 kronos instances → 20 slot
registries, 20 decorator pipelines, 20 RabbitMQ connections. After: one of each,
with per-module Postgres pools (which you genuinely want separate).

## 5. What `module-kit` keeps

It stays — thinner, and no longer a re-implementation of framework wiring.

| keeps | loses |
|---|---|
| `karma_<module>` / `<MODULE>_DATABASE_URL` resolution | `createModuleRuntime`, `ModuleRuntime` |
| karma's OTel + correlation defaults | per-module `boot()`, `KarmaModule` |
| `orpc`, `auth`, `scope`, `pip`, `hierarchy-mirror` | `defineModule`, `Slice` type |
| the `trackingView` / `trackingAutomation` failure-semantics split | the token-store/UoW/DLQ wiring under it |
| testing helpers | `schema.ts` (kronos owns its own tables) |

## 6. What this needs from kronos first

1. **Lifecycle.** `createApp` must become `await createApp(...)` with ordered
   start/stop, because rabbit connect, postgres bootstrap and processor start have
   a real order. This is the one genuinely unfinished piece.
2. **Token store carries its transaction manager**, so `postgresTokenStore(pool)`
   is one value and the pairing hazard is gone.
3. **Kronos owns `kronos_token_entries` / `kronos_dead_letters`** and their
   migrations, so `module.migrate()` is only the module's own read models.
4. **Drop the string-keyed config shim** — pass the typed record into the
   invocation path instead of `getComponent<T>(name)`.
5. **`ProcessingGroup` on the module record**, so rabbit queue names key on the
   module rather than the service — the identity finding from the previous spike.

## 7. Honest risks

- **19 module factories in one file.** The manifest is ~25 lines and each module
  is one call, so it reads fine — but every module now constructs its own pool at
  import-of-manifest time. Worth passing a shared pool factory so connection
  limits stay controllable.
- **You lose the ability to boot one module in isolation for free.** Today
  `module.boot({ withoutBroker: true })` is a first-class test path. Functionally
  it is `createApp({ modules: [billingModule(testEnv)] })` — arguably simpler, but
  every integration fixture that calls `boot()` changes.
- **Migration is not incremental.** Both styles cannot coexist, so this is a
  cutover: `module-kit` and all 19 modules change together. The domain code
  inside slices is untouched, which is what makes it feasible at all.
