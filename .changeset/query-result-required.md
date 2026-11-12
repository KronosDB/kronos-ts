---
"@kronos-ts/core": minor
---

**Breaking:** every query declares its result. `query({ … })`, `queryDescriptor({ … })` and `ns.query(name, { … })` require `result`, the schema of what the query answers. TypeScript refuses a query without one, and from JavaScript the declaration throws a `TypeError` naming the query. `QueryDescriptor<P, R>` has a required `result: R`, and `R` defaults to `StandardSchemaV1` instead of `undefined`, so a bare `QueryDescriptor<P>` means any query with that payload. A command's `result` stays optional.

Before:

```ts
const GetBalance = ns.query("GetBalance", { payload: z.object({ accountId: z.string() }) })

const balance = await query(queryBus, GetBalance, { accountId }) // unknown
```

After:

```ts
const GetBalance = ns.query("GetBalance", { payload: z.object({ accountId: z.string() }), result: z.number() })

const balance = await query(queryBus, GetBalance, { accountId }) // number
```

`query(bus, …)`, `ctx.query`, `subscriptionQuery` and `ctx.emitUpdate` no longer accept `undefined` as a query's result type. Code generic over a query descriptor drops `| undefined` from its result parameter.

Before:

```ts
function subscribe<P extends StandardSchemaV1, R extends StandardSchemaV1 | undefined>(descriptor: QueryDescriptor<P, R>, payload: InferOutput<P>)
```

After:

```ts
function subscribe<P extends StandardSchemaV1, R extends StandardSchemaV1>(descriptor: QueryDescriptor<P, R>, payload: InferOutput<P>)
```
