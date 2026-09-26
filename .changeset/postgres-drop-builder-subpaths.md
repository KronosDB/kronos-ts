---
"@kronos-ts/postgres": minor
---

Removed the `@kronos-ts/postgres/drizzle` and `@kronos-ts/postgres/kysely` subpaths, along with the optional `drizzle-orm` and `kysely` peer dependencies. BREAKING.

`drizzleHandler`, `kyselyHandler`, `DrizzleCapability` and `KyselyCapability` are gone. Build the client over `ctx.sql().unwrap()` in the handler, or write the wrapper in the app; the README has the recipe under "On `ctx.db`, as a handler wrapper".

```ts
// before
import { drizzleHandler, type DrizzleCapability } from "@kronos-ts/postgres/drizzle"
const wrap = (h) => postgresHandler(drizzleHandler(h, (client: Sql) => drizzle(client)), pg)
```

```ts
// after — drizzleHandler and DbCapability are app code
type DbCapability = { readonly db: PostgresJsDatabase }
const wrap = (h) => postgresHandler(drizzleHandler(h), pg)
```
