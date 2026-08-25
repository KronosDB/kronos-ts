---
"@kronos-ts/core": minor
---

`PersistenceFamily` is `UnitOfWorkBrand` — the slot names what it brands.

```ts
// before
type DrizzleUnitOfWork = PersistenceFamily<"drizzle", "build this processor's unitOfWork with drizzleUnitOfWork(next, db)">

// after
type DrizzleUnitOfWork = UnitOfWorkBrand<"drizzle", "build this processor's unitOfWork with drizzleUnitOfWork(next, db)">
```

BREAKING for anyone who spelled the slot; the six adapter packages are the
only known occupants and are updated in the same release. The mixing-check
diagnostic now reads `[unitOfWorkBrand].FIX`.
