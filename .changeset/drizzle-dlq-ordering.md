---
"@kronos-ts/drizzle": minor
---

`drizzleDeadLetterQueue` takes its handles positionally; the `asc` parameter is deleted.

Ordering was never configuration. Per-sequence FIFO is this queue's CONTRACT —
`asc` had exactly one valid value, and passing drizzle's sort helper in bought
nothing against a dual-instance `drizzle-orm`, because the `db` handle and the
table reference already come from the caller's instance. It was also dead: the
implementation used the `asc` imported from the extension's own `drizzle-orm`
peer dependency, never the one handed in.

With `asc` gone the config record held two required handles and one required
string, so it flattens — matching `knexDeadLetterQueue(knex, { ... })`, where
the handle is positional and only tuning lives in a record.

```ts
// before
const dlq = drizzleDeadLetterQueue({
  db, table: kronosDeadLetters, processingGroup: "my-processor", asc,
})

// after
const dlq = drizzleDeadLetterQueue(db, kronosDeadLetters, "my-processor")
const tuned = drizzleDeadLetterQueue(db, kronosDeadLetters, "my-processor", {
  maxSequences: 64,
})
```

`DrizzleDeadLetterQueueConfig` is replaced by `DrizzleDeadLetterQueueOptions`
(tuning only: `maxSequences`, `maxSequenceSize`, `claimDurationMs`) plus an
exported `DrizzleDb` handle type. `drizzle-orm` remains a peer dependency, which
is what keeps the instance identity correct.

The knex, kysely, prisma and typeorm dead letter queues were checked for the same
leaked-helper pattern and have none — they order with a plain `"asc"` string.
