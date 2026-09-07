---
"@kronos-ts/core": minor
"@kronos-ts/postgres": minor
---

Tags are decided in ONE place: the event descriptor. BREAKING.

```ts
// before — three places had a say: the descriptor, a resolver on the entry or the
// Postgres store, and the flush that merged them (which stored every tag twice)
event({ name, payload, tags: (p) => p.items.map((id) => tag("itemId", id)), tagKeys: ["itemId"] })
postgresEventStore(pg, { tagResolver: metadataBasedTagResolver("tenantId") })

// after — a record of lambdas over payload AND metadata; one value, several, or none
event({
  name, payload,
  tags: {
    itemId:   (p) => p.items,                             // several under one key
    region:   (p) => (p.export ? p.region : undefined),   // no tag this time
    tenantId: (_p, m) => m.tenantId as string,            // from metadata
  },
})
postgresEventStore(pg)
```

- The record is the only form of `tags` and it stays on the descriptor as written. The function form and `tagKeys` are gone; `tagKeysOf(descriptor)` reads the keys off the record and `tagsOf(descriptor, payload, metadata)` computes one event's tags at birth. An event's tags are a set: an identical key/value pair appears once.
- `TagResolver`, `descriptorBasedTagResolver`, `metadataBasedTagResolver` and `multiTagResolver` are deleted, with the `tagResolver` field on handler entries, on `postgresEventStore`'s config, and on `postgresSchedulingEventStore`'s config. `postgresEventStore(pg)` takes no second argument. The flush no longer merges anything.
- Fixes tags being stored twice: every command site defaulted to the descriptor resolver and the flush appended its output to the tags the event already carried.
