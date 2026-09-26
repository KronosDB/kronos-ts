---
"@kronos-ts/core": minor
"@kronos-ts/postgres": minor
---

A decision that loads several states is refused when any of them changed before it committed.

- The flushed append condition uses the earliest read's marker, not the latest. Previously an event matching the first state could land between two loads, below the second load's marker, and the decision committed on a stale first state.
- `AppendCondition` gains an optional `reads`: each read's query with its own marker. The in-memory and Postgres stores check it read by read, so an event a later read already saw is not a conflict. Stores that ignore it check the whole query against the earliest marker. A handler's `appendCondition` override drops `reads`.
- Postgres locks the tags an append writes as well as the tags its condition reads, with or without a condition, so writers of one tag commit in position order.
- A Postgres read that matches nothing, including a snapshotted read with no events after the snapshot, returns a marker just below where it started instead of the log head.

```ts
// before
appendCondition = { query: [...left, ...right], marker: { position: max(leftMarker, rightMarker) } }
```

```ts
// after
appendCondition = {
  query: [...left, ...right],
  marker: { position: min(leftMarker, rightMarker) },
  reads: [{ query: left, marker: leftMarker }, { query: right, marker: rightMarker }],
}
```
