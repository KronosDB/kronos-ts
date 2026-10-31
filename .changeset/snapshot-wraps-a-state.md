---
"@kronos-ts/core": minor
---

A state is cached by wrapping it: `snapshot(state, key, when)` returns the state with a snapshot policy. `state()` no longer takes `snapshot` or `lifecycle`.

- `state({ …, snapshot: { key, when } })` becomes `snapshot(state({ … }), key, when)`. The state passed in is unchanged, and wrapping a cached state again does not compile.
- `SnapshotPolicy` is a function, `(result: EvolutionResult) => boolean`. `afterEvents` and `whenSourcingTimeExceeds` return one. `.or` is removed; combine two with `(r) => a(r) || b(r)`.
- `noSnapshotPolicy()` is removed. A state that should not snapshot is not wrapped.
- `state({ lifecycle })` and `StateLifecycle` are removed. The hooks ran while a load folded, so on every load rather than once per event.
