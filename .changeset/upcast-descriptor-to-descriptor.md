---
"@kronos-ts/core": minor
"@kronos-ts/axon-server": patch
---

**Breaking:** an upcast is declared from one event descriptor to another, and `upcastingEventStore` takes a list of them:

```ts
upcastingEventStore(store, [
  upcast(CourseCreatedV1, CourseCreated, (payload) => ({ ...payload, capacity: 30 })),
  upcast(RentalOverdue, RentalLapsed, (payload) => payload),
])
```

The `Upcast` function type `(event) => event`, the `storedTypes` option and `UpcastingOptions` are removed. To move an upcaster over, name the descriptor it matched with `is()` as `from`, the descriptor it produced as `to`, and keep its payload change as `map`.

The store reads the names events are stored under off the `from` descriptors. An event stored under an old name is found without `storedTypes`, and a type filter is always passed on to the log; it was dropped when `storedTypes` was absent. An event goes through every step that applies to it, so upcasts from version 1 to 2 and from 2 to 3 take a stored version 1 to version 3. Two upcasts from one descriptor, and upcasts that lead back to where they started, are refused when the store is built.

`upcastingEventStore` no longer reads an event's tags, so it works over `axonServerEventStore`, whose events come back without them.
