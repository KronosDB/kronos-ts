---
"@kronos-ts/core": minor
---

The app returned by `kronos()` reports what its handlers were wired with: `app.eventStores`, `app.commandBuses` and `app.queryBuses` are the distinct stores and buses its entries and processors carry, by object identity, in the order first met. Each keeps its type, including every capability its wrappers added; a list that is absent contributes none. `App` takes these as type parameters (`App<S, C, Q>`), defaulting to the plain store and buses.

`EventProcessor` keeps the type of the log it reads (`EventProcessor<U, S>`, `S` defaulting to `EventStore`), and `concurrent`, `deadLettering` and `segmented` return the processor value they were given with its type unchanged, as `batched` and `polling` already did.
