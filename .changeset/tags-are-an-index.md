---
"@kronos-ts/core": minor
"@kronos-ts/kronosdb": minor
"@kronos-ts/postgres": minor
"@kronos-ts/axon-server": minor
"@kronos-ts/test": minor
---

**Breaking:** tags are the event store's index and are no longer a field of an event.

- `EventMessage` has no `tags`. An event read from a store, handed to a handler or an evolver, kept in a dead letter or listed by `recordingEventStore(...).appended` does not carry them.
- `EventStore.append` and `ScheduleStoreCapability.schedule` take a `TaggedEvent`, `{ event, tags }`: the event and the tags it is appended under. `ctx.append` and `ctx.schedule` are unchanged and work the tags out from the descriptor as before.
- Code that read `event.tags` reads the payload. A processor `Sequence` written `(e) => e.tags.find((t) => t.key === "accountId")?.value ?? e.identifier` becomes `(e) => (e.payload as { accountId?: string }).accountId ?? e.identifier`. `tagsOf(descriptor, payload, metadata)` still gives the tags a descriptor gives an event.
- `@kronos-ts/kronosdb`: `kronosDbTagsAt` is removed.
- `@kronos-ts/postgres`: reads no longer select the `tags` column.

Nothing stored changes: events are written with the same tags and found by the same queries.
