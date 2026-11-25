---
"@kronos-ts/core": patch
---

`upcastingEventStore` reads a log that hands events back without their tags. `EventStore` has an optional `returnsTags: false` for such a log. Over one, `source()` narrows what it read by type alone; it matched each event's tags against the query before, and returned nothing. A query whose items differ in tags and in types is refused with an error unless `storedTypes` is given. A log that returns tags is read as before.
