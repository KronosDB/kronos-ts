---
"@kronos-ts/postgres": patch
---

`postgresEventStore` inserts the events of one append with one statement per 100 events. It ran one `INSERT` per event. The events keep the order they were given, and an append of one event sends the statement it sent before.
