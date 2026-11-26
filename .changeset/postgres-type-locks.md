---
"@kronos-ts/postgres": minor
---

An append condition by event type alone is enforced. `postgresEventStore` locks every event type an append writes, as it locks every tag, and locks the types a condition reads by when the item has no tag. Before, an item with no tag was not locked on at all: several simultaneous appends with the same type-only condition were all accepted, and a read by type could miss an uncommitted event of that type and still condition an append.

Appends of the same event type now wait for each other, as appends of the same tag do. Eight writers appending one type went from about 2,000 to about 500 to 800 appends a second on a development machine; one writer, and writers of different types, are unchanged.

**Breaking:** an append condition with an item that has neither a tag nor a type, which matches every event, is refused with an error. It was accepted and not enforced.

`acquireWriteLocks`, `acquireReadLocks` and the intent lock keys in `advisory-locks.ts` are replaced by `acquireLocks`, `tagKey` and `typeKey`; none were exported from the package.
