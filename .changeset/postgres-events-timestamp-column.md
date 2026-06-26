---
"@kronos-ts/postgres": minor
---

Simplify the postgres event-store time columns.

- The events table drops `recorded_at` (the DB insert time). It was written by default but never read — the store carries only the EventMessage's authored timestamp.
- The authored-timestamp column is renamed `message_timestamp` → `timestamp` on both the events and scheduled-events tables, so a schedule row and the event it materialises into share the same column names (`version`, `timestamp`). It stays a `BIGINT` of epoch milliseconds (btree/BRIN-indexable).

Schema change is CREATE-only: an events or scheduled-events table created before this release must be hand-migrated (`ALTER TABLE … DROP COLUMN recorded_at`, `ALTER TABLE … RENAME COLUMN message_timestamp TO timestamp`) or reset before upgrading.
