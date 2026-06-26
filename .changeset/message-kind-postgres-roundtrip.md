---
"@kronos-ts/messaging": minor
"@kronos-ts/postgres": minor
---

Add a `kind` discriminator to messages and bring the postgres event store to full `EventMessage` round-trip parity.

- `Message` now carries `readonly kind: "command" | "event" | "query"` (exported as `MessageKind`), narrowed to the literal on `CommandMessage`, `EventMessage`, and `QueryMessage`. Handler interceptors can branch on message category at runtime via `message.kind`. Gateways, `send()`, `append()`, `schedule()`, and every event-store reconstruction set it; `kind` is derived, never persisted.
- The postgres event store now persists `version` and `message_timestamp` and selects `event_id`, so `source()` and `open()` reconstruct the complete `EventMessage` — `identifier`, authored `timestamp`, and `version`. Previously these three fields were dropped on read, diverging from the in-memory, axon-server, and kronosdb engines.
- Schema change is CREATE-only. `CREATE TABLE IF NOT EXISTS` does not add columns to an existing table and the new columns are `NOT NULL`, so an events table created before this release must be hand-migrated (`ALTER TABLE ... ADD COLUMN`) or reset before upgrading.
