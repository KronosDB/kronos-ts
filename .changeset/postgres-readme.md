---
"@kronos-ts/postgres": patch
---

README corrected, no code change. `kronos_snapshots` has one `key` primary key, not `(state_name, state_id)`. Writers of one tag serialise and both commit unless a condition refuses the second. A violated append condition is found by the store and thrown with `code` `KR001`; Postgres raises no `KR001`. A query handler's unit of work carries no transaction. The PgBouncer answer names what depends on a session, and the Postgres 14 requirement no longer gives `xid8` as its reason.
