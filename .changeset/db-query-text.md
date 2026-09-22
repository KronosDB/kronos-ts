---
"@kronos-ts/postgres": minor
---

`db.statement` spans carry the statement as `db.query.text`.

- The text is the SQL as written, placeholders included: `ctx.sql().query(text, params)`, a driver client's `.query(text | { text })`, postgres.js / Bun.sql `.unsafe(text, params)`, and the tagged-template form, whose strings are joined back with `$n`.
- Parameters are never recorded.
- `SpanningTrace.span` options gain an optional `attributes` record.
