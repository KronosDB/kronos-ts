---
"@kronos-ts/postgres": minor
---

**Breaking:** with `postgresAdapter` and `bunSqlAdapter`, an event's and a scheduled event's `payload` and `metadata`, and a snapshot's `metadata`, are stored as JSON objects. They were stored as a JSON string containing the JSON text, so `payload->>'field'` returned NULL. With `pgAdapter` they were already objects and nothing changes. Everything read through the stores is unchanged. What changes is what your own SQL sees under those two drivers: `SELECT payload` returns an object for a row written from now on, where it returned a string that had to be parsed.

Rows already stored as strings are not rewritten, and the stores keep reading them. If you query these columns in SQL, rewrite the rows with one statement per table:

```sql
UPDATE kronos_events
   SET payload  = CASE WHEN jsonb_typeof(payload)  = 'string' THEN (payload  #>> '{}')::jsonb ELSE payload  END,
       metadata = CASE WHEN jsonb_typeof(metadata) = 'string' THEN (metadata #>> '{}')::jsonb ELSE metadata END
 WHERE jsonb_typeof(payload) = 'string' OR jsonb_typeof(metadata) = 'string';
```

The same statement with `kronos_scheduled_events` rewrites schedules, and `UPDATE kronos_snapshots SET metadata = (metadata #>> '{}')::jsonb WHERE jsonb_typeof(metadata) = 'string'` rewrites snapshots. The statements parse every string, so they are for tables whose event payloads are objects or arrays, not strings.
