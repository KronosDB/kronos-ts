---
"@kronos-ts/postgres": patch
---

Order the gap-free tail by `transaction_id` numerically instead of lexically.

The streaming query selects `transaction_id::text AS transaction_id`, and `ORDER BY transaction_id` bound to that text alias — so the xid8 tail cursor was sorted as text. Once a working set's transaction ids straddle a power-of-ten boundary (e.g. 999 → 1000), lexical order (`"1000" < "999"`) diverges from numeric order while the `WHERE (transaction_id, sequence_position) > (…)` resume comparison stays numeric. The two disagree, so the stream delivers events out of commit order and strands events when reopened from a tracking token (e.g. after a handler error redelivery). `ORDER BY` now references the `transaction_id` / `sequence_position` columns directly, matching the numeric comparison used everywhere else.
