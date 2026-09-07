---
"@kronos-ts/core": minor
---

Unit-of-work phases are names, not numbers. `Phase.PRE_INVOCATION` is `"pre-invocation"`, `Phase.COMMIT` is `"commit"`, and so on; `PhaseValue` is the string union. The numeric values were Axon's ordering keys — spaced so custom phases could be inserted between them — and nothing in kronos-ts ever ordered by them: the phase sequence is the code in `execute`, and the only comparison is equality. `WrongUoWPhase` now prints the phase by name. BREAKING only for code that typed the numbers instead of `Phase.*`.
