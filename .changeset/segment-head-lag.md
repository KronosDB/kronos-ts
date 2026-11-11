---
"@kronos-ts/core": patch
---

A segmented processor moves every segment it holds to the head of the log. A segment whose last event came before other segments' final events no longer stays behind them, reporting `caughtUp: true`, until a new event of its own arrives.
