---
"@kronos-ts/core": minor
---

A reset now replays up to where the processor had got to instead of the head of the log, so events it was never handed are not replays. Resetting to the current position or past it, or before the processor ever ran, replays nothing; resetting again mid-replay keeps the first reset's point.

`ctx.isReplay()` is decided from each event's own position, so an event past the reset point is not a replay even when a filtered stream skips straight to it.

A replay in progress is stored with the token and survives a restart: the token row holds `ReplayToken` with the reset point, and existing rows read as before.

`resetTokens(from?, reset?)` takes an optional function that runs in the unit of work that stores the new token. Clearing the projection's tables there commits together with the reset; if it throws, neither happens.
