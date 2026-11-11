---
"@kronos-ts/test": minor
---

`given` events are no longer handed to the processors as a replay. The fixture leaves the app's processors running as `kronos` started them and never stops them or resets their tokens.

- `given(...)` appends its events through the recorded log and settles: every processor reaches the head of the log and what it dispatched has finished. The processors handle the events live, so `ctx.isReplay()` is false, and what an event handler does with them happens: a command an automation dispatches is sent and a deadline it arms is armed. An automation that decides from the state it loads stays correct. A scenario that relied on `given` not setting an automation off, such as a command scenario whose `given` leaves work for an automation in the same app, now sees what the automation did.
- `when(...)` opens the observation window before it acts. What `given`, or an `advance` before the act, set off is not in the outcome. Before, an `advance` before the act put what it fired in the outcome.
- `recordingEventStore` and `recordingCommandBus` no longer have `hold(on)`.
