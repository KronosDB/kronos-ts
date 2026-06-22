---
"@kronos-ts/eventsourcing": minor
"@kronos-ts/messaging": minor
"@kronos-ts/app": patch
---

Add `schedule()`, `scheduleAfter()`, and `cancelSchedule()` handler helpers for the event scheduler.

Call them from inside a command or event handler the same way as `append()` / `send()` — pass an event descriptor + payload and a fire time, and the helper builds the event message and uses the configured `EventScheduler`. No fetching the scheduler from the app or hand-building an `EventMessage`.

- `schedule(event, payload, at: Date)` schedules at an absolute time.
- `scheduleAfter(event, payload, delayMs)` schedules a delay from now.
- Both return a `ScheduleToken`; `cancelSchedule(token)` cancels it.

The scheduler is injected into the active UnitOfWork at handler-invocation entry (event processors and command handlers), so a schedule participates in the handler's transaction — it commits with the handler and rolls back if the handler throws. Event metadata defaults to the UoW metadata, carrying correlation/causation onto the fired event.
