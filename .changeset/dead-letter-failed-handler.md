---
"@kronos-ts/core": patch
---

A dead letter records which handler failed, and a replay calls that handler and the ones after it. Before, `reprocessDeadLetters` called every handler of the event again, including the ones that had succeeded when the event was first delivered. The handler is recorded as `diagnostics.handlerIndex`: its position among the processor's handlers for the event, in the order `kronos()` was given them. `diagnostics.handlerName`, which held the event's name, is no longer written. A letter parked behind another, a letter without `handlerIndex`, and a letter whose `handlerIndex` is past the end of the handlers are replayed through every handler, as before.
