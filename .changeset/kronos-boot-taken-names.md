---
"@kronos-ts/core": patch
---

`kronos()` refuses, before it subscribes anything, an entry for a command or query that an earlier `kronos()` call already subscribed on the same bus object. The error names the command or query and both entries. Before, the bus refused the second handler at subscribe time, after the entries ahead of it in the lists were already subscribed, so the failed boot left handlers on the buses. A name stays taken after the app that subscribed it has stopped, because a bus keeps its subscriptions. A handler put on a bus with `bus.subscribe` directly, or through a different wrapper object over the same inner bus, is not seen by this check.
