---
"@kronos-ts/core": patch
---

`inMemorySchedulingEventStore`: a `cancelSchedule` in a unit of work that then fails is undone completely. The schedule was set back to pending without its timer, so it never fired. The timer is now armed again for the schedule's fire time, or for now when that time has passed. A schedule and its cancel in one failed unit of work leave no record behind, so a later cancel of that token answers `not-found`. A cancel stands when the unit of work fails after its commit, in an `onAfterCommit` action.
