---
"@kronos-ts/core": patch
---

A processor recognises a refused segment claim by the new `isUnableToClaimToken(error)` guard, which reads a `claimRefused` field on `UnableToClaimTokenError`, instead of `instanceof`. A token store built against another installed copy of core, such as `@kronos-ts/postgres` pinned to a different core version, no longer makes contention between nodes fail the processor.
