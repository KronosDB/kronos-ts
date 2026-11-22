---
"@kronos-ts/axon-server": minor
---

**Breaking:** `messageSizeValidator`, `MessageSizeExceededError` and `MessageSizeConfig` are removed. Nothing in the package called the validator, so no message was ever checked by it. A message over Axon Server's size limit is refused by the server and still surfaces as an `AxonServerError` with `transient: false`.

Code that imported the validator and called it itself needs its own size check.
