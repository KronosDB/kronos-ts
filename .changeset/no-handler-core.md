---
"@kronos-ts/core": minor
---

`NoHandlerError` and `isNoHandler(error)` name a message nothing handles. `localCommandBus` and `localQueryBus` throw `NoHandlerError` for a name with no handler, with the same message as before, and `FailureKind` includes `"no-handler"`.
