---
"@kronos-ts/kronosdb": minor
---

A command, query or subscription query nothing handles fails with an error `isNoHandler` recognises: `NoHandlerForCommandError` and `NoHandlerForQueryError` carry core's `no-handler` kind, and the native protocol's new `no_handler` status becomes a `KronosDbRequestError` with status `"no_handler"`, the same kind, and `transient: false`. A server without that status still answers `unavailable`. The vendored protocol codec is updated to the server's current schema.
