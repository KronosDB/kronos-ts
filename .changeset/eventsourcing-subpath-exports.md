---
"@kronos-ts/eventsourcing": patch
---

Restore the `./append`, `./load` and `./schedule` subpath exports.
`@kronos-ts/messaging`'s handler context imports them; without the exports
map entries every `import ... from "@kronos-ts/messaging"` fails at runtime
with ERR_PACKAGE_PATH_NOT_EXPORTED while typechecking clean.
