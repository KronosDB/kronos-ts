---
"@kronos-ts/postgres": patch
"@kronos-ts/axon-server": patch
---

`resilience.log` defaults to `console.warn`, as its documentation says. `postgresPool().start()` and `axonServerConnection()` now write one line per retried attempt when no `log` is given; pass `log: () => {}` to silence it.
