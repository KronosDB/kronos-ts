---
"@kronos-ts/core": patch
---

A processor whose first claim round fails (for example, its token store is down when the app boots) keeps running: the failure is logged, reported as `status().error`, and the claim round is retried every claim interval until it succeeds. `start()` no longer rejects for it, so `kronos()` no longer leaves an unhandled rejection and a stopped processor behind. A later claim round that fails is reported the same way.
