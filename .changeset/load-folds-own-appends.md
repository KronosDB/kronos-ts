---
"@kronos-ts/core": patch
---

`ctx.load` after `ctx.append` folds an appended event only into the states whose query matches it. An append was folded into every loaded state that has an evolver for the event's type, so appending for one id changed the state loaded for another. A state first loaded after the append now sees the appended event too. `UoWStateCache.modules` is removed.
