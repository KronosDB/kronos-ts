---
"@kronos-ts/core": minor
---

A command handler returns its result as plain literals: `return { ok: false, rejection: "FULL" }` is accepted for a result declaring those literals, with no `as const` and no annotated return type. Before, every literal in the return widened (`{ ok: boolean; rejection: string }`) and the call was refused.

`commandHandler` has one signature where it had four overloads: `<P, const R = undefined, C>(descriptor, handler | { handler, appendCondition })`. An array literal in the return may come back readonly (`{ items: [id] }` is `readonly [string]`), and the handler's return type accepts that. **Breaking** only for a call that passes type arguments to a command without a result: the context type is now the third type argument, not the second (`commandHandler<P, undefined, MyContext>(…)`).
