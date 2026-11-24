---
"@kronos-ts/core": patch
---

`validatingHandler` and `correlatingHandler` accept a handler that takes only its message. The wrapped handler's context was typed `undefined`, so it could not be given to `commandHandler`, `eventHandler` or `queryHandler` without a cast, and calling it without a context threw `TypeError: Cannot destructure property 'send'`. Its context is now `unknown`, and a missing context is passed through. A handler with a context parameter keeps the context type it named.
