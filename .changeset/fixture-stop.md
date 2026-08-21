---
"@kronos-ts/test": minor
---

`TestFixture` gains `stop(): Promise<void>` — the fixture assembles a real app with running processors, and now it can also release them. A test runner that force-exits never needs it; a plain script, REPL, or a runner without forced exit does. Poll timers are also unref'd via core, so even a forgotten `stop()` no longer keeps the process alive.
