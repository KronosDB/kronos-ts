---
"@kronos-ts/otlp": minor
---

`otlpExporter({ errors })` takes a function, `(error: unknown) => string`, that gives a failed span's status message. Without it the message is the error's type, as before. `errors: "message"` becomes `errors: (e) => (e instanceof Error ? e.message : String(e))`.
