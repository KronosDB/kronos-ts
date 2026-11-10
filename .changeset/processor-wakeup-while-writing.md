---
"@kronos-ts/core": patch
---

An event appended while a processor was finishing a batch could wait until the next event arrived before being handled. It happened when the processor had already looked for more events and was still writing its position, as with the Postgres token store after a stretch of events it does not handle. A wake-up during a batch now makes the processor look again once the batch is done.
