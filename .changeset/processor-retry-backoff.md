---
"@kronos-ts/core": patch
---

An event processor now waits longer between retries of a failed batch. The first retry waits twice the polling interval, as before; each further consecutive failure doubles the wait, up to 60 seconds. A successful batch, or stopping and starting the processor, returns the wait to the first value. The error log line now includes the wait before the retry.
