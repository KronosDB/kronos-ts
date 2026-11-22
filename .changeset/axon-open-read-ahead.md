---
"@kronos-ts/axon-server": patch
---

`axonServerEventStore`'s `open()` holds at most `readAhead` events ready for its reader (1000 when the reader names none) and stops reading from Axon Server while it has that many. It used to read everything the server sent into memory, so a reader slower than the server grew the buffer without bound. `open()` throws a `RangeError` for a `readAhead` that is not a positive integer.
