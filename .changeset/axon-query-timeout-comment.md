---
"@kronos-ts/axon-server": patch
---

Doc comments corrected: the default query timeout is 30000 ms (a comment said one hour), `flowControl.refillThreshold` defaults to half of `permits`, and `ProcessingInstructions` are read from a command's metadata only.
