---
"@kronos-ts/axon-server": patch
---

`axonServerControlPlane` reads its processors on every instruction and every status report. It copied them once when it was created, so a processor added afterwards was never reported and could not be paused, started, split, merged or released from Axon Server.

It now takes the map itself, `axonServerControlPlane(axon, app.processors)`, as well as an array. A processor added to the map or array later is reported and addressable, and one removed is no longer reported. Values that are not processor-shaped are ignored. The parameter's type is exported as `ManagedProcessorSource`.

An iterator such as `app.processors.values()` is still accepted and still copied once, because it can be read only once: pass the map to see later processors. `control.processors` returns the processors addressed at the time it is read.
