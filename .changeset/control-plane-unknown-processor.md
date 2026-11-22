---
"@kronos-ts/kronosdb": patch
"@kronos-ts/axon-server": patch
---

A control plane fails an instruction that names a processor the application does not have, with `No event processor named "<name>" in this application`. It was treated as done.

- `kronosDbControlPlane`: the server is told the instruction failed, with that message. It was told the instruction succeeded.
- `axonServerControlPlane`: the failure is logged by the platform stream, which sends Axon Server no result for any instruction.
