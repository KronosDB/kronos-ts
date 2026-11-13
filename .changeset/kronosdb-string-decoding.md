---
"@kronos-ts/kronosdb": patch
---

Protocol strings and tag text are decoded with `TextDecoder` at every length. The vendored codec is regenerated from KronosDB; the schema hash is unchanged.
