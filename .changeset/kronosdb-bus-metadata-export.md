---
"@kronos-ts/kronosdb": patch
---

Export `busMetadata` beside `kronosMetadata`. The messaging-plane header
helper was added with the named-bus split but never re-exported, so a host
composing its own calls could reach the context helper and not the bus one.
