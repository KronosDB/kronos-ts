---
"@kronos-ts/common": patch
"@kronos-ts/messaging": patch
"@kronos-ts/modelling": patch
"@kronos-ts/eventsourcing": patch
"@kronos-ts/app": patch
"@kronos-ts/kronosdb": patch
---

Publish compiled `dist` entrypoints in npm manifests instead of development-only TypeScript source paths. This makes the packages directly importable in Node.js while retaining concrete versions for workspace dependencies.
