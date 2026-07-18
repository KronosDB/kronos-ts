# @kronos-ts/common

## 0.1.2

### Patch Changes

- f3f9fbc: Publish compiled `dist` entrypoints in npm manifests instead of development-only TypeScript source paths. This makes the packages directly importable in Node.js while retaining concrete versions for workspace dependencies.

## 0.1.1

### Patch Changes

- Publish via `bun publish` so `workspace:*` resolves to concrete versions in published manifests. Previously `changeset publish` shelled out to `npm publish`, which does not understand Bun's workspace protocol, leaving literal `"workspace:*"` strings in published manifests and breaking installs for consumers.
