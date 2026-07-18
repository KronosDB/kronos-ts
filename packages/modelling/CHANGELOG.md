# @kronos-ts/modelling

## 0.2.10

### Patch Changes

- f3f9fbc: Publish compiled `dist` entrypoints in npm manifests instead of development-only TypeScript source paths. This makes the packages directly importable in Node.js while retaining concrete versions for workspace dependencies.
- Updated dependencies [f3f9fbc]
  - @kronos-ts/common@0.1.2
  - @kronos-ts/messaging@0.9.2

## 0.2.9

### Patch Changes

- Updated dependencies [ad944b9]
  - @kronos-ts/messaging@0.9.1

## 0.2.8

### Patch Changes

- Updated dependencies [9eb84ff]
  - @kronos-ts/messaging@0.9.0

## 0.2.7

### Patch Changes

- Updated dependencies [56bfb6d]
- Updated dependencies [56bfb6d]
  - @kronos-ts/messaging@0.8.0

## 0.2.6

### Patch Changes

- Updated dependencies [dafdf12]
  - @kronos-ts/messaging@0.7.0

## 0.2.5

### Patch Changes

- Updated dependencies [291acd2]
- Updated dependencies [291acd2]
  - @kronos-ts/messaging@0.6.0

## 0.2.4

### Patch Changes

- @kronos-ts/messaging@0.5.1

## 0.2.3

### Patch Changes

- Updated dependencies [6a3dca4]
  - @kronos-ts/messaging@0.5.0

## 0.2.2

### Patch Changes

- Updated dependencies [dc0f67e]
  - @kronos-ts/messaging@0.4.0

## 0.2.1

### Patch Changes

- Updated dependencies [4b8faa5]
  - @kronos-ts/messaging@0.3.1

## 0.2.0

### Minor Changes

- c1a1cf5: **Breaking:** `state()` evolvers are now registered through a builder —
  `evolve: (on) => [...]` — instead of a bare array. The `on` handed to the
  builder is bound to the state type, so evolver callbacks no longer need a
  `(s: State)` annotation: the state type is fixed by `initial` before the
  evolvers are checked, so they are validated against it instead of competing to
  infer it. This delivers the documented "state type is inferred from `initial`"
  behavior.

  Migrate by wrapping the array in `(on) => [...]`, dropping the `(s: State)`
  annotations, and removing the now-unused `on` import (the builder supplies it).
  Annotate `initial` (`initial: (id): State => ...`) only when it under-specifies
  the type via empty arrays or unions.

  ```ts
  // before
  evolve: [
    on(CourseCreated, (s: CourseState, { payload }) => ({
      ...s,
      created: true,
    })),
  ];
  // after
  evolve: (on) => [
    on(CourseCreated, (s, { payload }) => ({ ...s, created: true })),
  ];
  ```

  `@kronos-ts/app`: `kronos({ states })` partial-config now accepts precisely-typed
  state modules (`StateModule<any, any>[]`), matching `App.states()`. The previous
  `StateModule[]` (`<unknown, unknown>`) rejected concrete modules because `Id`
  sits in a contravariant position — a latent bug surfaced once the evolve builder
  began inferring state types precisely.

### Patch Changes

- Updated dependencies [74dc43d]
  - @kronos-ts/messaging@0.3.0

## 0.1.2

### Patch Changes

- Updated dependencies
- Updated dependencies
  - @kronos-ts/messaging@0.2.0

## 0.1.1

### Patch Changes

- Publish via `bun publish` so `workspace:*` resolves to concrete versions in published manifests. Previously `changeset publish` shelled out to `npm publish`, which does not understand Bun's workspace protocol, leaving literal `"workspace:*"` strings in published manifests and breaking installs for consumers.
- Updated dependencies
  - @kronos-ts/common@0.1.1
  - @kronos-ts/messaging@0.1.1
