# @kronos-ts/modelling

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
