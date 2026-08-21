---
"@kronos-ts/core": minor
"@kronos-ts/test": minor
"@kronos-ts/axon-server": minor
"@kronos-ts/kronosdb": minor
"@kronos-ts/postgres": minor
---

Bring your own schema library. Descriptors now take any [Standard Schema](https://standardschema.dev), so zod, valibot, arktype and anything else that carries a `~standard` property all work — and `@kronos-ts/core` depends on none of them.

```ts
// before — the constraint named one library, and core shipped it
export type CommandDescriptor<P extends z.ZodType, R extends z.ZodType | undefined> = …
export type EventDescriptor<P extends z.ZodType> = { tags?: (p: z.infer<P>) => Tag[] }

// after — the constraint names the CONTRACT
export type CommandDescriptor<P extends StandardSchemaV1, R extends StandardSchemaV1 | undefined> = …
export type EventDescriptor<P extends StandardSchemaV1> = { tags?: (p: InferOutput<P>) => Tag[] }
```

Nothing a zod consumer writes changes. `command({ payload: z.object({ courseId: z.string() }) })` still gives a handler `message.payload: { courseId: string }`, exactly; a wrong payload is still a compile error at the call site; `state({ id: { courseId: z.string() } })` still infers `{ courseId: string }`. That claim is a compile-time test — `packages/core/src/messaging/__tests__/standard-schema.types.ts`, registered in the root `tsconfig.json` `files` array beside the correlation and drizzle probes — which pins the exact inferred types AND accepts a twelve-line hand-written schema object with no library anywhere.

`zod` moves from `dependencies` to `devDependencies` in `@kronos-ts/core` and `@kronos-ts/test`. The contract itself is VENDORED, types-only, in `messaging/standard-schema.ts` — ninety lines transcribed from the published `@standard-schema/spec`, because a types-only dependency is still a dependency that has to resolve at install time for a package whose runtime never touches it.

**BREAKING — `zodValidatingSerializer` is gone.** It briefly became `validatingSerializer` during this release cycle, and then serializer-side validation was deleted outright: a serializer encodes, and validation moved to where the descriptor is — see the validation changeset in this same release. `any(schema?)` in `@kronos-ts/test` took the same treatment — a diff is computed and rendered in one breath, so an async schema is reported as the mistake it is.

---

**BREAKING — `Unstamped<M>` and `stamped()` are gone.** `timestamp` is now optional on `Message`, and unset means one thing: this message has not been through a task yet.

```ts
// before
type Unstamped<M extends Message> = Omit<M, "timestamp"> & { timestamp?: number }
stamped(message: Unstamped<M>, clock: Clock): M
dispatch(m: Unstamped<CommandMessage>): Promise<unknown>

// after
type Message<P> = { …; readonly timestamp?: number }        // unset = not through a task yet
type EventMessage<P> = Message<P> & { …; readonly timestamp: number }   // a fact HAS an instant
dispatch(m: CommandMessage): Promise<unknown>
```

Nothing about WHEN the instant is settled changed. The bus still fills it from `uow.now()` when it mints the unit of work, a transport still fills it from system time at the wire, `ctx.append` still stamps at birth — the stamping is an unexported internal now, and the vocabulary for it is simply not on the surface. `EventMessage` (and therefore `SequencedEventMessage`, and everything a processor hands a handler or a store returns from a read) narrows `timestamp` back to REQUIRED, because a fact you can read has an instant, always.

A pleasant consequence: `interceptingCommandBus` and `interceptingQueryBus` have no casts left. `Intercept<M> = (m: M) => M` now takes and returns exactly the type the bus takes, because there is no longer a second type for the same message one moment earlier.

**BREAKING — the `Clock` type is gone.** Every site writes the arrow.

```ts
// before
export type Clock = () => number
unitOfWork(clock?: Clock) · testFixture(scope, { clock?: Clock })

// after
unitOfWork(clock?: () => number) · testFixture(scope, { clock?: () => number })
```

Same rule that leaves a unit-of-work factory spelled `() => UnitOfWork` and never named: naming a one-arrow type buys an import and hides the one thing the reader needed to see. What the clock MEANS — an instant, epoch milliseconds, the same unit `message.timestamp` carries — now lives on `unitOfWork`'s parameter, which is where it enters.

---

**Upcasting is a mechanism, and it moved to the log boundary.** It is the third one, deliberately the same shape as the other two: `Intercept` is `(message) => message` where a bus hands a message on, `Upcast` is `(event) => event` where the LOG hands an event back.

```ts
// before — a predicate/action method pair, a chain object, and raw JSON at the wire
type EventUpcaster = { canUpcast(type, revision): boolean; upcast(rep): rep | rep[] }
upcasterChain(v1ToV2, v2ToV3)
upcastingSerializer(jsonSerializer(), chain)

// after — a total function in the DOMAIN form, composed in function space
type Upcast = (event: EventMessage) => EventMessage       // identity when unconcerned
upcastingEventStore(store, (e) => v2ToV3(v1ToV2(e)))

const v1ToV2: Upcast = (e) =>
  is(e, CourseCreatedV1)                                  // the OUTDATED version, as its own descriptor
    ? { ...e, version: CourseCreated.version, payload: { ...e.payload, capacity: 30 } }
    : e
```

`EventUpcaster`, `upcasterChain`, `IntermediateEventRepresentation`, `singleEventUpcaster` and `upcastingSerializer` are all **removed**, and so is the shipped `upcastTo` constructor that briefly replaced them. `canUpcast` was a class in disguise — a predicate method and an action method that had to agree — and totality replaces it: "not mine" is "return it unchanged", so nothing has to be asked. `upcasterChain`'s runtime dispatch over a list is plain composition. And writing the match by hand IS the lesson: `is()` makes it a typed switch, the old shape gets its own descriptor so the compiler knows what `payload` looked like back then, and the target version is read off the CURRENT descriptor, where it already lives, so a version is never written twice and can never disagree with itself.

The store is the right boundary for four reasons that are one reason from different sides. The serializer never sees the domain form, so an upcaster written there is written against raw JSON and cannot say `event.tags`. An in-memory store has no serializer at all, so serializer-based upcasting silently skipped every test that used one. One placement covers a processor's deliveries (`open()`) and a `ctx.load` fold (`source()`) uniformly. And a validating serializer under an upcaster would judge the 2019 payload against the 2026 schema and reject it before anything could fix it.

Read paths only — `source()`, `open()`, `subscribe()`. Every write member passes straight through, so what was appended is what is stored, forever; upcasting is a reinterpretation on the way out. Commands and queries need nothing new, because a message crossing versions at a BUS is what `Intercept` already is; only events have a second boundary, because only events are kept.

---

**BREAKING — resilience has left core.** `withRetry`, `healthCheck`, `ResilienceConfig` and `RetryEvent` are no longer exported from `@kronos-ts/core`. Each of the three packages that used them — `@kronos-ts/axon-server`, `@kronos-ts/kronosdb`, `@kronos-ts/postgres` — now owns a package-private `src/resilience.ts`, exported from no barrel.

Same reasoning as the transaction glue, one folder up: it is `setTimeout` and a loop over a function, it touches no message and no unit of work, core's own `src/` never called it, and by this surface's own first rule a helper is not core. Nothing changes for a host — `kronosDbConnection({ resilience: { maxAttempts: 3 } })` and `postgresPool(url, { resilience })` are the same options with the same defaults. It breaks for anyone who imported the helpers directly, and the fix is to own the hundred lines. `postgres` carries only what it uses: no health probe, because a pool bootstraps and then either works or throws.

---

**`message/` is `messaging/`, and its five declaration files are one.**

```
// before                          // after
src/message/qualified-name.ts  ┐
src/message/metadata.ts        │
src/message/message.ts         ├─  src/messaging/messages.ts
src/message/descriptor.ts      │
src/message/namespace.ts       ┘
src/message/clock.ts               (deleted — the arrow IS the contract)
src/message/converter.ts           src/messaging/serialization/converter.ts
src/message/serializer.ts          src/messaging/serialization/serializer.ts
src/message/upcaster.ts            src/upcasting/upcasting-event-store.ts
src/message/tag.ts                 src/messaging/tag.ts            (unchanged)
src/message/identifier.ts          src/messaging/identifier.ts     (unchanged)
src/message/serialized-error.ts    src/messaging/serialized-error.ts (unchanged)
src/resilience.ts                  (deleted — three private copies)
```

A qualified name, a metadata map, a message and the descriptor that declares one are not four topics that happen to live near each other; they are the single answer to "what is a message, before any kind picks it up", and splitting them made five imports of one idea. What genuinely stood alone stayed alone. The barrel exports the same names from the same package, so this is invisible unless you were deep-importing into `@kronos-ts/core/src/message/...`, which was never a supported address.

`messaging/serialization/` is a folder rather than two loose files because a binary serializer is coming and it lands beside these, not on top of them.

---

**`state()` reads in dependency order.** The options are `id · tags · evolve · snapshot? · lifecycle?` now, and every example and test literal is reordered to match. The order is an argument: tags are a function of the id, and `evolve` carries its own seed at position zero.

```ts
// before                                       // after
state({                                         state({
  id: { courseId: z.string() },                   id: { courseId: z.string() },
  initial: () => ({ capacity: 0 }),               tags: (id) => ({ courseId: id.courseId }),
  tags: (id) => ({ courseId: id.courseId }),      evolve: [() => ({ capacity: 0 }), … ],
  evolve: [ … ],                                })
})
```
