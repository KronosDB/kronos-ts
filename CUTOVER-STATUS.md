# Container cutover — status

**Branch state: WIP, does not compile (55 type errors). Deliberate checkpoint.**
No backwards compatibility anywhere: `kronos()`, the slot registry, decorators,
extensions-as-mutators and `defineModule` are deleted, not deprecated.

## Done

- **Container deleted.** `app.ts` (948), `kronos.ts`, `slot-registry.ts`,
  `resolved.ts`, `decorator.ts`, `defaults.ts`, `components.ts`, `lifecycle.ts`,
  `warnings.ts`, `errors.ts` — plus `module.ts`/`module-scope.ts` (PR #19's
  `defineModule`) and 22 container tests. `packages/app/src` is now
  `create-app.ts` + `index.ts`.
- **`postgres` converted**, which settles the open lifecycle question.

## The lifecycle answer: there isn't a lifecycle system

An adapter is an async factory that connects eagerly and returns what it
provides plus the functions to run and stop it. Ordering lives in your
composition root, written down, instead of in framework stages:

```ts
const pg  = await postgres({ adapter, serializer, tagResolver })
const app = createApp({ components: { ...inMemoryComponents(), ...pg.components }, modules })
await pg.start()                       // scheduler, once every handler is subscribed
// …
await app.stop(); await pg.close()
```

`onStart("connect" | "warmup" | "register" | "processors" | "serve")` and
`onStop` are gone. What they encoded — connect before bootstrap, scheduler after
handlers — is now two awaits in the order you want them.

Dependencies that used to be resolved lazily from other slots are arguments:
`postgres()` takes `serializer` and `tagResolver` rather than receiving them from
a resolution proxy.

## Remaining (55 errors, all the same shape)

| file | errors | work |
|---|---|---|
| `extensions/rabbitmq` | 15 | same conversion; has a real connect/close |
| `extensions/kronosdb` | 12 | same conversion |
| `extensions/axon-server` | 12 | same conversion |
| `extensions/opentelemetry` | 4 | decorates buses → becomes `tracingCommandBus(bus, spans)` at the root |
| `packages/test` | 8 | fixture + recording-enhancer are built on the container; need rebuilding on `createApp` |
| `integrationtests/domain/courses` | 4 | uses the old registration surface |

Then, not visible to `tsc` because test files are excluded from it: **~40 test
files still call `kronos()`** and will fail at runtime. They convert to
`createApp` or, where they tested the container itself, get deleted.

## Order I would finish in

1. `opentelemetry` — smallest, and it validates that a decorator-style extension
   really does collapse into ordinary bus wrapping at the composition root.
2. `rabbitmq` — the one with genuine connect/close, so it stresses the lifecycle
   answer above.
3. `kronosdb`, `axon-server` — mechanical repeats of `postgres`.
4. `packages/test` — the fixture is the thing every integration test leans on, so
   it gates step 5.
5. The ~40 test files.

## Deliberately not done

`ProcessingContext`/`ProcessingGroup` renames, upcasters wired into the read
path, `eventDispatchInterceptor` (still wires to publish, not append),
`ctx.effect`, rejections on the command descriptor, and the eight-package
collapse. All independent of this cutover.
