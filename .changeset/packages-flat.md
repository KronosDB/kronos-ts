---
"@kronos-ts/core": patch
"@kronos-ts/test": patch
"@kronos-ts/rabbitmq": patch
"@kronos-ts/kronosdb": patch
"@kronos-ts/axon-server": patch
"@kronos-ts/postgres": patch
"@kronos-ts/drizzle": patch
"@kronos-ts/knex": patch
"@kronos-ts/kysely": patch
"@kronos-ts/prisma": patch
"@kronos-ts/typeorm": patch
"@kronos-ts/otlp": patch
---

The `extensions/` directory is gone, and so is the concept.

```
packages/{core,test,rabbitmq,kronosdb,axon-server,postgres,drizzle,knex,kysely,prisma,typeorm,otlp}
```

An "extension" implied a plugin contract that this framework does not have and
does not want: every one of these is a package of ordinary functions over the
public core shapes, no more privileged than something you write yourself. Nested
under `extensions/` they read as a second tier, which made "should this be core
or an extension?" a question anybody could ask about anything.

Published package names are unchanged; only repository paths, the workspace
globs, the tsconfig include and the CI globs moved.
