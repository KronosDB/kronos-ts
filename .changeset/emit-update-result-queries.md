---
"@kronos-ts/core": patch
---

`ctx.emitUpdate` accepts a query that declares a result. Its type took `QueryDescriptor<P>`, whose result defaults to `undefined`, so emitting for any query with a `result` schema was refused at compile time. The filter still reads the query's payload.
