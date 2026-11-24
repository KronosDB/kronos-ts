---
"@kronos-ts/postgres": minor
---

**Breaking:** the peer dependency ranges are `pg` `^8.20.0` and `postgres` `^3.4.9 || ^4.1.1`. They were `>=8.0.0` and `>=3.0.0`, while the README named 8.20+ and 3.4+. The test suites run on `pg` 8.20.0 and on `postgres` 3.4.9 and 4.1.1, so the ranges start at those versions. A plain `bun add postgres`, which installs 4, is inside the range. The README states the ranges and the tested versions.
