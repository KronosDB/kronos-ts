---
"@kronos-ts/test": minor
---

Moving time is a capability, not a method everyone has. `.wait` is gone; a
scenario says `.advance` to move the clock and `.await` to let the world catch
up, and only a fixture given a clock it can MOVE will run the first. BREAKING.

```ts
// before — one verb for two ideas, and a runtime throw when it could not
scenario().when(command(…)).wait(90_000).then(…)

// after — the clock, and the world, said separately
scenario().when(command(…)).advance(90_000).then(…)   // moves the clock
scenario().when(command(…)).await().then(…)           // processors catch up
scenario().when(command(…)).await(({ events }) => events.length === 3, 2_000).then(…)
```

`.advance` makes a `Scenario<true>`, and `run` accepts one only on a fixture
built over an `advanceableClock()` — so pairing a time-advancing scenario with
a fixture that cannot move time is a compile error at the line that pairs them,
where it used to be a throw part-way through the run.

```ts
testFixture(scope)                                    // advanceable by default
testFixture(scope, { clock: advanceableClock() })     // …or say it
testFixture(scope, { clock: Date.now })               // reads time, never moves it
```

`realTime` is gone with the flag it stood in for: whether time can be faked is
now decided by the clock the fixture holds. Against real infrastructure — a
postgres poller, a kronosdb server — nothing could hurry those anyway, and
`.await(until)` is what waits for them.

`await` also replaces the `within` re-judging that ran invisibly after every
scenario: waiting is a step you can see, positioned where you meant it.
