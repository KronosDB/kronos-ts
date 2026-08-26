---
"@kronos-ts/test": minor
---

Moving time is a capability, not a method everyone has. `.wait` is gone; a
scenario says `.advance` to move the clock and `.await` to let the world catch
up, and only a fixture given a clock it can MOVE will run the first. BREAKING.

```ts
// before — one verb for two ideas, and a runtime throw when it could not
scenario().when(command(…)).wait(90_000).then(…)

// after — moving the clock, and judging a world that is still working, are
// different things and say so
scenario().when(command(…)).advance(90_000).then(…)   // move the clock
scenario().when(command(…)).then(event(Opened, …))    // holds NOW
scenario().when(command(…)).await(event(Opened, …))   // holds EVENTUALLY
```

`await` is `then` with a deadline: the same claims, in the same vocabulary,
re-judged until they hold or `run`'s `within` passes. Nothing extra to
write — what you are waiting for is what you were going to assert anyway. That
is the shape a world which keeps working after the act returns needs: a
projection behind a database, a processor on another node.

`then` still fails on the first look, because a deterministic scope has nothing
to wait for and waiting would only make failures slow. Which claim style to use
is now the scenario's own statement rather than a guess the fixture made from
whether it recognised your resources.

The line is not "did this cross the event store". An append is in the recording
the moment it happens, and a processor the fixture assembled is settled
automatically before anything is judged — so a command that appends, an
automation that reacts, and the command that automation dispatches are all
`then`. The boundary is what the fixture can WATCH settle: it holds its own
processors and can ask them; it cannot ask a projection landing in a database
on its own schedule, a processor on another node, or an effect a handler kicked
off without waiting for. Those are `await`.

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
