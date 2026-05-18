# Project Notes

## Command dispatch & UnitOfWork model (RESOLVED — aligned with Axon Framework 5)

A command handler is the atomic boundary. This was settled after reading AF5
5.1.0-SNAPSHOT internals; the earlier "distributed UnitOfWork" exploration has
been removed.

### The model

- **One command handler = one `ProcessingContext` = one UnitOfWork = one
  atomic event-store append.** A handler loads state (building the DCB
  read-set / append condition), decides, appends events, and commits once.
- **`commandBus.dispatch` always starts a fresh UnitOfWork** — primary OR
  nested. A command dispatched from inside another handler (via `send()`) does
  NOT join the caller's UnitOfWork. This matches AF5's `SimpleCommandBus`,
  which creates a new `UnitOfWork` per command and discards the caller's
  `ProcessingContext`.
- **Commands compose by independent commit, not shared transaction.** There is
  no atomic aggregation of nested/remote handler events back into the
  originator. AF5 does not do this and cannot over a generic command
  transport; the `CommandBusConnector` result contract carries only a result
  message, no buffered events.
- **There is one command-sending helper: `send()`.** It copies the caller's
  metadata onto the outgoing command. Correlation/causation lineage propagates
  the AF5 way — through message metadata applied by the correlation-data
  dispatch interceptor — across any transport. No processing-context object is
  threaded through the command API or serialized over the wire.
- **DCB read-set merging happens only WITHIN a single handler's UnitOfWork**
  (multiple `load()`s → one combined append condition → one commit). It does
  not cross command boundaries.

### Conflict handling

- A DCB append-condition conflict is a non-transient failure (AF5:
  `AppendEventsTransactionRejectedException extends AxonNonTransientException`).
- Commands are **not** auto-retried on conflict — the failure propagates to the
  caller, which decides whether to re-dispatch. Automatic retry is built in for
  event processors only (roll back the batch token, re-claim, re-process).
- There is no framework-level idempotency. A handler that may re-run must be
  idempotent by checking its own loaded state (e.g. an AF5-style `notified`
  flag persisted via an event).

Why AF5 threads a `ProcessingContext` through `dispatch(command, context)`: it
is a dispatch-side hook channel for interceptors / retry / tracing, NOT a
transaction handle. Kronos gets that hook access for free via AsyncLocalStorage,
so it needs no context parameter on the command API.
