---
"@kronos-ts/kronosdb": minor
---

Adopt KronosDB 0.5 batched read wire format (requires server >= 0.5).

Source and Stream responses now arrive as event batches (`SequencedEventBatch`)
instead of one event per gRPC message; the client unpacks them transparently,
so `source()` results and event-processor streams behave exactly as before —
just faster (server-side batching removes per-message framing overhead on
whole-log reads and live tailing). Permit-based flow control now counts
events, not messages. `SourceRequest`/`StreamSubscribe` gain a `batchSize`
option (0 = server default). Older servers (<= 0.4) are not supported by this
version of the client.

Also fixes `generate-proto` to emit `.js` import suffixes so regenerated
sources pass typecheck under `moduleResolution: node16`.
