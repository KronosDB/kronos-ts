---
"@kronos-ts/kronosdb": patch
---

Subscription queries refill their flow-control credit. Long-lived subscriptions
used to stop delivering after the initial window and never resume.

The server grants a subscription a window of credit, decrements it per update,
and DROPS updates once it reaches zero — silently, with nothing sent back to say
so. The client granted `bufferSize ?? 256` on `subscribe` and never topped it
up, so every subscription query stopped delivering after that many updates, with
no error on either side to read as the cause.

```ts
// before — one grant, no refill: works for `window` updates, then stops
outboundSub.send({ subscribe: { subscriptionIdentifier, numberOfPermits: BigInt(bufferSize ?? 256), … } })

// after — the window is topped back up as updates are consumed
handler.offer(update)
if (++consumedSinceRefill >= refillBatch && !subscriptionClosed) {
  outboundSub.send({ flowControl: { subscriptionIdentifier, numberOfPermits: BigInt(consumedSinceRefill) } })
  consumedSinceRefill = 0
}
```

Refills go out once a quarter-window has accrued, so the wire carries one small
message per quarter-window rather than one per update, and none is sent after
`close()`.

The requested window is also clamped to the server's own `[1, 1024]` bound
instead of being trusted: a host passing `bufferSize: 5000` is granted 1024, and
pacing refills against a 5000-wide window would let 1250 updates pass before
topping up — 226 of them beyond the credit the server actually holds, and
dropped. Refill cadence follows what the server granted.

Nothing changes on the handler leg: credit is enforced centrally on the server's
subscription registry, so a handler tracking permits locally would double-count
the same window.
