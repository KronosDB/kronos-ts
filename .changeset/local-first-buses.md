---
"@kronos-ts/core": minor
"@kronos-ts/rabbitmq": minor
"@kronos-ts/axon-server": minor
"@kronos-ts/kronosdb": minor
---

`localFirstCommandBus(next, local)` and `localFirstQueryBus(next, local)` hand a message whose handler was subscribed through them to `local` instead of `next`. They replace `preferLocal` on the RabbitMQ buses and `shortcutQueriesToLocalHandlers` on the Axon Server and KronosDB query buses.

- RabbitMQ buses send every dispatch over the broker unless wrapped. `preferLocal` defaulted to `true`, so wrap `rabbitMqCommandBus(local, rabbit)` in `localFirstCommandBus(…, local)`, and the query bus likewise, to keep local handling.
- `shortcutQueriesToLocalHandlers: true` becomes `localFirstQueryBus(axonServerQueryBus(local, conn), local)` or `localFirstQueryBus(kronosDbQueryBus(local, kdb), local)`.
- A message handled locally is no longer counted by the transport: its shutdown does not wait for it, and its request limit and timeout do not apply.
- A RabbitMQ subscription query fetches its initial result over the broker, even when the handler is in this process.
