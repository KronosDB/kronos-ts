---
"@kronos-ts/rabbitmq": patch
---

The comments on `rabbitMqCommandBus` and `rabbitMqQueryBus` say what the buses do: every dispatch goes over the broker, including one for a handler in the same process, and `localFirstCommandBus` / `localFirstQueryBus` handle a co-located message without it. They described a bus that sent a subscribed name to the local bus instead. No behaviour changes.
