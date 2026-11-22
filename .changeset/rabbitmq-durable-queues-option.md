---
"@kronos-ts/rabbitmq": minor
---

**Breaking:** the `durableQueues` topology option is removed. Nothing read it: handler queues were declared durable whatever it was set to, and they still are. A `topology` that sets `durableQueues` no longer compiles; delete the property.
